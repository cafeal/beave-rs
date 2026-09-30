use super::fixtures::{Capture, FieldSource};
use beavers::{App, ErrorPolicy, FailureAction, InMemorySink, Subscription};
use tracing::Level;

#[tokio::test]
async fn deliveries_are_traced_by_stage() {
    let capture = Capture::global();
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                FieldSource::text(&["1"]),
                sink.clone(),
                |n: i32| async move { Ok(n) },
            )
            .name("traced"),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![1]);

    let spans = capture.spans("traced");
    let subscription = spans
        .iter()
        .find(|span| span.name == "subscription")
        .unwrap();
    assert_eq!(subscription.field("subscription"), Some("traced"));
    let message = spans.iter().find(|span| span.name == "message").unwrap();
    assert_eq!(message.scope, ["subscription"]);
    for stage in ["decode", "handler", "encode", "publish", "ack"] {
        let span = spans.iter().find(|span| span.name == stage).unwrap();
        assert_eq!(span.scope, ["message", "subscription"], "{stage}");
    }
    let handler = spans.iter().find(|span| span.name == "handler").unwrap();
    assert_eq!(handler.field("attempt"), Some("1"));
}

#[tokio::test]
async fn discarded_and_dead_lettered_deliveries_are_logged() {
    let capture = Capture::global();
    App::new()
        .subscription(
            Subscription::new(
                FieldSource::text(&["oops", "-1"]),
                InMemorySink::default(),
                |n: i32| async move {
                    if n < 0 {
                        return Err(beavers::HandlerError::Reject(anyhow::anyhow!("negative")));
                    }
                    Ok(n)
                },
            )
            .name("logged")
            .dlq(InMemorySink::default())
            .error_policy(ErrorPolicy {
                decode: FailureAction::Discard,
                ..ErrorPolicy::default()
            }),
        )
        .run()
        .await
        .unwrap();

    let events = capture.events("logged");
    let discarded = events
        .iter()
        .find(|event| event.name == "discarding delivery")
        .unwrap();
    assert_eq!(discarded.level, Level::WARN);
    assert_eq!(discarded.field("failure"), Some("decode failed"));
    assert!(discarded.field("error").unwrap().contains("invalid digit"));
    assert_eq!(discarded.scope, ["message", "subscription"]);

    let dead_lettered = events
        .iter()
        .find(|event| event.name == "dead-lettered delivery")
        .unwrap();
    assert_eq!(dead_lettered.level, Level::WARN);
    assert_eq!(
        dead_lettered.field("failure"),
        Some("handler rejected input")
    );
    assert_eq!(dead_lettered.field("attempts"), Some("1"));
}

#[tokio::test]
async fn subscription_failure_is_logged() {
    let capture = Capture::global();
    let result = App::new()
        .subscription(
            Subscription::new(
                FieldSource::text(&["oops"]),
                InMemorySink::default(),
                |n: i32| async move { Ok(n) },
            )
            .name("failing"),
        )
        .run()
        .await;
    assert!(result.is_err());
    let failed = capture
        .events("failing")
        .into_iter()
        .find(|event| event.name == "subscription failed")
        .unwrap();
    assert_eq!(failed.level, Level::ERROR);
    assert_eq!(failed.scope, ["subscription"]);
    assert!(failed.field("error").unwrap().contains("decode failed"));
}
