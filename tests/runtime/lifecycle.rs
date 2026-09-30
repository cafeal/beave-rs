use beavers::{App, Emit, InMemorySink, IterSource, Subscription};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn finite_input_publishes_then_acks() {
    let source = IterSource::new([1, 2, 3]);
    let acks = source.acknowledgements();
    let sink = InMemorySink::default();
    App::new()
        .subscribe(
            "finite_input_publishes_then_acks",
            source,
            sink.clone(),
            |n| async move { Ok(n * 2) },
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![2, 4, 6]);
    assert_eq!(acks.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn explicit_many_and_none_and_plain_vec() {
    let sink = InMemorySink::default();
    App::new()
        .subscription(Subscription::new_emitting(
            "explicit_many_and_none_and_plain_vec",
            IterSource::new([0, 1]),
            sink.clone(),
            |n| async move {
                Ok(if n == 0 {
                    Emit::None
                } else {
                    Emit::Many(vec![1, 2])
                })
            },
        ))
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![1, 2]);

    let sink = InMemorySink::default();
    App::new()
        .subscribe(
            "explicit_many_and_none_and_plain_vec",
            IterSource::new([1]),
            sink.clone(),
            |n| async move { Ok(vec![n, n]) },
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![vec![1, 1]]);
}

#[tokio::test]
async fn invalid_config_fails_before_processing() {
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    assert!(
        App::new()
            .subscription(
                Subscription::new(
                    "invalid_config_fails_before_processing",
                    source,
                    InMemorySink::default(),
                    |n| async move { Ok(n) }
                )
                .concurrency(0)
            )
            .run()
            .await
            .is_err()
    );
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn subscription_names_must_be_unique_and_non_empty() {
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    let error = App::new()
        .subscribe(
            "orders",
            source,
            InMemorySink::default(),
            |n: i32| async move { Ok(n) },
        )
        .subscribe(
            "orders",
            IterSource::new([2]),
            InMemorySink::default(),
            |n: i32| async move { Ok(n) },
        )
        .subscribe(
            " ",
            IterSource::new([3]),
            InMemorySink::default(),
            |n: i32| async move { Ok(n) },
        )
        .run()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("duplicate subscription name \"orders\""),
        "{error}"
    );
    assert!(
        error.contains("subscription name must not be empty"),
        "{error}"
    );
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}
