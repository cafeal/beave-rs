use super::fixtures::{FieldSource, Value, counter, install_metrics, series};
use beavers::{
    App, ErrorPolicy, FailureAction, HandlerError, InMemorySink, RetryPolicy, Sink, Subscription,
};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

fn fast() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        initial_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
        ..RetryPolicy::default()
    }
}

#[tokio::test]
async fn failures_are_counted_by_kind_and_action() {
    install_metrics();
    let name = "counted-failures";
    let sink = InMemorySink::default();
    let dlq = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                FieldSource::text(&["1", "oops", "-2", "3"]),
                sink.clone(),
                |n: i32| async move {
                    if n < 0 {
                        return Err(HandlerError::Reject(anyhow::anyhow!("negative")));
                    }
                    Ok(n)
                },
            )
            .name(name)
            .dlq(dlq.clone())
            .error_policy(ErrorPolicy {
                decode: FailureAction::Discard,
                ..ErrorPolicy::default()
            }),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![1, 3]);
    assert_eq!(dlq.values().len(), 1);

    let subscription = ("subscription", name);
    assert_eq!(
        counter("beavers_deliveries_received_total", &[subscription]),
        4
    );
    assert_eq!(
        counter("beavers_deliveries_acknowledged_total", &[subscription]),
        4
    );
    let failures = |failure, action| {
        counter(
            "beavers_delivery_failures_total",
            &[subscription, ("failure", failure), ("action", action)],
        )
    };
    assert_eq!(failures("decode", "discard"), 1);
    assert_eq!(failures("rejected", "dead_letter"), 1);
    assert_eq!(failures("retry_exhausted", "dead_letter"), 0);
    assert_eq!(failures("encode", "stop"), 0);
}

#[tokio::test]
async fn dead_letter_action_without_sink_is_reported_as_stop() {
    install_metrics();
    let name = "stop-without-dlq";
    let result = App::new()
        .subscription(
            Subscription::new(
                FieldSource::text(&["1"]),
                InMemorySink::default(),
                |_: i32| async move { Err::<i32, _>(HandlerError::Reject(anyhow::anyhow!("no"))) },
            )
            .name(name),
        )
        .run()
        .await;
    assert!(result.is_err());
    assert_eq!(
        counter(
            "beavers_delivery_failures_total",
            &[
                ("subscription", name),
                ("failure", "rejected"),
                ("action", "stop")
            ],
        ),
        1
    );
    assert_eq!(
        counter(
            "beavers_deliveries_acknowledged_total",
            &[("subscription", name)]
        ),
        0
    );
}

struct FailTwice(AtomicUsize);

impl Sink<i32> for FailTwice {
    type Prepared = i32;

    fn prepare(&self, value: i32) -> anyhow::Result<i32> {
        Ok(value)
    }

    async fn publish(&self, _: &i32) -> anyhow::Result<()> {
        anyhow::ensure!(self.0.fetch_add(1, Ordering::SeqCst) >= 2, "offline");
        Ok(())
    }
}

#[tokio::test]
async fn retries_and_stage_durations_are_recorded() {
    install_metrics();
    let name = "retries-and-stages";
    let calls = std::sync::Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    App::new()
        .subscription(
            Subscription::new(
                FieldSource::text(&["7"]),
                FailTwice(AtomicUsize::new(0)),
                move |n: i32| {
                    let calls = handler_calls.clone();
                    async move {
                        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                            return Err(HandlerError::Retry(anyhow::anyhow!("busy")));
                        }
                        Ok(n)
                    }
                },
            )
            .name(name)
            .retry(fast())
            .publish_retry(fast()),
        )
        .run()
        .await
        .unwrap();
    let subscription = ("subscription", name);
    assert_eq!(counter("beavers_handler_retries_total", &[subscription]), 1);
    assert_eq!(
        counter(
            "beavers_publish_failures_total",
            &[subscription, ("sink", "output")]
        ),
        2
    );
    let samples = |stage| -> usize {
        series(
            "beavers_stage_duration_seconds",
            &[subscription, ("stage", stage)],
        )
        .into_iter()
        .map(|value| match value {
            Value::Histogram(samples) => samples.len(),
            other => panic!("not a histogram: {other:?}"),
        })
        .sum()
    };
    assert_eq!(samples("decode"), 1);
    assert_eq!(samples("handler"), 2);
    assert_eq!(samples("encode"), 1);
    assert_eq!(samples("publish"), 1);
    assert_eq!(samples("ack"), 1);
    assert_eq!(samples("dead_letter"), 0);
}
