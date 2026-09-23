use super::fixtures::{Flaky, Waiting, fast};
use beavers::{App, HandlerError, InMemorySink, IterSource, Subscription};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[tokio::test]
async fn reject_goes_to_dlq_before_ack() {
    let source = IterSource::new([7]);
    let acks = source.acknowledgements();
    let dlq = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(source, InMemorySink::<i32>::default(), |_| async {
                Err(HandlerError::Reject(anyhow::anyhow!("invalid")))
            })
            .dlq(dlq.clone()),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(dlq.values(), vec![7]);
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn rejects_without_dlq_and_fatal_leave_input_unacked() {
    for fatal in [false, true] {
        let source = IterSource::new([1]);
        let acks = source.acknowledgements();
        assert!(
            App::new()
                .subscribe(
                    source,
                    InMemorySink::<i32>::default(),
                    move |_| async move {
                        Err(if fatal {
                            HandlerError::Fatal(anyhow::anyhow!("fatal"))
                        } else {
                            HandlerError::Reject(anyhow::anyhow!("reject"))
                        })
                    }
                )
                .run()
                .await
                .is_err()
        );
        assert_eq!(acks.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn fatal_stops_other_subscription_waiting_for_input() {
    let app = App::new()
        .subscribe(
            Waiting,
            InMemorySink::<i32>::default(),
            |n| async move { Ok(n) },
        )
        .subscribe(
            IterSource::new([1]),
            InMemorySink::<i32>::default(),
            |_| async { Err(HandlerError::Fatal(anyhow::anyhow!("stop"))) },
        );
    assert!(
        tokio::time::timeout(Duration::from_secs(1), app.run())
            .await
            .unwrap()
            .is_err()
    );
}

#[tokio::test]
async fn failed_dlq_does_not_ack() {
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    let dlq = Flaky {
        calls: Arc::default(),
        acks: acks.clone(),
        fail_always: true,
    };
    assert!(
        App::new()
            .subscription(
                Subscription::new(source, InMemorySink::<i32>::default(), |_| async {
                    Err(HandlerError::Reject(anyhow::anyhow!("reject")))
                })
                .dlq(dlq)
                .publish_retry(fast())
            )
            .run()
            .await
            .is_err()
    );
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn mapping_failure_does_not_rerun_handler_or_publish() {
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    let sink = InMemorySink::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    assert!(
        App::new()
            .subscription(
                Subscription::new(source, sink.clone(), move |n| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    async move { Ok(n) }
                })
                .middleware(|_, _| Err(HandlerError::Retry(anyhow::anyhow!("mapping failed"))))
            )
            .run()
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(sink.values().is_empty());
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}
