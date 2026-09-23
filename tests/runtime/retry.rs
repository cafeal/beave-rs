use super::fixtures::{Flaky, ScriptedSource, fast};
use beavers::{
    App, CancellationToken, HandlerError, InMemorySink, IterSource, RetryPolicy, Subscription,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[tokio::test]
async fn publish_retry_does_not_repeat_handler_or_mapping() {
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    let calls = Arc::new(AtomicUsize::new(0));
    let handlers = Arc::new(AtomicUsize::new(0));
    let handler_calls = handlers.clone();
    let mappings = Arc::new(AtomicUsize::new(0));
    let mapping_calls = mappings.clone();
    App::new()
        .subscription(
            Subscription::new(
                source,
                Flaky {
                    calls: calls.clone(),
                    acks: acks.clone(),
                    fail_always: false,
                },
                move |n| {
                    handler_calls.fetch_add(1, Ordering::SeqCst);
                    async move { Ok(n) }
                },
            )
            .publish_retry(fast())
            .middleware(move |_, n| {
                mapping_calls.fetch_add(1, Ordering::SeqCst);
                Ok(n)
            }),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(handlers.load(Ordering::SeqCst), 1);
    assert_eq!(mappings.load(Ordering::SeqCst), 1);
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn exhausted_publish_never_acks() {
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    let sink = Flaky {
        calls: Arc::default(),
        acks: acks.clone(),
        fail_always: true,
    };
    assert!(
        App::new()
            .subscription(
                Subscription::new(source, sink, |n| async move { Ok(n) }).publish_retry(fast())
            )
            .run()
            .await
            .is_err()
    );
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn receive_retries_reset_after_success() {
    let source = ScriptedSource {
        events: [
            Err(true),
            Err(true),
            Ok(Some(1)),
            Err(true),
            Err(true),
            Ok(Some(2)),
            Ok(None),
        ]
        .into(),
        calls: Arc::default(),
    };
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(source, sink.clone(), |n| async move { Ok(n) }).receive_retry(fast()),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![1, 2]);
}

#[tokio::test]
async fn receive_fatal_and_exhaustion_stop() {
    for (events, expected) in [(vec![Err(true); 3], 3), (vec![Err(false)], 1)] {
        let calls = Arc::new(AtomicUsize::new(0));
        let source = ScriptedSource {
            events: events.into(),
            calls: calls.clone(),
        };
        assert!(
            App::new()
                .subscription(
                    Subscription::new(source, InMemorySink::default(), |n| async move { Ok(n) })
                        .receive_retry(fast())
                )
                .run()
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), expected);
    }
}

#[tokio::test]
async fn receive_backoff_can_be_interrupted() {
    let calls = Arc::new(AtomicUsize::new(0));
    let source = ScriptedSource {
        events: [Err(true)].into(),
        calls: calls.clone(),
    };
    let token = CancellationToken::new();
    let stop = token.clone();
    let app = App::new().subscription(
        Subscription::new(source, InMemorySink::default(), |n| async move { Ok(n) }).receive_retry(
            RetryPolicy {
                max_attempts: 3,
                initial_delay: Duration::from_secs(60),
                max_delay: Duration::from_secs(60),
            },
        ),
    );
    let run = tokio::spawn(app.run_until(token));
    while calls.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn handler_retry_is_explicit() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    App::new()
        .subscription(
            Subscription::new(source, InMemorySink::default(), move |n| {
                let attempt = counter.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt < 2 {
                        Err(HandlerError::Retry(anyhow::anyhow!("again")))
                    } else {
                        Ok(n)
                    }
                }
            })
            .retry(fast()),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}
