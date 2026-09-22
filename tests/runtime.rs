use beavers::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
fn fast() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        initial_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
    }
}

#[tokio::test]
async fn finite_input_publishes_then_acks() {
    let source = IterSource::new([1, 2, 3]);
    let acks = source.acknowledgements();
    let sink = InMemorySink::default();
    App::new()
        .subscribe(source, sink.clone(), |n| async move { Ok(n * 2) })
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![2, 4, 6]);
    assert_eq!(acks.load(Ordering::SeqCst), 3);
}
struct Flaky {
    calls: Arc<AtomicUsize>,
    acks: Arc<AtomicUsize>,
    fail_always: bool,
}
impl Sink<i32> for Flaky {
    type Prepared = i32;
    fn prepare(&self, value: i32) -> anyhow::Result<i32> {
        Ok(value)
    }
    async fn publish(&self, _: &i32) -> anyhow::Result<()> {
        assert_eq!(self.acks.load(Ordering::SeqCst), 0);
        let calls = self.calls.fetch_add(1, Ordering::SeqCst);
        anyhow::ensure!(!self.fail_always && calls >= 2, "offline");
        Ok(())
    }
}
#[tokio::test]
async fn publish_retry_does_not_repeat_handler_or_mapping() {
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    let calls = Arc::new(AtomicUsize::new(0));
    let handlers = Arc::new(AtomicUsize::new(0));
    let h = handlers.clone();
    let mappings = Arc::new(AtomicUsize::new(0));
    let m = mappings.clone();
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
                    h.fetch_add(1, Ordering::SeqCst);
                    async move { Ok(n) }
                },
            )
            .publish_retry(fast())
            .middleware(move |_, n| {
                m.fetch_add(1, Ordering::SeqCst);
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
async fn explicit_many_and_none_and_plain_vec() {
    let sink = InMemorySink::default();
    App::new()
        .subscription(Subscription::new_emitting(
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
        .subscribe(IterSource::new([1]), sink.clone(), |n| async move {
            Ok(vec![n, n])
        })
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![vec![1, 1]]);
}
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
struct Waiting;
impl Source for Waiting {
    type Message = Delivery<i32>;
    async fn receive(&mut self) -> std::result::Result<Receive<Delivery<i32>>, ReceiveError> {
        std::future::pending().await
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
async fn shutdown_aborts_timed_out_processing_without_ack() {
    let token = CancellationToken::new();
    let stop = token.clone();
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    let app = App::new().subscription(
        Subscription::new(source, InMemorySink::<i32>::default(), move |_| {
            stop.cancel();
            async { std::future::pending::<Result<i32>>().await }
        })
        .drain_timeout(Duration::from_millis(10)),
    );
    assert!(app.run_until(token).await.is_err());
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn invalid_config_fails_before_processing() {
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    assert!(
        App::new()
            .subscription(
                Subscription::new(source, InMemorySink::default(), |n| async move { Ok(n) })
                    .concurrency(0)
            )
            .run()
            .await
            .is_err()
    );
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

struct ScriptedSource {
    events: std::collections::VecDeque<std::result::Result<Option<i32>, bool>>,
    calls: Arc<AtomicUsize>,
}
impl Source for ScriptedSource {
    type Message = Delivery<i32>;
    async fn receive(&mut self) -> std::result::Result<Receive<Delivery<i32>>, ReceiveError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.events.pop_front().unwrap_or(Ok(None)) {
            Ok(Some(v)) => Ok(Receive::Message(Delivery::untracked(v))),
            Ok(None) => Ok(Receive::End),
            Err(true) => Err(ReceiveError::Retry(anyhow::anyhow!("retry"))),
            Err(false) => Err(ReceiveError::Fatal(anyhow::anyhow!("fatal"))),
        }
    }
}
#[tokio::test]
async fn receive_retries_reset_after_success() {
    let calls = Arc::default();
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
        calls,
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
#[tokio::test]
async fn backpressure_limits_receiving_and_allows_parallel_work() {
    let source = IterSource::new(0..6);
    let acks = source.acknowledgements();
    let entered = Arc::new(AtomicUsize::new(0));
    let count = entered.clone();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let handler_gate = gate.clone();
    let app = App::new().subscription(
        Subscription::new(source, InMemorySink::default(), move |n| {
            let gate = handler_gate.clone();
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                gate.acquire().await.unwrap().forget();
                Ok(n)
            }
        })
        .concurrency(4)
        .max_in_flight(2),
    );
    let run = tokio::spawn(app.run());
    tokio::time::timeout(Duration::from_secs(1), async {
        while entered.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(entered.load(Ordering::SeqCst), 2);
    assert_eq!(acks.load(Ordering::SeqCst), 0);
    gate.add_permits(6);
    run.await.unwrap().unwrap();
    assert_eq!(acks.load(Ordering::SeqCst), 6);
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
