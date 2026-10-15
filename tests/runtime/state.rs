use super::fixtures::{Flaky, TransactionalSource, Transactions, fast};
use beavers::{
    App, BoxError, CancellationToken, Completion, DeadLetter, Delivery, Emit, ErrorPolicy,
    FailureKind, HandlerError, InMemorySink, IterSource, KeyedMessage, OrderingKey, Position,
    Receive, ReceiveError, Sink, Source, SourceMessage, Subscription,
    state::{InMemoryState, State, stateful},
};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

/// A delivery of a source with message keys.
struct Keyed {
    delivery: Delivery<i32>,
    key: Option<Vec<u8>>,
}

impl SourceMessage for Keyed {
    type Item = i32;
    type Raw = <Delivery<i32> as SourceMessage>::Raw;

    fn decode(&self) -> Result<i32, BoxError> {
        self.delivery.decode()
    }

    async fn ack(self) -> Result<(), BoxError> {
        self.delivery.ack().await
    }

    fn raw(&self) -> Self::Raw {
        self.delivery.raw()
    }

    fn ordering_key(&self) -> Option<OrderingKey> {
        self.delivery.ordering_key()
    }

    fn message_key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    fn position(&self) -> Option<Position> {
        self.delivery.position()
    }

    fn revocation(&self) -> Option<CancellationToken> {
        self.delivery.revocation()
    }
}

impl KeyedMessage for Keyed {}

fn keyed(value: i32, key: &str) -> Keyed {
    with_key(Delivery::untracked(value), key)
}

fn with_key(delivery: Delivery<i32>, key: &str) -> Keyed {
    Keyed {
        delivery,
        key: Some(key.into()),
    }
}

/// Delivers keyed deliveries in order.
struct KeyedSource(VecDeque<Keyed>);

impl KeyedSource {
    fn new(deliveries: impl IntoIterator<Item = Keyed>) -> Self {
        Self(deliveries.into_iter().collect())
    }
}

impl Source for KeyedSource {
    type Message = Keyed;

    async fn receive(&mut self) -> Result<Receive<Keyed>, ReceiveError> {
        Ok(match self.0.pop_front() {
            Some(delivery) => Receive::Message(delivery),
            None => Receive::End,
        })
    }
}

async fn running_total(value: i32, mut total: State<i32>) -> beavers::Result<i32> {
    *total += value;
    Ok(*total)
}

#[tokio::test]
async fn state_is_kept_per_message_key() {
    let totals = InMemoryState::<i32>::new();
    let sink = InMemorySink::default();
    let source = KeyedSource::new([keyed(1, "a"), keyed(10, "b"), keyed(2, "a"), keyed(20, "b")]);
    App::new()
        .subscribe(
            "totals",
            source,
            sink.clone(),
            stateful(totals.clone(), running_total),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), [1, 10, 3, 30]);
    assert_eq!(totals.get("a").await, Some(3));
    assert_eq!(totals.get("b").await, Some(30));
}

#[tokio::test]
async fn a_key_chosen_from_the_input_replaces_the_message_key() {
    let totals =
        InMemoryState::<i32>::new().key(|value: &i32| if *value % 2 == 0 { "even" } else { "odd" });
    let sink = InMemorySink::default();
    App::new()
        .subscribe(
            "parity",
            IterSource::new([1, 2, 3, 4]),
            sink.clone(),
            stateful(totals.clone(), running_total),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), [1, 2, 4, 6]);
    assert_eq!(totals.get("odd").await, Some(4));
}

#[tokio::test]
async fn a_delivery_without_a_message_key_is_rejected() {
    let dlq = InMemorySink::default();
    let acks = Arc::new(AtomicUsize::new(0));
    let counted = acks.clone();
    let keyless = Keyed {
        delivery: Delivery::new(5, move || async move {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok::<_, BoxError>(())
        }),
        key: None,
    };
    App::new()
        .subscription(
            Subscription::new(
                "keyless",
                KeyedSource::new([keyless]),
                InMemorySink::<i32>::default(),
                stateful(InMemoryState::<i32>::new(), running_total),
            )
            .dead_letter(dlq.clone()),
        )
        .run()
        .await
        .unwrap();
    let [dead_letter]: [DeadLetter<i32, ()>; 1] = dlq.values().try_into().unwrap();
    assert_eq!(dead_letter.failure, FailureKind::Rejected);
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_failed_attempt_discards_its_state_change() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let totals = InMemoryState::<i32>::new().key(|_: &i32| "all");
    let sink = InMemorySink::default();
    let counted = attempts.clone();
    App::new()
        .subscription(
            Subscription::new(
                "retries",
                IterSource::new([1, 2]),
                sink.clone(),
                stateful(totals.clone(), move |value: i32, mut total: State<i32>| {
                    let attempt = counted.fetch_add(1, Ordering::SeqCst);
                    async move {
                        *total += value;
                        if attempt == 0 {
                            return Err(HandlerError::Retry("not yet".into()));
                        }
                        Ok(*total)
                    }
                }),
            )
            .handler_retry(fast()),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(sink.values(), [1, 3]);
    assert_eq!(totals.get("all").await, Some(3));
}

#[tokio::test]
async fn a_dead_lettered_delivery_does_not_change_state() {
    let totals = InMemoryState::<i32>::new().key(|_: &i32| "all");
    let dlq = InMemorySink::<DeadLetter<i32, ()>>::default();
    App::new()
        .subscription(
            Subscription::new(
                "rejects",
                IterSource::new([1, -5, 2]),
                InMemorySink::<i32>::default(),
                stateful(
                    totals.clone(),
                    |value: i32, mut total: State<i32>| async move {
                        *total += value;
                        if value < 0 {
                            return Err(HandlerError::Reject("negative".into()));
                        }
                        Ok(*total)
                    },
                ),
            )
            .dead_letter(dlq.clone())
            .error_policy(ErrorPolicy::dead_letter_all()),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(dlq.values().len(), 1);
    assert_eq!(totals.get("all").await, Some(3));
}

#[tokio::test]
async fn state_is_not_committed_when_publishing_fails() {
    let totals = InMemoryState::<i32>::new().key(|_: &i32| "all");
    let source = IterSource::new([4]);
    let acks = source.acknowledgements();
    let result = App::new()
        .subscription(
            Subscription::new(
                "unpublished",
                source,
                Flaky {
                    calls: Arc::default(),
                    acks: acks.clone(),
                    fail_always: true,
                },
                stateful(totals.clone(), running_total),
            )
            .publish_retry(fast()),
        )
        .run()
        .await;
    assert!(result.is_err());
    assert_eq!(acks.count(), 0);
    assert_eq!(totals.get("all").await, None);
}

#[tokio::test]
async fn a_redelivery_the_state_already_covers_skips_the_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let totals = InMemoryState::<i32>::new();
    let sink = InMemorySink::default();
    let acks = Arc::new(AtomicUsize::new(0));
    let delivery = |value: i32, offset: i64| {
        let acks = acks.clone();
        let delivery = Delivery::new(value, move || async move {
            acks.fetch_add(1, Ordering::SeqCst);
            Ok::<_, BoxError>(())
        })
        .with_position(Position::new("events", 0, offset));
        with_key(delivery, "a")
    };
    let counted = calls.clone();
    App::new()
        .subscribe(
            "redelivery",
            KeyedSource::new([
                delivery(1, 7),
                delivery(2, 8),
                delivery(1, 7),
                delivery(4, 9),
            ]),
            sink.clone(),
            stateful(totals.clone(), move |value: i32, mut total: State<i32>| {
                counted.fetch_add(1, Ordering::SeqCst);
                async move {
                    *total += value;
                    Ok(*total)
                }
            }),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(sink.values(), [1, 3, 7]);
    assert_eq!(acks.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn state_stored_under_a_revoked_partition_is_discarded() {
    let totals = InMemoryState::<i32>::new();
    let sink = InMemorySink::default();
    let revoked = CancellationToken::new();
    let first = with_key(Delivery::untracked(1).with_revocation(revoked.clone()), "a");
    App::new()
        .subscribe(
            "revoked",
            KeyedSource::new([first]),
            sink.clone(),
            stateful(totals.clone(), running_total),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(totals.get("a").await, Some(1));
    revoked.cancel();
    assert_eq!(totals.get("a").await, None);
    App::new()
        .subscribe(
            "reassigned",
            KeyedSource::new([keyed(5, "a")]),
            sink.clone(),
            stateful(totals.clone(), running_total),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), [1, 5]);
}

#[tokio::test]
async fn clearing_removes_the_value_and_reading_stores_nothing() {
    let totals = InMemoryState::<i32>::new();
    let sink = InMemorySink::default();
    App::new()
        .subscription(Subscription::new_emitting(
            "clear",
            KeyedSource::new([keyed(1, "a"), keyed(0, "a"), keyed(0, "b")]),
            sink.clone(),
            stateful(
                totals.clone(),
                |value: i32, mut total: State<i32>| async move {
                    match value {
                        0 if total.is_new() => Ok(Emit::None),
                        0 => {
                            total.clear();
                            Ok(Emit::One(-1))
                        }
                        _ => {
                            *total += value;
                            Ok(Emit::One(*total))
                        }
                    }
                },
            ),
        ))
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), [1, -1]);
    assert_eq!(totals.get("a").await, None);
    assert_eq!(totals.get("b").await, None);
}

#[tokio::test]
async fn deliveries_with_one_key_take_turns_across_jobs() {
    let totals = InMemoryState::<i32>::new().key(|_: &i32| "all");
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                "concurrent",
                IterSource::new(1..=50),
                sink.clone(),
                stateful(
                    totals.clone(),
                    |value: i32, mut total: State<i32>| async move {
                        let before = *total;
                        tokio::task::yield_now().await;
                        *total = before + value;
                        Ok(*total)
                    },
                ),
            )
            .concurrency(8),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(totals.get("all").await, Some((1..=50).sum()));
}

#[tokio::test]
async fn a_transactional_subscription_cannot_be_stateful() {
    let error = App::new()
        .subscription(
            Subscription::new(
                "transactional",
                TransactionalSource::new([1]),
                Transactions::default(),
                stateful(
                    InMemoryState::<i32>::new().key(|_: &i32| "all"),
                    running_total,
                ),
            )
            .transactional(),
        )
        .run()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("stateful"), "{error}");
}

/// Confirms each output after a delay, recording the confirmation in `log`.
#[derive(Clone, Default)]
struct Confirming {
    log: Arc<Mutex<Vec<String>>>,
}

impl Sink<i32> for Confirming {
    type Prepared = i32;

    fn prepare(&self, value: i32) -> Result<i32, BoxError> {
        Ok(value)
    }

    async fn publish(&self, _: &i32) -> Result<(), BoxError> {
        unreachable!("outputs are submitted")
    }

    async fn submit(&self, output: &i32) -> Result<Completion, BoxError> {
        let log = self.log.clone();
        let output = *output;
        Ok(Completion::pending(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            log.lock().unwrap().push(format!("confirmed {output}"));
            Ok(())
        }))
    }
}

#[tokio::test]
async fn state_commits_after_pending_outputs_complete() {
    let totals = InMemoryState::<i32>::new().key(|_: &i32| "all");
    let sink = Confirming::default();
    let log = sink.log.clone();
    let handler_log = log.clone();
    App::new()
        .subscribe(
            "pending",
            IterSource::new([1, 2]),
            sink,
            stateful(totals.clone(), move |value: i32, mut total: State<i32>| {
                handler_log
                    .lock()
                    .unwrap()
                    .push(format!("handled {value} after {}", *total));
                async move {
                    *total += value;
                    Ok(*total)
                }
            }),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(
        *log.lock().unwrap(),
        [
            "handled 1 after 0",
            "confirmed 1",
            "handled 2 after 1",
            "confirmed 3"
        ]
    );
    assert_eq!(totals.get("all").await, Some(3));
}
