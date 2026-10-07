use beavers::{
    App, BoxError, CancellationToken, ChannelOutput, ChannelRaw, ChannelSink, ChannelSource,
    Completion, DeadLetter, Delivery, ErrorPolicy, HandlerError, InMemorySink, IterSource,
    OrderingKey, Receive, ReceiveError, Sink, Source, SourceMessage, Subscription, blocking,
    channel,
};
use std::{
    collections::VecDeque,
    future::{Future, pending, poll_fn},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};

async fn assert_pending<F: Future>(future: Pin<&mut F>) {
    let mut future = future;
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

/// Yields its deliveries, each counting ACKs and carrying a revocation token, then waits.
struct Held {
    deliveries: VecDeque<Delivery<i32>>,
}

impl Held {
    fn new(values: &[i32], acks: &Arc<AtomicUsize>, revocation: &CancellationToken) -> Self {
        let deliveries = values
            .iter()
            .map(|&value| {
                let acks = acks.clone();
                Delivery::new(value, move || async move {
                    acks.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .with_revocation(revocation.clone())
            })
            .collect();
        Self { deliveries }
    }
}

impl Source for Held {
    type Message = Delivery<i32>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        match self.deliveries.pop_front() {
            Some(delivery) => Ok(Receive::Message(delivery)),
            None => pending().await,
        }
    }
}

/// Yields keyed deliveries whose raw form is a record name, then ends.
struct Records(VecDeque<Delivery<i32, String>>);

impl Records {
    fn new(values: &[(i32, i64)]) -> Self {
        Self(
            values
                .iter()
                .map(|&(value, partition)| {
                    Delivery::untracked(value)
                        .with_ordering_key(OrderingKey::new("orders", partition))
                        .with_raw(format!("record-{value}"))
                })
                .collect(),
        )
    }
}

impl Source for Records {
    type Message = Delivery<i32, String>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        Ok(match self.0.pop_front() {
            Some(delivery) => Receive::Message(delivery),
            None => Receive::End,
        })
    }
}

#[tokio::test]
async fn deliveries_keep_the_upstream_ordering_key_and_raw_form() {
    let (sink, mut fetched) = channel(4);
    let run = tokio::spawn(
        App::new()
            .subscribe(
                "fetch",
                Records::new(&[(1, 0), (2, 3)]),
                sink,
                |n: i32| async move { Ok(n * 10) },
            )
            .run_until(CancellationToken::new()),
    );
    for (value, partition) in [(10, 0), (20, 3)] {
        let Receive::Message(delivery) = fetched.receive().await.unwrap() else {
            panic!("missing delivery")
        };
        assert_eq!(delivery.decode().unwrap(), value);
        assert_eq!(
            delivery.ordering_key(),
            Some(OrderingKey::new("orders", partition))
        );
        let raw = delivery.raw();
        let expected = format!("record-{}", value / 10);
        assert_eq!(raw.downcast_ref::<String>(), Some(&expected));
        assert_eq!(format!("{raw:?}"), format!("{expected:?}"));
        delivery.ack().await.unwrap();
    }
    run.await.unwrap().unwrap();
    assert!(matches!(fetched.receive().await.unwrap(), Receive::End));
}

#[tokio::test]
async fn downstream_dead_letters_keep_the_first_upstream_delivery() {
    let (to_score, fetched) = channel(4);
    let (to_store, scored) = channel(4);
    let dead_letters = InMemorySink::<DeadLetter<i32, ChannelRaw>>::default();
    App::new()
        .subscribe(
            "fetch",
            Records::new(&[(1, 0), (2, 1)]),
            to_score,
            |n: i32| async move { Ok(n * 10) },
        )
        .subscribe(
            "score",
            fetched,
            to_store,
            |n: i32| async move { Ok(n + 1) },
        )
        .subscription(
            Subscription::new(
                "store",
                scored,
                InMemorySink::default(),
                |n: i32| async move {
                    if n == 21 {
                        return Err(HandlerError::Reject(BoxError::from("invalid")));
                    }
                    Ok(n)
                },
            )
            .dlq(dead_letters.clone())
            .error_policy(ErrorPolicy::dead_letter_all()),
        )
        .run_until(CancellationToken::new())
        .await
        .unwrap();
    let [dead_letter] = &dead_letters.values()[..] else {
        panic!("expected one dead letter")
    };
    assert_eq!(dead_letter.input, Some(21));
    assert_eq!(
        dead_letter.raw.downcast_ref::<String>().map(String::as_str),
        Some("record-2")
    );
    let json = serde_json::to_value(dead_letter).unwrap();
    assert_eq!(json["raw"], "record-2");
}

#[tokio::test]
async fn values_from_application_code_have_no_upstream_delivery() {
    let (sender, mut input) = ChannelSource::bounded(1);
    sender.send(1).await.unwrap();
    let Receive::Message(delivery) = input.receive().await.unwrap() else {
        panic!("missing delivery")
    };
    assert_eq!(delivery.ordering_key(), None);
    let raw = delivery.raw();
    assert!(raw.is_empty());
    assert_eq!(raw.downcast_ref::<()>(), None);
    assert_eq!(serde_json::to_value(&raw).unwrap(), serde_json::Value::Null);
}

#[tokio::test]
async fn upstream_ack_waits_for_downstream_completion() {
    let source = IterSource::new([1, 2]);
    let acks = source.acknowledgements();
    let (sink, fetched) = channel(4);
    let output = InMemorySink::default();
    let gate = Arc::new(Semaphore::new(0));
    let started = Arc::new(Notify::new());
    let app = App::new()
        .subscribe("fetch", source, sink, |n: i32| async move { Ok(n * 10) })
        .subscribe("score", fetched, output.clone(), {
            let gate = gate.clone();
            let started = started.clone();
            move |n: i32| {
                let gate = gate.clone();
                let started = started.clone();
                async move {
                    started.notify_one();
                    gate.acquire().await.unwrap().forget();
                    Ok(n + 1)
                }
            }
        });
    let run = tokio::spawn(app.run_until(CancellationToken::new()));
    started.notified().await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(acks.load(Ordering::SeqCst), 0);
    gate.add_permits(2);
    run.await.unwrap().unwrap();
    assert_eq!(output.values(), [11, 21]);
    assert_eq!(acks.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn async_and_blocking_stages_chain() {
    let (to_score, fetched) = channel(1);
    let (to_store, scored) = channel(1);
    let output = InMemorySink::default();
    App::new()
        .subscribe(
            "fetch",
            IterSource::new(["a", "bb", "ccc"]),
            to_score,
            |id: &'static str| async move { Ok(id.to_uppercase()) },
        )
        .subscribe(
            "score",
            fetched,
            to_store,
            blocking(|document: String| Ok(document.len())),
        )
        .subscribe("store", scored, output.clone(), |score: usize| async move {
            Ok(score * 2)
        })
        .run_until(CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(output.values(), [2, 4, 6]);
}

#[tokio::test]
async fn shutdown_completes_values_already_sent() {
    let acks = Arc::new(AtomicUsize::new(0));
    let (sink, fetched) = channel(1);
    let output = InMemorySink::default();
    let gate = Arc::new(Semaphore::new(0));
    let started = Arc::new(Notify::new());
    let shutdown = CancellationToken::new();
    let app = App::new()
        .subscribe(
            "fetch",
            Held::new(&[7], &acks, &CancellationToken::new()),
            sink,
            |n: i32| async move { Ok(n) },
        )
        .subscribe("score", fetched, output.clone(), {
            let gate = gate.clone();
            let started = started.clone();
            move |n: i32| {
                let gate = gate.clone();
                let started = started.clone();
                async move {
                    started.notify_one();
                    gate.acquire().await.unwrap().forget();
                    Ok(n)
                }
            }
        });
    let run = tokio::spawn(app.run_until(shutdown.clone()));
    started.notified().await;
    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(20)).await;
    gate.add_permits(1);
    run.await.unwrap().unwrap();
    assert_eq!(output.values(), [7]);
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn upstream_revocation_abandons_downstream_work() {
    let acks = Arc::new(AtomicUsize::new(0));
    let revocation = CancellationToken::new();
    let (sink, fetched) = channel(1);
    let output = InMemorySink::default();
    let started = Arc::new(Notify::new());
    let shutdown = CancellationToken::new();
    let app = App::new()
        .subscribe(
            "fetch",
            Held::new(&[1], &acks, &revocation),
            sink,
            |n: i32| async move { Ok(n) },
        )
        .subscribe("score", fetched, output.clone(), {
            let started = started.clone();
            move |n: i32| {
                let started = started.clone();
                async move {
                    started.notify_one();
                    pending::<()>().await;
                    Ok(n)
                }
            }
        });
    let run = tokio::spawn(app.run_until(shutdown.clone()));
    started.notified().await;
    revocation.cancel();
    tokio::time::sleep(Duration::from_millis(20)).await;
    shutdown.cancel();
    run.await.unwrap().unwrap();
    assert!(output.values().is_empty());
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn downstream_failure_leaves_upstream_unacknowledged() {
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    let (sink, fetched) = channel(1);
    let result = App::new()
        .subscribe("fetch", source, sink, |n: i32| async move { Ok(n) })
        .subscribe(
            "score",
            fetched,
            InMemorySink::default(),
            |_: i32| async move { Err::<i32, _>(HandlerError::Fatal(BoxError::from("broken"))) },
        )
        .run_until(CancellationToken::new())
        .await;
    assert!(result.is_err());
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn publish_completes_on_downstream_ack() {
    let (sink, mut source) = channel(1);
    let value = ChannelOutput::from(5);
    let publish = sink.publish(&value);
    tokio::pin!(publish);
    assert_pending(publish.as_mut()).await;
    let Receive::Message(delivery) = source.receive().await.unwrap() else {
        panic!("missing delivery")
    };
    assert_eq!(delivery.decode().unwrap(), 5);
    assert_pending(publish.as_mut()).await;
    delivery.ack().await.unwrap();
    publish.await.unwrap();
}

#[tokio::test]
async fn dropped_delivery_fails_publication() {
    let (sink, mut source) = channel(1);
    let value = ChannelOutput::from(5);
    let publish = sink.publish(&value);
    tokio::pin!(publish);
    assert_pending(publish.as_mut()).await;
    let Receive::Message(delivery) = source.receive().await.unwrap() else {
        panic!("missing delivery")
    };
    drop(delivery);
    assert!(publish.await.is_err());
}

#[tokio::test]
async fn dropped_publication_revokes_and_skips_values() {
    let (sink, mut source) = channel(2);
    {
        let value = ChannelOutput::from(1);
        let publish = sink.publish(&value);
        tokio::pin!(publish);
        assert_pending(publish.as_mut()).await;
    }
    let value = ChannelOutput::from(2);
    let mut publish = Box::pin(sink.publish(&value));
    assert_pending(publish.as_mut()).await;
    // The abandoned first value is skipped.
    let Receive::Message(delivery) = source.receive().await.unwrap() else {
        panic!("missing delivery")
    };
    assert_eq!(delivery.decode().unwrap(), 2);
    let revocation = delivery.revocation().unwrap();
    assert!(!revocation.is_cancelled());
    drop(publish);
    assert!(revocation.is_cancelled());
}

#[tokio::test]
async fn publication_waiting_for_capacity_can_be_cancelled() {
    let (sink, mut source) = channel(1);
    let value = ChannelOutput::from(1);
    let first = sink.publish(&value);
    tokio::pin!(first);
    assert_pending(first.as_mut()).await;
    {
        let value = ChannelOutput::from(2);
        let second = sink.publish(&value);
        tokio::pin!(second);
        assert_pending(second.as_mut()).await;
    }
    let Receive::Message(delivery) = source.receive().await.unwrap() else {
        panic!("missing delivery")
    };
    assert_eq!(delivery.decode().unwrap(), 1);
    delivery.ack().await.unwrap();
    first.await.unwrap();
    sink.close().await.unwrap();
    assert!(matches!(source.receive().await.unwrap(), Receive::End));
}

#[tokio::test]
async fn closed_channel_rejects_publication() {
    let (sink, mut source) = channel::<i32>(1);
    sink.close().await.unwrap();
    sink.close().await.unwrap();
    assert!(sink.publish(&ChannelOutput::from(1)).await.is_err());
    assert!(matches!(source.receive().await.unwrap(), Receive::End));

    let (sink, mut source) = channel::<i32>(1);
    source.close().await.unwrap();
    assert!(sink.publish(&ChannelOutput::from(1)).await.is_err());
}

#[tokio::test]
async fn cloned_sinks_fan_in_and_end_after_every_clone_closes() {
    let (sink, fetched) = channel(2);
    let first = IterSource::new([1, 2]);
    let second = IterSource::new([10]);
    let (first_acks, second_acks) = (first.acknowledgements(), second.acknowledgements());
    let output = InMemorySink::default();
    App::new()
        .subscribe("left", first, sink.clone(), |n: i32| async move { Ok(n) })
        .subscribe("right", second, sink, |n: i32| async move { Ok(n) })
        .subscribe(
            "sum",
            fetched,
            output.clone(),
            |n: i32| async move { Ok(n + 1) },
        )
        .run_until(CancellationToken::new())
        .await
        .unwrap();
    let mut values = output.values();
    values.sort();
    assert_eq!(values, [2, 3, 11]);
    assert_eq!(first_acks.load(Ordering::SeqCst), 2);
    assert_eq!(second_acks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn closing_one_clone_keeps_the_others_open() {
    let (sink, mut source) = channel::<i32>(1);
    let clone = sink.clone();
    sink.close().await.unwrap();
    assert!(sink.publish(&ChannelOutput::from(1)).await.is_err());
    {
        let value = ChannelOutput::from(2);
        let publish = clone.publish(&value);
        tokio::pin!(publish);
        assert_pending(publish.as_mut()).await;
        let Receive::Message(delivery) = source.receive().await.unwrap() else {
            panic!("missing delivery")
        };
        delivery.ack().await.unwrap();
        publish.await.unwrap();
    }
    drop(clone);
    assert!(matches!(source.receive().await.unwrap(), Receive::End));
}

#[tokio::test]
async fn application_ends_run_an_in_process_worker() {
    let (sender, input) = ChannelSource::bounded(4);
    let (output, mut results) = ChannelSink::bounded(4);
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(
        App::new()
            .subscribe("double", input, output, |n: i32| async move { Ok(n * 2) })
            .run_until(shutdown.clone()),
    );
    sender.send(1).await.unwrap();
    assert_eq!(results.recv().await, Some(2));
    // Completes once the output is taken from the receiver.
    let (sent, received) = tokio::join!(sender.send_and_wait(2), results.recv());
    sent.unwrap();
    assert_eq!(received, Some(4));
    drop(sender);
    run.await.unwrap().unwrap();
    assert_eq!(results.recv().await, None);
}

#[tokio::test]
async fn application_receiver_completes_publication_on_recv() {
    let (sink, mut results) = ChannelSink::bounded(2);
    let value = ChannelOutput::from(1);
    let mut first = Box::pin(sink.publish(&value));
    assert_pending(first.as_mut()).await;
    {
        let value = ChannelOutput::from(2);
        let abandoned = sink.publish(&value);
        tokio::pin!(abandoned);
        assert_pending(abandoned.as_mut()).await;
    }
    assert_eq!(results.recv().await, Some(1));
    first.await.unwrap();
    // The abandoned value is skipped.
    let value = ChannelOutput::from(3);
    let third = sink.publish(&value);
    tokio::pin!(third);
    assert_pending(third.as_mut()).await;
    assert_eq!(results.recv().await, Some(3));
    third.await.unwrap();
    sink.close().await.unwrap();
    assert!(sink.publish(&4.into()).await.is_err());
    assert_eq!(results.recv().await, None);
}

#[tokio::test]
async fn dropped_receiver_fails_buffered_publications() {
    let (sink, results) = ChannelSink::<i32>::bounded(1);
    let value = ChannelOutput::from(1);
    let mut publish = Box::pin(sink.publish(&value));
    assert_pending(publish.as_mut()).await;
    drop(results);
    assert!(publish.await.is_err());
    assert!(sink.publish(&2.into()).await.is_err());
}

#[tokio::test]
async fn upstream_ack_waits_for_application_recv() {
    let source = IterSource::new([1, 2]);
    let acks = source.acknowledgements();
    let (sink, mut results) = ChannelSink::bounded(4);
    let run = tokio::spawn(
        App::new()
            .subscribe("fetch", source, sink, |n: i32| async move { Ok(n) })
            .run_until(CancellationToken::new()),
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(acks.load(Ordering::SeqCst), 0);
    assert_eq!(results.recv().await, Some(1));
    assert_eq!(results.recv().await, Some(2));
    run.await.unwrap().unwrap();
    assert_eq!(acks.load(Ordering::SeqCst), 2);
    assert_eq!(results.recv().await, None);
}

#[tokio::test]
async fn application_fed_source_stops_on_shutdown() {
    let (sender, input) = ChannelSource::<i32>::bounded(1);
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(
        App::new()
            .subscribe(
                "idle",
                input,
                InMemorySink::default(),
                |n: i32| async move { Ok(n) },
            )
            .run_until(shutdown.clone()),
    );
    shutdown.cancel();
    run.await.unwrap().unwrap();
    drop(sender);
}

async fn run_ahead(capacity: usize, max_in_flight: usize) -> (usize, usize) {
    let source = IterSource::new(0..10);
    let acks = source.acknowledgements();
    let calls = Arc::new(AtomicUsize::new(0));
    let (sink, mut results) = ChannelSink::bounded(capacity);
    let counted = calls.clone();
    let run = tokio::spawn(
        App::new()
            .subscription(
                Subscription::new("ahead", source, sink, move |n: i32| {
                    counted.fetch_add(1, Ordering::SeqCst);
                    async move { Ok(n) }
                })
                .max_in_flight(max_in_flight),
            )
            .run_until(CancellationToken::new()),
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    let ahead = calls.load(Ordering::SeqCst);
    assert_eq!(acks.load(Ordering::SeqCst), 0);
    for expected in 0..10 {
        assert_eq!(results.recv().await, Some(expected));
    }
    run.await.unwrap().unwrap();
    assert_eq!(acks.load(Ordering::SeqCst), 10);
    (ahead, calls.load(Ordering::SeqCst))
}

#[tokio::test]
async fn enqueued_outputs_free_the_job_slot_until_capacity() {
    // Three values fill the channel; the fourth finished its handler and waits for space.
    assert_eq!(run_ahead(3, 64).await, (4, 10));
}

#[tokio::test]
async fn enqueued_outputs_do_not_count_toward_max_in_flight() {
    assert_eq!(run_ahead(8, 2).await, (9, 10));
}

struct FailingCompletion;

impl Sink<i32> for FailingCompletion {
    type Prepared = i32;

    fn prepare(&self, value: i32) -> Result<i32, BoxError> {
        Ok(value)
    }

    async fn publish(&self, output: &i32) -> Result<(), BoxError> {
        self.submit(output).await?.wait().await
    }

    async fn submit(&self, _: &i32) -> Result<Completion, BoxError> {
        Ok(Completion::pending(async { Err("lost".into()) }))
    }
}

#[tokio::test]
async fn failed_completion_stops_without_ack() {
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    let result = App::new()
        .subscribe(
            "lost",
            source,
            FailingCompletion,
            |n: i32| async move { Ok(n) },
        )
        .run_until(CancellationToken::new())
        .await;
    let error = format!("{:#}", result.unwrap_err());
    assert!(error.contains("output completion failed"), "{error}");
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}
