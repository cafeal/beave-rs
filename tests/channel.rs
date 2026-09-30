use beavers::{
    App, CancellationToken, ChannelSink, ChannelSource, Delivery, HandlerError, InMemorySink,
    IterSource, Receive, ReceiveError, Sink, Source, SourceMessage, blocking, channel,
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
            |_: i32| async move { Err::<i32, _>(HandlerError::Fatal(anyhow::anyhow!("broken"))) },
        )
        .run_until(CancellationToken::new())
        .await;
    assert!(result.is_err());
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn publish_completes_on_downstream_ack() {
    let (sink, mut source) = channel(1);
    let publish = sink.publish(&5);
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
    let publish = sink.publish(&5);
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
        let publish = sink.publish(&1);
        tokio::pin!(publish);
        assert_pending(publish.as_mut()).await;
    }
    let mut publish = Box::pin(sink.publish(&2));
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
    let first = sink.publish(&1);
    tokio::pin!(first);
    assert_pending(first.as_mut()).await;
    {
        let second = sink.publish(&2);
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
    assert!(sink.publish(&1).await.is_err());
    assert!(matches!(source.receive().await.unwrap(), Receive::End));

    let (sink, mut source) = channel::<i32>(1);
    source.close().await.unwrap();
    assert!(sink.publish(&1).await.is_err());
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
    assert!(sink.publish(&1).await.is_err());
    {
        let publish = clone.publish(&2);
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
    sender.send_and_wait(2).await.unwrap();
    assert_eq!(results.recv().await, Some(2));
    assert_eq!(results.recv().await, Some(4));
    drop(sender);
    run.await.unwrap().unwrap();
    assert_eq!(results.recv().await, None);
}

#[tokio::test]
async fn application_receiver_completes_publication_at_enqueue() {
    let (sink, mut results) = ChannelSink::bounded(1);
    sink.publish(&1).await.unwrap();
    {
        let publish = sink.publish(&2);
        tokio::pin!(publish);
        assert_pending(publish.as_mut()).await;
    }
    assert_eq!(results.recv().await, Some(1));
    sink.close().await.unwrap();
    assert!(sink.publish(&3).await.is_err());
    assert_eq!(results.recv().await, None);

    let (sink, mut results) = ChannelSink::<i32>::bounded(1);
    results.close();
    assert!(sink.publish(&1).await.is_err());
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
