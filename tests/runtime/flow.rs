use super::fixtures::{DeliverySource, keyed};
use beavers::{
    App, CancellationToken, Delivery, InMemorySink, OrderingKey, Receive, ReceiveError, Source,
    SourceMessage, Subscription,
};
use std::{
    collections::{HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};

#[derive(Debug, PartialEq, Eq)]
enum Flow {
    Pause(i64),
    Resume(i64),
}

/// Delivers keyed deliveries in order, skipping keys while they are paused,
/// and ends once every delivery was received.
struct PausableSource {
    deliveries: VecDeque<Delivery<i32>>,
    paused: HashSet<OrderingKey>,
    flow: Arc<Mutex<Vec<Flow>>>,
    resumed: Arc<Notify>,
}

impl PausableSource {
    fn new(deliveries: impl IntoIterator<Item = Delivery<i32>>) -> Self {
        Self {
            deliveries: deliveries.into_iter().collect(),
            paused: HashSet::new(),
            flow: Arc::default(),
            resumed: Arc::default(),
        }
    }
}

impl Source for PausableSource {
    type Message = Delivery<i32>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        loop {
            if self.deliveries.is_empty() {
                return Ok(Receive::End);
            }
            let next = self
                .deliveries
                .iter()
                .position(|delivery| !self.paused.contains(&delivery.ordering_key().unwrap()));
            if let Some(index) = next {
                return Ok(Receive::Message(self.deliveries.remove(index).unwrap()));
            }
            self.resumed.notified().await;
        }
    }

    fn pause(&mut self, key: &OrderingKey) -> bool {
        self.paused.insert(key.clone());
        self.flow.lock().unwrap().push(Flow::Pause(key.index()));
        true
    }

    fn resume(&mut self, key: &OrderingKey) {
        self.paused.remove(key);
        self.flow.lock().unwrap().push(Flow::Resume(key.index()));
        self.resumed.notify_one();
    }
}

#[tokio::test]
async fn a_slow_key_pauses_while_other_keys_keep_flowing() {
    let acks = Arc::new(AtomicUsize::new(0));
    // Partition 0 comes first and blocks until the gate opens.
    let source = PausableSource::new(
        (0..6)
            .map(|value| keyed(value, 0, &acks))
            .chain((100..104).map(|value| keyed(value, 1, &acks))),
    );
    let flow = source.flow.clone();
    let gate = Arc::new(Semaphore::new(0));
    let handler_gate = gate.clone();
    let sink = InMemorySink::default();
    let app = App::new().subscription(
        Subscription::new(
            "a_slow_key_pauses_while_other_keys_keep_flowing",
            source,
            sink.clone(),
            move |value: i32| {
                let gate = handler_gate.clone();
                async move {
                    if value < 100 {
                        gate.acquire().await.unwrap().forget();
                    }
                    Ok(value)
                }
            },
        )
        .concurrency(2)
        .max_in_flight(4)
        .max_in_flight_per_key(2),
    );
    let run = tokio::spawn(app.run());

    tokio::time::timeout(Duration::from_secs(1), async {
        while sink.values().len() < 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("partition 1 was stalled by partition 0");
    assert_eq!(sink.values(), [100, 101, 102, 103]);
    assert_eq!(*flow.lock().unwrap(), [Flow::Pause(0)]);

    gate.add_permits(6);
    run.await.unwrap().unwrap();
    assert_eq!(acks.load(Ordering::SeqCst), 10);
    let flow = flow.lock().unwrap();
    assert_eq!(flow.first(), Some(&Flow::Pause(0)));
    assert!(flow.contains(&Flow::Resume(0)));
}

#[tokio::test]
async fn a_source_that_cannot_pause_keeps_the_global_bound() {
    let acks = Arc::new(AtomicUsize::new(0));
    let source = DeliverySource::new((0..4).map(|value| keyed(value, 0, &acks)));
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                "a_source_that_cannot_pause_keeps_the_global_bound",
                source,
                sink.clone(),
                |value| async move { Ok(value) },
            )
            .max_in_flight_per_key(1),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), [0, 1, 2, 3]);
    assert_eq!(acks.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn a_handoff_abandons_unstarted_deliveries_and_lets_started_ones_finish() {
    let acks = Arc::new(AtomicUsize::new(0));
    let handoff = CancellationToken::new();
    let source = DeliverySource::new(
        (0..3).map(|value| keyed(value, 0, &acks).with_handoff(handoff.clone())),
    );
    let started = Arc::new(Mutex::new(Vec::new()));
    let handler_started = started.clone();
    let gate = Arc::new(Semaphore::new(0));
    let handler_gate = gate.clone();
    let sink = InMemorySink::default();
    let app = App::new().subscription(Subscription::new(
        "a_handoff_abandons_unstarted_deliveries_and_lets_started_ones_finish",
        source,
        sink.clone(),
        move |value: i32| {
            handler_started.lock().unwrap().push(value);
            let gate = handler_gate.clone();
            async move {
                gate.acquire().await.unwrap().forget();
                Ok(value)
            }
        },
    ));
    let run = tokio::spawn(app.run());

    tokio::time::timeout(Duration::from_secs(1), async {
        while started.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    handoff.cancel();
    gate.add_permits(3);
    run.await.unwrap().unwrap();
    assert_eq!(*started.lock().unwrap(), [0]);
    assert_eq!(sink.values(), [0]);
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn zero_per_key_limit_is_rejected() {
    let error = App::new()
        .subscription(
            Subscription::new(
                "zero_per_key_limit_is_rejected",
                DeliverySource::new([]),
                InMemorySink::default(),
                |value: i32| async move { Ok(value) },
            )
            .max_in_flight_per_key(0),
        )
        .run()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("max_in_flight_per_key"));
}
