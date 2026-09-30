use super::fixtures::{DeliverySource, keyed};
use beavers::{
    App, CancellationToken, Delivery, InMemorySink, OrderingKey, ProcessingOrder, Result,
    Subscription,
};
use std::{
    collections::HashMap,
    future::pending,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

/// Records which values started and the peak parallelism per partition
/// (`value / 100`), then waits for a permit before completing.
#[derive(Clone)]
struct Probe {
    started: Arc<Mutex<Vec<i32>>>,
    active: Arc<Mutex<HashMap<i32, usize>>>,
    peak: Arc<AtomicUsize>,
    gate: Arc<Semaphore>,
}

impl Probe {
    fn new() -> Self {
        Self {
            started: Arc::default(),
            active: Arc::default(),
            peak: Arc::default(),
            gate: Arc::new(Semaphore::new(0)),
        }
    }

    async fn handle(self, value: i32) -> Result<i32> {
        self.started.lock().unwrap().push(value);
        {
            let mut active = self.active.lock().unwrap();
            let count = active.entry(value / 100).or_default();
            *count += 1;
            self.peak.fetch_max(*count, Ordering::SeqCst);
        }
        self.gate.acquire().await.unwrap().forget();
        *self.active.lock().unwrap().get_mut(&(value / 100)).unwrap() -= 1;
        Ok(value)
    }

    fn started(&self) -> Vec<i32> {
        self.started.lock().unwrap().clone()
    }

    async fn wait_started(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while self.started.lock().unwrap().len() < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("handlers did not start");
    }
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(20)).await;
}

#[tokio::test]
async fn same_key_runs_sequentially_while_other_keys_run_in_parallel() {
    let acks = Arc::new(AtomicUsize::new(0));
    let source = DeliverySource::new([
        keyed(0, 0, &acks),
        keyed(1, 0, &acks),
        keyed(100, 1, &acks),
        keyed(101, 1, &acks),
    ]);
    let probe = Probe::new();
    let handler_probe = probe.clone();
    let sink = InMemorySink::default();
    let app = App::new().subscription(
        Subscription::new(
            "same_key_runs_sequentially_while_other_keys_run_in_parallel",
            source,
            sink.clone(),
            move |value| handler_probe.clone().handle(value),
        )
        .concurrency(4),
    );
    let run = tokio::spawn(app.run());

    probe.wait_started(2).await;
    settle().await;
    assert_eq!(probe.started(), vec![0, 100]);

    probe.gate.add_permits(4);
    run.await.unwrap().unwrap();
    assert_eq!(probe.peak.load(Ordering::SeqCst), 1);
    assert_eq!(acks.load(Ordering::SeqCst), 4);
    let values = sink.values();
    let position = |value| values.iter().position(|v| *v == value).unwrap();
    assert!(position(0) < position(1));
    assert!(position(100) < position(101));
}

#[tokio::test]
async fn unordered_mode_runs_same_key_in_parallel() {
    let acks = Arc::new(AtomicUsize::new(0));
    let source = DeliverySource::new([keyed(0, 0, &acks), keyed(1, 0, &acks)]);
    let probe = Probe::new();
    let handler_probe = probe.clone();
    let app = App::new().subscription(
        Subscription::new(
            "unordered_mode_runs_same_key_in_parallel",
            source,
            InMemorySink::default(),
            move |value| handler_probe.clone().handle(value),
        )
        .concurrency(2)
        .ordering(ProcessingOrder::Unordered),
    );
    let run = tokio::spawn(app.run());

    probe.wait_started(2).await;
    probe.gate.add_permits(2);
    run.await.unwrap().unwrap();
    assert_eq!(probe.peak.load(Ordering::SeqCst), 2);
    assert_eq!(acks.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn max_in_flight_bounds_deliveries_queued_behind_a_key() {
    let acks = Arc::new(AtomicUsize::new(0));
    let source = DeliverySource::new((0..5).map(|value| keyed(value, 0, &acks)));
    let receives = source.receives.clone();
    let probe = Probe::new();
    let handler_probe = probe.clone();
    let app = App::new().subscription(
        Subscription::new(
            "max_in_flight_bounds_deliveries_queued_behind_a_key",
            source,
            InMemorySink::default(),
            move |value| handler_probe.clone().handle(value),
        )
        .concurrency(4)
        .max_in_flight(3),
    );
    let run = tokio::spawn(app.run());

    probe.wait_started(1).await;
    settle().await;
    assert_eq!(receives.load(Ordering::SeqCst), 3);
    assert_eq!(probe.started(), vec![0]);

    probe.gate.add_permits(5);
    run.await.unwrap().unwrap();
    assert_eq!(probe.started(), vec![0, 1, 2, 3, 4]);
    assert_eq!(acks.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn revocation_abandons_in_flight_work_without_ack_or_failure() {
    let acks = Arc::new(AtomicUsize::new(0));
    let revoked = CancellationToken::new();
    let source = DeliverySource::new([
        keyed(0, 0, &acks).with_revocation(revoked.clone()),
        keyed(1, 0, &acks),
    ]);
    let started = Arc::new(AtomicUsize::new(0));
    let handler_started = started.clone();
    let sink = InMemorySink::default();
    let app = App::new().subscription(Subscription::new(
        "revocation_abandons_in_flight_work_without_ack_or_failure",
        source,
        sink.clone(),
        move |value: i32| {
            handler_started.fetch_add(1, Ordering::SeqCst);
            async move {
                if value == 0 {
                    pending::<()>().await;
                }
                Ok(value)
            }
        },
    ));
    let run = tokio::spawn(app.run());

    tokio::time::timeout(Duration::from_secs(1), async {
        while started.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    revoked.cancel();
    run.await.unwrap().unwrap();
    assert_eq!(sink.values(), vec![1]);
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ack_rejected_because_of_revocation_is_not_a_failure() {
    let revoked = CancellationToken::new();
    let ack_revoked = revoked.clone();
    let delivery = Delivery::new(7, move || async move {
        ack_revoked.cancel();
        anyhow::bail!("assignment revoked")
    })
    .with_ordering_key(OrderingKey::new("events", 0))
    .with_revocation(revoked);
    let sink = InMemorySink::default();
    App::new()
        .subscribe(
            "ack_rejected_because_of_revocation_is_not_a_failure",
            DeliverySource::new([delivery]),
            sink.clone(),
            |value| async move { Ok(value) },
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![7]);
}

#[tokio::test]
async fn shutdown_leaves_deliveries_queued_behind_a_key_unstarted() {
    let acks = Arc::new(AtomicUsize::new(0));
    let source = DeliverySource::new([keyed(0, 0, &acks), keyed(1, 0, &acks)]);
    let probe = Probe::new();
    let handler_probe = probe.clone();
    let receives = source.receives.clone();
    let token = CancellationToken::new();
    let app = App::new().subscription(
        Subscription::new(
            "shutdown_leaves_deliveries_queued_behind_a_key_unstarted",
            source,
            InMemorySink::default(),
            move |value| handler_probe.clone().handle(value),
        )
        .concurrency(2),
    );
    let run = tokio::spawn(app.run_until(token.clone()));

    probe.wait_started(1).await;
    settle().await;
    assert!(receives.load(Ordering::SeqCst) >= 2);
    token.cancel();
    probe.gate.add_permits(2);
    run.await.unwrap().unwrap();
    assert_eq!(probe.started(), vec![0]);
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}
