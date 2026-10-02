//! Health state recorded by subscription runtimes and read as a [`HealthReport`].
use crate::shutdown::CancellationToken;
use serde::Serialize;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
};

/// Lifecycle stage of one subscription.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionStatus {
    /// Registered, but the application has not started running it.
    Pending,
    /// Receiving and processing deliveries.
    Running,
    /// Stopped receiving and draining deliveries already received.
    Stopping,
    /// Finished without an error.
    Stopped,
    /// Finished with an error, which stops the application.
    Failed,
}

impl SubscriptionStatus {
    const ALL: [Self; 5] = [
        Self::Pending,
        Self::Running,
        Self::Stopping,
        Self::Stopped,
        Self::Failed,
    ];
}

/// Health of one subscription at the time of the report.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SubscriptionReport {
    /// Subscription name.
    pub name: String,
    /// Lifecycle stage.
    pub status: SubscriptionStatus,
    /// Consecutive failed receives since the last successful one.
    pub receive_failures: usize,
    /// The last receive failed and the runtime is waiting for its retry delay
    /// before receiving again.
    pub receive_backoff: bool,
    /// Deliveries whose output publication or transaction commit failed and is
    /// waiting to be retried.
    pub publish_retries: usize,
    /// Whether this subscription counts as ready: it is running and neither
    /// waiting to retry a receive nor retrying a publication, or its source
    /// ended and it stopped.
    pub ready: bool,
}

/// Application health at the time of the report.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HealthReport {
    /// `false` once a subscription has failed, which stops the application.
    pub live: bool,
    /// `true` while the application runs, has not started shutting down, and
    /// every subscription is ready.
    pub ready: bool,
    /// Shutdown was requested by a signal, the cancellation token passed to
    /// [`App::run_until`](crate::App::run_until), or a subscription failure.
    pub shutting_down: bool,
    /// One report per registered subscription, in registration order.
    pub subscriptions: Vec<SubscriptionReport>,
}

/// A cloneable handle to the health of an [`App`](crate::App)'s subscriptions,
/// obtained with [`App::health`](crate::App::health) before the application
/// runs.
#[derive(Clone, Default)]
pub struct Health(Arc<Registry>);

#[derive(Default)]
struct Registry {
    subscriptions: Mutex<Vec<Arc<Tracker>>>,
    /// Set when the application starts running.
    shutdown: OnceLock<CancellationToken>,
}

impl Health {
    /// Snapshot of the application's current health.
    pub fn report(&self) -> HealthReport {
        let subscriptions: Vec<_> = self
            .0
            .subscriptions
            .lock()
            .unwrap()
            .iter()
            .map(|tracker| tracker.report())
            .collect();
        let started = self.0.shutdown.get();
        let shutting_down = started.is_some_and(CancellationToken::is_cancelled);
        HealthReport {
            live: subscriptions
                .iter()
                .all(|subscription| subscription.status != SubscriptionStatus::Failed),
            ready: started.is_some()
                && !shutting_down
                && subscriptions.iter().all(|subscription| subscription.ready),
            shutting_down,
            subscriptions,
        }
    }

    /// Whether no subscription has failed; see [`HealthReport::live`].
    pub fn is_live(&self) -> bool {
        self.report().live
    }

    /// Whether the application is ready; see [`HealthReport::ready`].
    pub fn is_ready(&self) -> bool {
        self.report().ready
    }

    pub(crate) fn register(&self, name: &str) -> Arc<Tracker> {
        let tracker = Arc::new(Tracker::new(name));
        self.0.subscriptions.lock().unwrap().push(tracker.clone());
        tracker
    }

    pub(crate) fn start(&self, shutdown: &CancellationToken) {
        let _ = self.0.shutdown.set(shutdown.clone());
    }
}

/// State one subscription runtime updates as it receives and publishes.
pub(crate) struct Tracker {
    name: String,
    status: AtomicU8,
    receive_failures: AtomicUsize,
    receive_backoff: AtomicBool,
    publish_retries: AtomicUsize,
}

impl Tracker {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            status: AtomicU8::new(SubscriptionStatus::Pending as u8),
            receive_failures: AtomicUsize::new(0),
            receive_backoff: AtomicBool::new(false),
            publish_retries: AtomicUsize::new(0),
        }
    }

    pub(crate) fn set_status(&self, status: SubscriptionStatus) {
        self.status.store(status as u8, Ordering::Relaxed);
    }

    /// Records a failed receive; the runtime waits before receiving again.
    pub(crate) fn receive_failed(&self, failures: usize) {
        self.receive_failures.store(failures, Ordering::Relaxed);
        self.receive_backoff.store(true, Ordering::Relaxed);
    }

    /// Records that the runtime is receiving again after its retry delay.
    pub(crate) fn receive_resumed(&self) {
        self.receive_backoff.store(false, Ordering::Relaxed);
    }

    pub(crate) fn receive_succeeded(&self) {
        self.receive_failures.store(0, Ordering::Relaxed);
    }

    /// Counts a delivery waiting to retry its publication until the returned
    /// guard is dropped.
    pub(crate) fn publish_retry(&self) -> PublishRetry<'_> {
        self.publish_retries.fetch_add(1, Ordering::Relaxed);
        PublishRetry(self)
    }

    fn report(&self) -> SubscriptionReport {
        let status = SubscriptionStatus::ALL[self.status.load(Ordering::Relaxed) as usize];
        let receive_failures = self.receive_failures.load(Ordering::Relaxed);
        let receive_backoff = self.receive_backoff.load(Ordering::Relaxed);
        let publish_retries = self.publish_retries.load(Ordering::Relaxed);
        let ready = match status {
            SubscriptionStatus::Running => !receive_backoff && publish_retries == 0,
            SubscriptionStatus::Stopped => true,
            SubscriptionStatus::Pending
            | SubscriptionStatus::Stopping
            | SubscriptionStatus::Failed => false,
        };
        SubscriptionReport {
            name: self.name.clone(),
            status,
            receive_failures,
            receive_backoff,
            publish_retries,
            ready,
        }
    }
}

pub(crate) struct PublishRetry<'a>(&'a Tracker);

impl Drop for PublishRetry<'_> {
    fn drop(&mut self) {
        self.0.publish_retries.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_follows_lifecycle_and_failures() {
        let health = Health::default();
        let orders = health.register("orders");
        let seed = health.register("seed");
        assert!(!health.is_ready(), "not ready before the application runs");
        assert!(health.is_live());

        let shutdown = CancellationToken::new();
        health.start(&shutdown);
        orders.set_status(SubscriptionStatus::Running);
        seed.set_status(SubscriptionStatus::Running);
        assert!(health.is_ready());

        orders.receive_failed(2);
        assert!(!health.is_ready());
        orders.receive_resumed();
        assert!(health.is_ready(), "a retried receive that waits is ready");
        assert_eq!(health.report().subscriptions[0].receive_failures, 2);
        orders.receive_succeeded();
        {
            let _retry = orders.publish_retry();
            assert_eq!(health.report().subscriptions[0].publish_retries, 1);
            assert!(!health.is_ready());
        }
        assert!(health.is_ready());

        seed.set_status(SubscriptionStatus::Stopped);
        assert!(health.is_ready(), "a source that ended keeps the app ready");

        shutdown.cancel();
        let report = health.report();
        assert!(report.shutting_down && !report.ready && report.live);

        orders.set_status(SubscriptionStatus::Failed);
        assert!(!health.is_live());
    }
}
