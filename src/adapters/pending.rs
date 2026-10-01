//! Bound on outputs a broker sink has submitted and not yet seen confirmed.
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// One permit per submitted output awaiting its broker confirmation, such as a
/// delivery report or receipt. Pending completions do not count toward a
/// subscription's `max_in_flight`, so a sink bounds them here and `submit`
/// waits for a permit.
pub(crate) struct PendingLimit {
    permits: Arc<Semaphore>,
    broker: &'static str,
}

impl PendingLimit {
    pub(crate) fn new(max_pending: usize, broker: &'static str) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max_pending.min(Semaphore::MAX_PERMITS))),
            broker,
        }
    }

    /// Checks a configured `max_pending` without creating a limit.
    pub(crate) fn validate(max_pending: usize, broker: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=Semaphore::MAX_PERMITS).contains(&max_pending),
            "{broker} sink max_pending must be between 1 and {}",
            Semaphore::MAX_PERMITS
        );
        Ok(())
    }

    /// Waits for a free slot, held until the returned permit is dropped. Fails
    /// once the limit is closed.
    pub(crate) async fn acquire(&self) -> anyhow::Result<OwnedSemaphorePermit> {
        self.permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("{} sink is closed", self.broker))
    }

    /// Fails every current and later `acquire`.
    pub(crate) fn close(&self) {
        self.permits.close();
    }
}

#[cfg(test)]
mod tests {
    use super::PendingLimit;

    #[tokio::test]
    async fn closing_fails_waiting_and_later_acquires() {
        let limit = PendingLimit::new(1, "Test");
        let held = limit.acquire().await.unwrap();
        let waiting = limit.acquire();
        limit.close();
        assert_eq!(
            waiting.await.unwrap_err().to_string(),
            "Test sink is closed"
        );
        drop(held);
        assert!(limit.acquire().await.is_err());
    }

    #[test]
    fn validation_rejects_zero() {
        assert!(PendingLimit::validate(0, "Test").is_err());
        assert!(PendingLimit::validate(1, "Test").is_ok());
    }
}
