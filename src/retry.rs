//! Bounded exponential-backoff policy shared by independent retry domains.

use std::time::Duration;

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Includes the initial attempt; must be at least one.
    pub max_attempts: usize,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
        }
    }
}
impl RetryPolicy {
    pub(crate) fn delay(&self, failures: usize) -> Duration {
        self.initial_delay
            .saturating_mul(2u32.saturating_pow(failures.saturating_sub(1).min(31) as u32))
            .min(self.max_delay)
    }
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.max_attempts > 0, "max_attempts must be positive");
        Ok(())
    }
}
