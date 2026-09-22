//! Runtime policy belongs to the subscription, not the transport adapters.
use crate::retry::RetryPolicy;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct SubscriptionConfig {
    pub name: String,
    pub concurrency: usize,
    pub max_in_flight: usize,
    pub handler_retry: RetryPolicy,
    pub receive_retry: RetryPolicy,
    pub publish_retry: RetryPolicy,
    pub drain_timeout: Duration,
}
impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            name: "subscription".into(),
            concurrency: 1,
            max_in_flight: 64,
            handler_retry: RetryPolicy::default(),
            receive_retry: RetryPolicy::default(),
            publish_retry: RetryPolicy::default(),
            drain_timeout: Duration::from_secs(30),
        }
    }
}
impl SubscriptionConfig {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.concurrency > 0 && self.max_in_flight > 0,
            "concurrency and max_in_flight must be positive"
        );
        self.handler_retry.validate()?;
        self.receive_retry.validate()?;
        self.publish_retry.validate()?;
        Ok(())
    }
}
