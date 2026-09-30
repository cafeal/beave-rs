//! Runtime policy belongs to the subscription, not the transport adapters.
use crate::{error_policy::ErrorPolicy, retry::RetryPolicy};
use std::time::Duration;

/// How deliveries that share an [`OrderingKey`](crate::message::OrderingKey) are scheduled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ProcessingOrder {
    /// Process deliveries with the same ordering key one at a time, in receive
    /// order. Deliveries with different keys, or without a key, run in parallel.
    #[default]
    PerKey,
    /// Ignore ordering keys. Deliveries from one ordering scope may complete out
    /// of order; source adapters still acknowledge only safe progress.
    Unordered,
}

#[derive(Clone, Debug)]
pub struct SubscriptionConfig {
    pub name: String,
    /// Maximum number of deliveries processed at the same time.
    pub concurrency: usize,
    /// Maximum number of received deliveries that are unfinished, including
    /// deliveries waiting behind an earlier delivery with the same ordering key.
    pub max_in_flight: usize,
    pub ordering: ProcessingOrder,
    pub handler_retry: RetryPolicy,
    pub receive_retry: RetryPolicy,
    pub publish_retry: RetryPolicy,
    pub dead_letter_retry: RetryPolicy,
    pub error_policy: ErrorPolicy,
    pub drain_timeout: Duration,
}
impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            name: "subscription".into(),
            concurrency: 1,
            max_in_flight: 64,
            ordering: ProcessingOrder::default(),
            handler_retry: RetryPolicy::default(),
            receive_retry: RetryPolicy::default(),
            publish_retry: RetryPolicy::default(),
            dead_letter_retry: RetryPolicy::default(),
            error_policy: ErrorPolicy::default(),
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
        self.dead_letter_retry.validate()?;
        Ok(())
    }
}
