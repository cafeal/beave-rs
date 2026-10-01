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

/// How a transactional subscription groups deliveries into sink transactions.
///
/// A batch opens with the first delivery that completes processing and closes
/// when it holds `max_deliveries` deliveries or `max_linger` after that first
/// delivery, whichever comes first. Batches commit one at a time; deliveries
/// that complete while a batch commits form the next one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransactionBatch {
    /// Maximum number of deliveries committed in one transaction.
    pub max_deliveries: usize,
    /// Maximum time the first delivery of a batch waits for more deliveries
    /// before the batch commits.
    pub max_linger: Duration,
}

impl TransactionBatch {
    pub fn new(max_deliveries: usize, max_linger: Duration) -> Self {
        Self {
            max_deliveries,
            max_linger,
        }
    }

    /// One transaction per delivery, committed without waiting.
    pub fn single() -> Self {
        Self::new(1, Duration::ZERO)
    }

    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.max_deliveries > 0,
            "transaction batch size must be positive"
        );
        Ok(())
    }
}

impl Default for TransactionBatch {
    fn default() -> Self {
        Self::new(100, Duration::from_millis(10))
    }
}

#[derive(Clone, Debug)]
pub struct SubscriptionConfig {
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
    /// Batching of a [transactional](crate::Subscription::transactional)
    /// subscription's commits; ignored by other subscriptions.
    pub transaction_batch: TransactionBatch,
}
impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            concurrency: 1,
            max_in_flight: 64,
            ordering: ProcessingOrder::default(),
            handler_retry: RetryPolicy::default(),
            receive_retry: RetryPolicy::default(),
            publish_retry: RetryPolicy::default(),
            dead_letter_retry: RetryPolicy::default(),
            error_policy: ErrorPolicy::default(),
            drain_timeout: Duration::from_secs(30),
            transaction_batch: TransactionBatch::default(),
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
        self.transaction_batch.validate()
    }
}
