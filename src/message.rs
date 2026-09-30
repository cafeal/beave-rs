//! Received message ownership, decoding, ordering scope, and acknowledgement.
use crate::shutdown::CancellationToken;
use std::{future::Future, pin::Pin, sync::Arc};

/// Identifies a source-defined ordering scope, such as a Kafka topic partition.
///
/// Deliveries with equal keys are processed sequentially under
/// [`ProcessingOrder::PerKey`](crate::subscription::ProcessingOrder::PerKey).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OrderingKey {
    scope: Arc<str>,
    index: i64,
}

impl OrderingKey {
    pub fn new(scope: impl Into<Arc<str>>, index: i64) -> Self {
        Self {
            scope: scope.into(),
            index,
        }
    }
    pub fn scope(&self) -> &str {
        &self.scope
    }
    pub fn index(&self) -> i64 {
        self.index
    }
}

/// A delivery owns its ACK capability; handlers only receive decoded values.
/// Decode must not acknowledge or publish. Dropping a message must never ACK it.
/// Implementations may retain raw bytes and broker-specific metadata internally.
pub trait SourceMessage: Send + 'static {
    type Item: Clone + Send + Sync + 'static;
    fn decode(&self) -> anyhow::Result<Self::Item>;
    /// Success means the adapter safely recorded completion. Broker commit ordering
    /// and assignment validity remain the adapter's responsibility.
    fn ack(self) -> impl Future<Output = anyhow::Result<()>> + Send;
    /// The scope within which the source delivers in order. `None` means the
    /// delivery has no ordering relationship with other deliveries.
    fn ordering_key(&self) -> Option<OrderingKey> {
        None
    }
    /// A token the source cancels when it no longer owns this delivery, for
    /// example after a partition revocation. The runtime then abandons the
    /// delivery without ACK and without treating it as a failure.
    fn revocation(&self) -> Option<CancellationToken> {
        None
    }
}

type AckFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
type Acknowledge = Box<dyn FnOnce() -> AckFuture + Send>;

/// Acknowledgement belongs to the delivery, not the handler's payload.
pub struct Delivery<T> {
    pub value: T,
    ack: Acknowledge,
    ordering_key: Option<OrderingKey>,
    revocation: Option<CancellationToken>,
}
impl<T> Delivery<T> {
    pub fn new<F, Fut>(value: T, ack: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        Self {
            value,
            ack: Box::new(|| Box::pin(ack())),
            ordering_key: None,
            revocation: None,
        }
    }
    pub fn untracked(value: T) -> Self {
        Self::new(value, || async { Ok(()) })
    }
    pub fn with_ordering_key(mut self, key: OrderingKey) -> Self {
        self.ordering_key = Some(key);
        self
    }
    pub fn with_revocation(mut self, token: CancellationToken) -> Self {
        self.revocation = Some(token);
        self
    }
    pub async fn ack(self) -> anyhow::Result<()> {
        (self.ack)().await
    }
}

impl<T: Clone + Send + Sync + 'static> SourceMessage for Delivery<T> {
    type Item = T;
    fn decode(&self) -> anyhow::Result<T> {
        Ok(self.value.clone())
    }
    async fn ack(self) -> anyhow::Result<()> {
        Delivery::ack(self).await
    }
    fn ordering_key(&self) -> Option<OrderingKey> {
        self.ordering_key.clone()
    }
    fn revocation(&self) -> Option<CancellationToken> {
        self.revocation.clone()
    }
}
