//! Received message ownership, decoding, and acknowledgement.
use std::{future::Future, pin::Pin};

/// A delivery owns its ACK capability; handlers only receive decoded values.
/// Decode must not acknowledge or publish. Dropping a message must never ACK it.
/// Implementations may retain raw bytes and broker-specific metadata internally.
pub trait SourceMessage: Send + 'static {
    type Item: Clone + Send + Sync + 'static;
    fn decode(&self) -> anyhow::Result<Self::Item>;
    /// Success means the adapter safely recorded completion. Broker commit ordering
    /// and assignment validity remain the adapter's responsibility.
    fn ack(self) -> impl Future<Output = anyhow::Result<()>> + Send;
}

type AckFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
type Acknowledge = Box<dyn FnOnce() -> AckFuture + Send>;

/// Acknowledgement belongs to the delivery, not the handler's payload.
pub struct Delivery<T> {
    pub value: T,
    ack: Acknowledge,
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
        }
    }
    pub fn untracked(value: T) -> Self {
        Self::new(value, || async { Ok(()) })
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
}
