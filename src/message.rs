//! Received message ownership, decoding, ordering scope, and acknowledgement.
use crate::shutdown::CancellationToken;
use std::{future::Future, pin::Pin, sync::Arc};

/// Identifies a source-defined ordering scope, such as a Kafka topic partition
/// or a message key within a Pulsar topic partition.
///
/// Deliveries with equal keys are processed sequentially under
/// [`ProcessingOrder::PerKey`](crate::subscription::ProcessingOrder::PerKey).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OrderingKey {
    scope: Arc<str>,
    index: i64,
    key: Option<Arc<[u8]>>,
}

impl OrderingKey {
    /// A whole partition-like scope, identified by a name and an index.
    pub fn new(scope: impl Into<Arc<str>>, index: i64) -> Self {
        Self {
            scope: scope.into(),
            index,
            key: None,
        }
    }
    /// Narrows the scope to one message key within it.
    pub fn with_key(mut self, key: impl Into<Arc<[u8]>>) -> Self {
        self.key = Some(key.into());
        self
    }
    pub fn scope(&self) -> &str {
        &self.scope
    }
    pub fn index(&self) -> i64 {
        self.index
    }
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
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
    /// Undecoded form of this delivery, copied into dead letters so failures keep the
    /// original payload and broker metadata even when decoding failed.
    type Raw: Send + Sync + 'static;
    fn raw(&self) -> Self::Raw;
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
    /// Text-map fields received with this delivery that can carry trace context,
    /// such as a W3C `traceparent` Kafka header or Pulsar property. With the
    /// `opentelemetry` feature, the runtime extracts the delivery span's remote
    /// parent from them. Sources without such metadata return nothing.
    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        Vec::new()
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
    propagation: Vec<(String, String)>,
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
            propagation: Vec::new(),
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
    /// Text-map fields, such as a W3C `traceparent`, from which the runtime
    /// continues the trace of the code that produced this value.
    pub fn with_propagation_fields(mut self, fields: Vec<(String, String)>) -> Self {
        self.propagation = fields;
        self
    }
    pub async fn ack(self) -> anyhow::Result<()> {
        (self.ack)().await
    }
}

impl<T: Clone + Send + Sync + 'static> SourceMessage for Delivery<T> {
    type Item = T;
    /// Already typed local input has no separate undecoded form.
    type Raw = ();
    fn decode(&self) -> anyhow::Result<T> {
        Ok(self.value.clone())
    }
    fn raw(&self) {}
    async fn ack(self) -> anyhow::Result<()> {
        Delivery::ack(self).await
    }
    fn ordering_key(&self) -> Option<OrderingKey> {
        self.ordering_key.clone()
    }
    fn revocation(&self) -> Option<CancellationToken> {
        self.revocation.clone()
    }
    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        self.propagation
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect()
    }
}
