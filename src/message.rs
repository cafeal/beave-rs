//! Received message ownership, decoding, ordering scope, and acknowledgement.
use crate::error::BoxError;
use crate::shutdown::CancellationToken;
use serde::Serialize;
use std::{borrow::Cow, fmt::Debug, future::Future, pin::Pin, sync::Arc};

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
    /// Name of the scope, such as a topic.
    pub fn scope(&self) -> &str {
        &self.scope
    }
    /// Index within the scope, such as a partition number.
    pub fn index(&self) -> i64 {
        self.index
    }
    /// Message key the scope is narrowed to, if any.
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }
}

/// A delivery owns its ACK capability; handlers only receive decoded values.
/// Decode must not acknowledge or publish. Dropping a message must never ACK it.
/// Implementations may retain raw bytes and broker-specific metadata internally.
pub trait SourceMessage: Send + 'static {
    /// Decoded value passed to the handler.
    type Item: Clone + Send + Sync + 'static;
    /// Decode the payload. Called once per delivery, before any handler attempt.
    fn decode(&self) -> Result<Self::Item, BoxError>;
    /// Success means the adapter safely recorded completion. Broker commit ordering
    /// and assignment validity remain the adapter's responsibility.
    fn ack(self) -> impl Future<Output = Result<(), BoxError>> + Send;
    /// Undecoded form of this delivery, copied into dead letters so failures keep the
    /// original payload and broker metadata even when decoding failed. It is
    /// serializable so that dead letters can be published as they are, and so
    /// that a [`channel`](crate::channel) can keep it for the next subscription.
    type Raw: Debug + Serialize + Send + Sync + 'static;
    /// Copy of the undecoded delivery.
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
    /// A token the source cancels when it is handing this delivery's scope to
    /// another consumer but still lets work in progress finish, as in the Kafka
    /// source's graceful handoff. The runtime then abandons the delivery
    /// without ACK if it has not started, and lets it finish otherwise. The
    /// source cancels [`revocation`](Self::revocation) when the handoff ends.
    fn handoff(&self) -> Option<CancellationToken> {
        None
    }
    /// Text-map fields received with this delivery that can carry trace context,
    /// such as a W3C `traceparent` Kafka header or Pulsar property. With the
    /// `opentelemetry` feature, the runtime extracts the delivery span's remote
    /// parent from them. Sources without such metadata return nothing.
    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        Vec::new()
    }
    /// What this delivery is in OpenTelemetry messaging terms, such as its
    /// broker, topic, partition, and offset. The runtime records the attributes
    /// on the delivery's `message` span. Sources that are not message brokers
    /// return `None`.
    fn messaging_attributes(&self) -> Option<MessagingAttributes<'_>> {
        None
    }
}

/// OpenTelemetry messaging semantic-convention attributes of one delivery.
///
/// Built by a [`SourceMessage`] from its own metadata and recorded by the
/// runtime on the delivery's `message` span, together with
/// `messaging.operation.type` and `messaging.operation.name` set to `process`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessagingAttributes<'a> {
    pub(crate) system: &'static str,
    pub(crate) destination: Option<&'a str>,
    pub(crate) partition: Option<i64>,
    pub(crate) kafka_offset: Option<i64>,
    pub(crate) message_id: Option<Cow<'a, str>>,
    pub(crate) consumer_group: Option<&'a str>,
    pub(crate) subscription: Option<&'a str>,
}

impl<'a> MessagingAttributes<'a> {
    /// Attributes of a delivery from `system`, the `messaging.system` value
    /// such as `kafka`, `pulsar`, `rabbitmq`, `aws_sqs`, `gcp_pubsub`, or
    /// `servicebus`.
    pub fn new(system: &'static str) -> Self {
        Self {
            system,
            destination: None,
            partition: None,
            kafka_offset: None,
            message_id: None,
            consumer_group: None,
            subscription: None,
        }
    }
    /// `messaging.destination.name`: the topic or queue the delivery came from.
    pub fn destination(mut self, name: &'a str) -> Self {
        self.destination = Some(name);
        self
    }
    /// `messaging.destination.partition.id`.
    pub fn partition(mut self, id: i64) -> Self {
        self.partition = Some(id);
        self
    }
    /// `messaging.kafka.offset`.
    pub fn kafka_offset(mut self, offset: i64) -> Self {
        self.kafka_offset = Some(offset);
        self
    }
    /// `messaging.message.id`.
    pub fn message_id(mut self, id: impl Into<Cow<'a, str>>) -> Self {
        self.message_id = Some(id.into());
        self
    }
    /// `messaging.consumer.group.name`, such as a Kafka consumer group.
    pub fn consumer_group(mut self, name: &'a str) -> Self {
        self.consumer_group = Some(name);
        self
    }
    /// `messaging.destination.subscription.name`, such as a Pulsar, Pub/Sub,
    /// or Service Bus subscription.
    pub fn subscription(mut self, name: &'a str) -> Self {
        self.subscription = Some(name);
        self
    }
}

type AckFuture = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;
type Acknowledge = Box<dyn FnOnce() -> AckFuture + Send>;

/// Acknowledgement belongs to the delivery, not the handler's payload.
///
/// `R` is the delivery's [`Raw`](SourceMessage::Raw) form, `()` unless set with
/// [`with_raw`](Self::with_raw).
pub struct Delivery<T, R = ()> {
    /// The delivered value, passed to the handler as is.
    pub value: T,
    ack: Acknowledge,
    ordering_key: Option<OrderingKey>,
    revocation: Option<CancellationToken>,
    handoff: Option<CancellationToken>,
    propagation: Vec<(String, String)>,
    raw: R,
}
impl<T> Delivery<T> {
    /// A delivery whose acknowledgement runs `ack`.
    pub fn new<F, Fut>(value: T, ack: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        Self {
            value,
            ack: Box::new(|| Box::pin(ack())),
            ordering_key: None,
            revocation: None,
            handoff: None,
            propagation: Vec::new(),
            raw: (),
        }
    }
    /// A delivery whose acknowledgement does nothing.
    pub fn untracked(value: T) -> Self {
        Self::new(value, || async { Ok(()) })
    }
}
impl<T, R> Delivery<T, R> {
    /// The form dead letters of this delivery carry as their raw delivery.
    pub fn with_raw<U>(self, raw: U) -> Delivery<T, U> {
        Delivery {
            value: self.value,
            ack: self.ack,
            ordering_key: self.ordering_key,
            revocation: self.revocation,
            handoff: self.handoff,
            propagation: self.propagation,
            raw,
        }
    }
    /// Set the ordering scope reported by [`SourceMessage::ordering_key`].
    pub fn with_ordering_key(mut self, key: OrderingKey) -> Self {
        self.ordering_key = Some(key);
        self
    }
    /// Set the token reported by [`SourceMessage::revocation`].
    pub fn with_revocation(mut self, token: CancellationToken) -> Self {
        self.revocation = Some(token);
        self
    }
    /// Set the token reported by [`SourceMessage::handoff`].
    pub fn with_handoff(mut self, token: CancellationToken) -> Self {
        self.handoff = Some(token);
        self
    }
    /// Text-map fields, such as a W3C `traceparent`, from which the runtime
    /// continues the trace of the code that produced this value.
    pub fn with_propagation_fields(mut self, fields: Vec<(String, String)>) -> Self {
        self.propagation = fields;
        self
    }
    /// Run the acknowledgement.
    pub async fn ack(self) -> Result<(), BoxError> {
        (self.ack)().await
    }
}

impl<T, R> SourceMessage for Delivery<T, R>
where
    T: Clone + Send + Sync + 'static,
    R: Clone + Debug + Serialize + Send + Sync + 'static,
{
    type Item = T;
    /// Already typed local input has no separate undecoded form unless one was
    /// set with [`Delivery::with_raw`].
    type Raw = R;
    fn decode(&self) -> Result<T, BoxError> {
        Ok(self.value.clone())
    }
    fn raw(&self) -> R {
        self.raw.clone()
    }
    async fn ack(self) -> Result<(), BoxError> {
        Delivery::ack(self).await
    }
    fn ordering_key(&self) -> Option<OrderingKey> {
        self.ordering_key.clone()
    }
    fn revocation(&self) -> Option<CancellationToken> {
        self.revocation.clone()
    }
    fn handoff(&self) -> Option<CancellationToken> {
        self.handoff.clone()
    }
    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        self.propagation
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect()
    }
}
