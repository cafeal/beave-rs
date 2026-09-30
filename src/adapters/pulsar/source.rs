use super::{
    client::{SharedClient, connect, partition_topics},
    config::{PulsarSourceConfig, PulsarSubscriptionType},
    record::{PulsarMessageId, PulsarMetadata, PulsarRecord},
};
use crate::{
    codec::Decoder,
    message::{OrderingKey, SourceMessage},
    source::{Receive, ReceiveError, Source},
};
use anyhow::Context as _;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use futures_util::future::select_all;
use magnetar::{
    proto::{IncomingMessage, MessageId},
    runtime_tokio::Consumer,
};
use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// Establishes its broker client and subscription on the first `receive` call.
///
/// A partitioned topic is consumed through one consumer per partition.
pub struct PulsarSource<C, T> {
    config: PulsarSourceConfig,
    codec: Arc<C>,
    connection: Option<Connection>,
    closed: bool,
    marker: PhantomData<T>,
}

struct Connection {
    client: Arc<SharedClient>,
    consumers: Vec<Consumer>,
    subscription: Arc<str>,
    closed: Arc<AtomicBool>,
    /// The consumer polled first by the next `receive`, so that a busy
    /// partition cannot starve the others.
    next: usize,
}

impl<C: Default, T> PulsarSource<C, T> {
    pub fn new(config: PulsarSourceConfig) -> Self {
        Self::with_codec(config, C::default())
    }
}

impl<C, T> PulsarSource<C, T> {
    pub fn with_codec(config: PulsarSourceConfig, codec: C) -> Self {
        Self {
            config,
            codec: Arc::new(codec),
            connection: None,
            closed: false,
            marker: PhantomData,
        }
    }

    async fn connect(&self) -> anyhow::Result<Connection> {
        self.config.validate()?;
        let client = connect(
            &self.config.service_url,
            self.config.authentication.as_ref(),
        )
        .await?;
        let mut consumers = Vec::new();
        for topic in partition_topics(&client, &self.config.topic).await? {
            let consumer = client
                .consumer(topic)
                .subscription(&self.config.subscription)
                .subscription_type(self.config.subscription_type.into())
                .receiver_queue_size(self.config.buffer_size)
                .subscribe()
                .await?;
            consumers.push(consumer);
        }
        Ok(Connection {
            client: SharedClient::new(client),
            consumers,
            subscription: self.config.subscription.as_str().into(),
            closed: Arc::new(AtomicBool::new(false)),
            next: 0,
        })
    }
}

impl Connection {
    /// Waits for the next message of any partition. Cancel-safe: a message is
    /// taken from a consumer's queue only when its receive future completes.
    async fn receive(&mut self) -> Result<(Consumer, IncomingMessage), ReceiveError> {
        let count = self.consumers.len();
        let start = self.next % count;
        let receives = (0..count).map(|offset| {
            let index = (start + offset) % count;
            let consumer = &self.consumers[index];
            Box::pin(async move { (index, consumer.receive().await) })
        });
        let ((index, message), _, _) = select_all(receives).await;
        self.next = index + 1;
        let message = message.map_err(|error| ReceiveError::Retry(error.into()))?;
        Ok((self.consumers[index].clone(), message))
    }

    /// Closes the consumers. Deliveries still held fail to acknowledge from
    /// now on, and the client closes when the last of them is dropped.
    async fn close(self) -> anyhow::Result<()> {
        self.closed.store(true, Ordering::Release);
        let mut result = Ok(());
        for consumer in self.consumers {
            if let Err(error) = consumer.close().await {
                result = Err(error.into());
            }
        }
        result
    }
}

impl<C: Decoder<T>, T: Clone + Send + Sync + 'static> Source for PulsarSource<C, T> {
    type Message = PulsarMessage<C, T>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        if self.closed {
            return Ok(Receive::End);
        }
        if self.connection.is_none() {
            self.connection = Some(self.connect().await.map_err(ReceiveError::Fatal)?);
        }
        let connection = self.connection.as_mut().expect("connected source");
        let (consumer, message) = connection.receive().await?;
        let delivery = Delivery::new(consumer.topic(), &message).map_err(ReceiveError::Fatal)?;
        Ok(Receive::Message(PulsarMessage {
            ordering_key: ordering_key(self.config.subscription_type, &delivery),
            payload: delivery
                .payload
                .filter(|bytes| !(self.config.empty_payload_is_tombstone && bytes.is_empty())),
            key: delivery.key,
            properties: delivery.properties,
            event_time: delivery.event_time,
            metadata: delivery.metadata,
            codec: self.codec.clone(),
            acknowledgement: Acknowledgement {
                consumer,
                id: message.message_id,
                subscription: connection.subscription.clone(),
                closed: connection.closed.clone(),
                _client: connection.client.clone(),
            },
            marker: PhantomData,
        }))
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        match self.connection.take() {
            Some(connection) => connection.close().await,
            None => Ok(()),
        }
    }
}

/// The application fields and delivery facts of one received message.
struct Delivery {
    /// `None` when the producer marked the message value as null.
    payload: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
    ordering_key: Option<Vec<u8>>,
    properties: HashMap<String, String>,
    event_time: Option<u64>,
    metadata: PulsarMetadata,
}

impl Delivery {
    /// Reads the per-message fields of a batched message from its single
    /// message metadata and those of any other message from the entry's
    /// metadata.
    fn new(topic: String, message: &IncomingMessage) -> anyhow::Result<Self> {
        let entry = &message.metadata;
        let (properties, key, key_b64_encoded, ordering_key, event_time, null_value) =
            match &message.single_metadata {
                Some(single) => (
                    &single.properties,
                    &single.partition_key,
                    single.partition_key_b64_encoded,
                    &single.ordering_key,
                    single.event_time,
                    single.null_value,
                ),
                None => (
                    &entry.properties,
                    &entry.partition_key,
                    entry.partition_key_b64_encoded,
                    &entry.ordering_key,
                    entry.event_time,
                    entry.null_value,
                ),
            };
        let key = key
            .as_ref()
            .map(|key| {
                if key_b64_encoded.unwrap_or(false) {
                    BASE64
                        .decode(key)
                        .context("Pulsar message key is not valid base64")
                } else {
                    Ok(key.as_bytes().to_vec())
                }
            })
            .transpose()?;
        Ok(Self {
            payload: (!null_value.unwrap_or(false)).then(|| message.payload.to_vec()),
            key,
            ordering_key: ordering_key.as_ref().map(|key| key.to_vec()),
            properties: properties
                .iter()
                .map(|property| (property.key.clone(), property.value.clone()))
                .collect(),
            event_time,
            metadata: PulsarMetadata {
                topic,
                message_id: message_id(&message.message_id),
                publish_time: entry.publish_time,
            },
        })
    }
}

fn message_id(id: &MessageId) -> PulsarMessageId {
    PulsarMessageId {
        ledger_id: id.ledger_id,
        entry_id: id.entry_id,
        partition: id.partition,
        batch_index: id.batch_index,
    }
}

/// The scope within which the broker delivers to one consumer in order.
///
/// Exclusive and Failover subscriptions deliver each topic partition in order.
/// Key_Shared delivers each ordering key, or message key when no ordering key is
/// set, in order. Shared subscriptions and keyless Key_Shared messages have no
/// ordering scope.
fn ordering_key(
    subscription_type: PulsarSubscriptionType,
    delivery: &Delivery,
) -> Option<OrderingKey> {
    let partition = OrderingKey::new(
        delivery.metadata.topic.as_str(),
        i64::from(delivery.metadata.message_id.partition),
    );
    match subscription_type {
        PulsarSubscriptionType::Exclusive | PulsarSubscriptionType::Failover => Some(partition),
        PulsarSubscriptionType::KeyShared => delivery
            .ordering_key
            .as_deref()
            .or(delivery.key.as_deref())
            .map(|key| partition.with_key(key)),
        PulsarSubscriptionType::Shared => None,
    }
}

/// What acknowledging one delivery needs.
#[derive(Clone)]
pub(super) struct Acknowledgement {
    pub(super) consumer: Consumer,
    pub(super) id: MessageId,
    pub(super) subscription: Arc<str>,
    closed: Arc<AtomicBool>,
    _client: Arc<SharedClient>,
}

impl Acknowledgement {
    pub(super) fn ensure_open(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "Pulsar source is closed"
        );
        Ok(())
    }
}

/// One delivery; dropping it leaves the broker message unacknowledged.
pub struct PulsarMessage<C, T> {
    ordering_key: Option<OrderingKey>,
    payload: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
    properties: HashMap<String, String>,
    event_time: Option<u64>,
    metadata: PulsarMetadata,
    codec: Arc<C>,
    pub(super) acknowledgement: Acknowledgement,
    marker: PhantomData<T>,
}

impl<C, T> PulsarMessage<C, T> {
    pub(super) fn topic(&self) -> &str {
        &self.metadata.topic
    }
}

impl<C: Decoder<T>, T: Clone + Send + Sync + 'static> SourceMessage for PulsarMessage<C, T> {
    type Item = PulsarRecord<T>;
    type Raw = PulsarRecord<Vec<u8>>;

    fn decode(&self) -> anyhow::Result<Self::Item> {
        Ok(PulsarRecord {
            value: self
                .payload
                .as_deref()
                .map(|bytes| self.codec.decode(bytes))
                .transpose()?,
            key: self.key.clone(),
            properties: self.properties.clone(),
            event_time: self.event_time,
            metadata: self.metadata.clone(),
        })
    }

    fn raw(&self) -> Self::Raw {
        PulsarRecord {
            value: self.payload.clone(),
            key: self.key.clone(),
            properties: self.properties.clone(),
            event_time: self.event_time,
            metadata: self.metadata.clone(),
        }
    }

    async fn ack(self) -> anyhow::Result<()> {
        self.acknowledgement.ensure_open()?;
        self.acknowledgement
            .consumer
            .ack(self.acknowledgement.id)
            .await?;
        Ok(())
    }

    fn ordering_key(&self) -> Option<OrderingKey> {
        self.ordering_key.clone()
    }

    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        self.properties
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use magnetar::proto::pb::{KeyValue, MessageMetadata, SingleMessageMetadata};
    use std::time::Instant;

    fn incoming(
        metadata: MessageMetadata,
        single: Option<SingleMessageMetadata>,
    ) -> IncomingMessage {
        let mut message_id = MessageId::EARLIEST;
        message_id.ledger_id = 7;
        message_id.entry_id = 3;
        message_id.partition = 1;
        if single.is_some() {
            message_id.batch_index = 0;
            message_id.batch_size = 1;
        }
        IncomingMessage {
            message_id,
            metadata: Arc::new(metadata),
            single_metadata: single,
            payload: Default::default(),
            redelivery_count: 0,
            broker_entry_metadata: None,
            arrived_at: Instant::now(),
        }
    }

    #[test]
    fn deliveries_decode_base64_keys_and_null_markers() {
        let metadata = MessageMetadata {
            partition_key: Some("AAEC/f7/".into()),
            partition_key_b64_encoded: Some(true),
            null_value: Some(true),
            publish_time: 41,
            ..MessageMetadata::default()
        };
        let delivery =
            Delivery::new("orders-partition-1".into(), &incoming(metadata, None)).unwrap();
        assert_eq!(delivery.key, Some(vec![0, 1, 2, 253, 254, 255]));
        assert_eq!(delivery.payload, None);
        assert_eq!(delivery.metadata.publish_time, 41);
        assert_eq!(
            delivery.metadata.message_id,
            PulsarMessageId {
                ledger_id: 7,
                entry_id: 3,
                partition: 1,
                batch_index: -1,
            }
        );
    }

    #[test]
    fn batched_deliveries_read_their_single_message_metadata() {
        let metadata = MessageMetadata {
            partition_key: Some("entry".into()),
            event_time: Some(1),
            ..MessageMetadata::default()
        };
        let single = SingleMessageMetadata {
            partition_key: Some("customer".into()),
            properties: vec![KeyValue {
                key: "kind".into(),
                value: "order".into(),
            }],
            event_time: Some(42),
            null_value: Some(true),
            ..SingleMessageMetadata::default()
        };
        let delivery = Delivery::new("orders".into(), &incoming(metadata, Some(single))).unwrap();
        assert_eq!(delivery.key.as_deref(), Some(b"customer".as_slice()));
        assert_eq!(delivery.event_time, Some(42));
        assert_eq!(
            delivery.properties.get("kind").map(String::as_str),
            Some("order")
        );
        assert_eq!(delivery.payload, None);
        assert_eq!(delivery.metadata.message_id.batch_index, 0);
    }

    fn delivery(key: Option<&[u8]>, ordering_key: Option<&[u8]>) -> Delivery {
        Delivery {
            payload: Some(vec![]),
            key: key.map(<[u8]>::to_vec),
            ordering_key: ordering_key.map(<[u8]>::to_vec),
            properties: HashMap::new(),
            event_time: None,
            metadata: PulsarMetadata {
                topic: "persistent://public/default/orders-partition-2".into(),
                message_id: PulsarMessageId {
                    ledger_id: 0,
                    entry_id: 0,
                    partition: 2,
                    batch_index: -1,
                },
                publish_time: 0,
            },
        }
    }

    #[test]
    fn ordering_scope_follows_the_subscription_type() {
        let partition = OrderingKey::new("persistent://public/default/orders-partition-2", 2);
        let delivery = delivery(Some(b"customer"), None);
        assert_eq!(
            ordering_key(PulsarSubscriptionType::Exclusive, &delivery),
            Some(partition.clone())
        );
        assert_eq!(
            ordering_key(PulsarSubscriptionType::Failover, &delivery),
            Some(partition.clone())
        );
        assert_eq!(
            ordering_key(PulsarSubscriptionType::Shared, &delivery),
            None
        );
        assert_eq!(
            ordering_key(PulsarSubscriptionType::KeyShared, &delivery),
            Some(partition.clone().with_key(b"customer".as_slice()))
        );
    }

    #[test]
    fn key_shared_prefers_the_ordering_key_and_ignores_keyless_messages() {
        let partition = OrderingKey::new("persistent://public/default/orders-partition-2", 2);
        assert_eq!(
            ordering_key(
                PulsarSubscriptionType::KeyShared,
                &delivery(Some(b"customer"), Some(b"order"))
            ),
            Some(partition.with_key(b"order".as_slice()))
        );
        assert_eq!(
            ordering_key(PulsarSubscriptionType::KeyShared, &delivery(None, None)),
            None
        );
    }
}
