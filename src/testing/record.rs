use crate::{codec::Decoder, message::OrderingKey};
use serde::Serialize;
use std::fmt::Debug;

#[cfg(feature = "kafka")]
use crate::adapters::kafka::{KafkaMetadata, KafkaRecord, text_headers};
#[cfg(feature = "pulsar")]
use crate::adapters::pulsar::{PulsarMessageId, PulsarMetadata, PulsarRecord};
#[cfg(feature = "rabbitmq")]
use crate::adapters::rabbitmq::{
    RabbitMqHeaders, RabbitMqMetadata, RabbitMqProperties, RabbitMqRecord,
    text_headers as rabbitmq_text_headers,
};
#[cfg(feature = "sqs")]
use crate::adapters::sqs::{SqsAttributes, SqsMetadata, SqsRecord, text_attributes};
#[cfg(feature = "pulsar")]
use std::collections::HashMap;

/// A received record with its payload left undecoded, as a
/// [`TestSource`](super::TestSource) delivers it.
///
/// The record is also the delivery's [`SourceMessage::Raw`] form, so dead
/// letters carry it exactly as the adapter's own raw record would.
///
/// [`SourceMessage::Raw`]: crate::message::SourceMessage::Raw
pub trait TestRecord: Clone + Debug + Serialize + Send + Sync + 'static {
    /// The handler input decoded from this record, such as `KafkaRecord<T>`
    /// for a `KafkaRecord<Vec<u8>>`.
    type Decoded<T: Clone + Send + Sync + 'static>: Clone + Send + Sync + 'static;

    /// Decode the payload with the source's codec, keeping the metadata.
    fn decode<C, T>(&self, codec: &C) -> anyhow::Result<Self::Decoded<T>>
    where
        C: Decoder<T>,
        T: Clone + Send + Sync + 'static;

    /// The ordering scope the owning adapter assigns to this record.
    fn ordering_key(&self) -> Option<OrderingKey> {
        None
    }

    /// The trace-context fields the owning adapter reads from this record.
    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        Vec::new()
    }
}

/// A bare payload without record metadata. Deliveries have no ordering scope.
impl TestRecord for Vec<u8> {
    type Decoded<T: Clone + Send + Sync + 'static> = T;

    fn decode<C, T>(&self, codec: &C) -> anyhow::Result<T>
    where
        C: Decoder<T>,
        T: Clone + Send + Sync + 'static,
    {
        codec.decode(self)
    }
}

/// Decodes like a [`KafkaSource`](crate::adapters::kafka::KafkaSource): a null
/// value stays `None`. The ordering scope is the topic partition, and UTF-8
/// headers carry trace context.
#[cfg(feature = "kafka")]
impl TestRecord for KafkaRecord<Vec<u8>> {
    type Decoded<T: Clone + Send + Sync + 'static> = KafkaRecord<T>;

    fn decode<C, T>(&self, codec: &C) -> anyhow::Result<KafkaRecord<T>>
    where
        C: Decoder<T>,
        T: Clone + Send + Sync + 'static,
    {
        Ok(KafkaRecord {
            key: self.key.clone(),
            value: self
                .value
                .as_deref()
                .map(|bytes| codec.decode(bytes))
                .transpose()?,
            headers: self.headers.clone(),
            metadata: self.metadata.clone(),
        })
    }

    fn ordering_key(&self) -> Option<OrderingKey> {
        Some(OrderingKey::new(
            self.metadata.topic.as_str(),
            i64::from(self.metadata.partition),
        ))
    }

    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        text_headers(
            self.headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_deref())),
        )
    }
}

/// Decodes like a [`PulsarSource`](crate::adapters::pulsar::PulsarSource): a
/// null value stays `None`. The ordering scope is the topic partition, as for
/// Exclusive and Failover subscriptions, and properties carry trace context.
#[cfg(feature = "pulsar")]
impl TestRecord for PulsarRecord<Vec<u8>> {
    type Decoded<T: Clone + Send + Sync + 'static> = PulsarRecord<T>;

    fn decode<C, T>(&self, codec: &C) -> anyhow::Result<PulsarRecord<T>>
    where
        C: Decoder<T>,
        T: Clone + Send + Sync + 'static,
    {
        Ok(PulsarRecord {
            value: self
                .value
                .as_deref()
                .map(|bytes| codec.decode(bytes))
                .transpose()?,
            key: self.key.clone(),
            properties: self.properties.clone(),
            event_time: self.event_time,
            metadata: self.metadata.clone(),
        })
    }

    fn ordering_key(&self) -> Option<OrderingKey> {
        Some(OrderingKey::new(
            self.metadata.topic.as_str(),
            i64::from(self.metadata.message_id.partition),
        ))
    }

    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        self.properties
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect()
    }
}

/// Decodes like a [`RabbitMqSource`](crate::adapters::rabbitmq::RabbitMqSource).
/// Deliveries have no ordering scope, as for a source that is not
/// [`ordered`](crate::adapters::rabbitmq::RabbitMqSourceConfig::ordered), and
/// string headers carry trace context.
#[cfg(feature = "rabbitmq")]
impl TestRecord for RabbitMqRecord<Vec<u8>> {
    type Decoded<T: Clone + Send + Sync + 'static> = RabbitMqRecord<T>;

    fn decode<C, T>(&self, codec: &C) -> anyhow::Result<RabbitMqRecord<T>>
    where
        C: Decoder<T>,
        T: Clone + Send + Sync + 'static,
    {
        Ok(RabbitMqRecord {
            value: codec.decode(&self.value)?,
            headers: self.headers.clone(),
            properties: self.properties.clone(),
            metadata: self.metadata.clone(),
        })
    }

    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        rabbitmq_text_headers(&self.headers)
    }
}

/// Decodes like an [`SqsSource`](crate::adapters::sqs::SqsSource). A FIFO
/// queue message, whose queue URL ends in `.fifo`, is ordered by its message
/// group, and `String` attributes carry trace context.
#[cfg(feature = "sqs")]
impl TestRecord for SqsRecord<Vec<u8>> {
    type Decoded<T: Clone + Send + Sync + 'static> = SqsRecord<T>;

    fn decode<C, T>(&self, codec: &C) -> anyhow::Result<SqsRecord<T>>
    where
        C: Decoder<T>,
        T: Clone + Send + Sync + 'static,
    {
        Ok(SqsRecord {
            value: codec.decode(&self.value)?,
            attributes: self.attributes.clone(),
            metadata: self.metadata.clone(),
        })
    }

    fn ordering_key(&self) -> Option<OrderingKey> {
        let group = self.metadata.message_group_id.as_deref()?;
        self.metadata.queue_url.ends_with(".fifo").then(|| {
            OrderingKey::new(self.metadata.queue_url.as_str(), 0).with_key(group.as_bytes())
        })
    }

    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        text_attributes(&self.attributes)
    }
}

/// A Kafka record at `offset` of a topic partition, without key, headers, or
/// timestamp. Set the public fields for anything else, such as `value = None`
/// for a tombstone.
#[cfg(feature = "kafka")]
pub fn kafka_record(
    topic: impl Into<String>,
    partition: i32,
    offset: i64,
    value: impl Into<Vec<u8>>,
) -> KafkaRecord<Vec<u8>> {
    KafkaRecord {
        key: None,
        value: Some(value.into()),
        headers: Vec::new(),
        metadata: KafkaMetadata {
            topic: topic.into(),
            partition,
            offset,
            timestamp: None,
        },
    }
}

/// A Pulsar message at `entry_id` of ledger 0 on a non-partitioned topic,
/// without key, properties, or event time, published at time 0. Set the public
/// fields for anything else, such as `metadata.message_id.partition` for a
/// partition topic or `value = None` for a tombstone.
#[cfg(feature = "pulsar")]
pub fn pulsar_record(
    topic: impl Into<String>,
    entry_id: u64,
    value: impl Into<Vec<u8>>,
) -> PulsarRecord<Vec<u8>> {
    PulsarRecord {
        value: Some(value.into()),
        key: None,
        properties: HashMap::new(),
        event_time: None,
        metadata: PulsarMetadata {
            topic: topic.into(),
            message_id: PulsarMessageId {
                ledger_id: 0,
                entry_id,
                partition: -1,
                batch_index: -1,
            },
            publish_time: 0,
        },
    }
}

/// A RabbitMQ message with `delivery_tag`, consumed from `queue` after being
/// published to the default exchange with the queue name as its routing key,
/// persistent and not redelivered, without headers or properties. Set the
/// public fields for anything else.
#[cfg(feature = "rabbitmq")]
pub fn rabbitmq_record(
    queue: impl Into<String>,
    delivery_tag: u64,
    value: impl Into<Vec<u8>>,
) -> RabbitMqRecord<Vec<u8>> {
    let queue = queue.into();
    RabbitMqRecord {
        value: value.into(),
        headers: RabbitMqHeaders::new(),
        properties: RabbitMqProperties::default(),
        metadata: RabbitMqMetadata {
            routing_key: queue.clone(),
            queue,
            exchange: String::new(),
            redelivered: false,
            persistent: true,
            delivery_tag,
        },
    }
}

/// An SQS message with `message_id` received from `queue_url` for the first
/// time, without attributes, timestamps, or FIFO fields. Set the public fields
/// for anything else, such as `metadata.message_group_id` on a FIFO queue.
#[cfg(feature = "sqs")]
pub fn sqs_record(
    queue_url: impl Into<String>,
    message_id: impl Into<String>,
    value: impl Into<Vec<u8>>,
) -> SqsRecord<Vec<u8>> {
    SqsRecord {
        value: value.into(),
        attributes: SqsAttributes::new(),
        metadata: SqsMetadata {
            queue_url: queue_url.into(),
            message_id: message_id.into(),
            receive_count: 1,
            sent_timestamp: None,
            first_receive_timestamp: None,
            message_group_id: None,
            deduplication_id: None,
            sequence_number: None,
        },
    }
}
