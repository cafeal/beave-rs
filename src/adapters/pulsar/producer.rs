use super::{
    client::{connect, partition_topics},
    config::PulsarSinkConfig,
    sink::PulsarPrepared,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use magnetar::{
    PulsarClient, java_string_hash,
    proto::{TxnId, pb::KeyValue, pb::MessageMetadata, producer::OutgoingMessage},
    runtime_tokio::{Producer, SendFut},
};
use std::sync::atomic::{AtomicUsize, Ordering};

/// A client with one producer per partition of the sink topic, or a single
/// producer for a non-partitioned topic.
pub(super) struct Producers {
    pub(super) client: PulsarClient,
    partitions: Vec<Partition>,
    /// Round-robin position for keyless messages.
    next: AtomicUsize,
}

pub(super) struct Partition {
    pub(super) topic: String,
    producer: Producer,
}

impl Producers {
    pub(super) async fn connect(config: &PulsarSinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        let client = connect(&config.service_url, config.authentication.as_ref()).await?;
        let mut partitions = Vec::new();
        for topic in partition_topics(&client, &config.topic).await? {
            let mut builder = client.producer(&topic);
            if let Some(name) = &config.producer_name {
                builder = builder.name(name);
            }
            let producer = builder.create().await?;
            partitions.push(Partition { topic, producer });
        }
        Ok(Self {
            client,
            partitions,
            next: AtomicUsize::new(0),
        })
    }

    /// Picks the partition for `output` the way Pulsar's Java client does by
    /// default: a keyed message goes to the Java `String.hashCode` of its wire
    /// key modulo the partition count, and keyless messages rotate.
    pub(super) fn route(&self, output: &PulsarPrepared) -> &Partition {
        let count = self.partitions.len();
        let index = match output.key.as_deref() {
            Some(key) if !key.is_empty() => java_string_hash(&BASE64.encode(key)) as usize % count,
            _ => self.next.fetch_add(1, Ordering::Relaxed) % count,
        };
        &self.partitions[index]
    }

    pub(super) async fn close(self) -> anyhow::Result<()> {
        let mut result = Ok(());
        for partition in self.partitions {
            if let Err(error) = partition.producer.close().await {
                result = Err(error.into());
            }
        }
        self.client.close().await;
        result
    }
}

impl Partition {
    /// Publishes `output`, as part of `transaction` when one is given, and
    /// waits for the broker receipt.
    pub(super) async fn send(
        &self,
        output: &PulsarPrepared,
        transaction: Option<TxnId>,
    ) -> anyhow::Result<()> {
        self.enqueue(output, transaction).await?;
        Ok(())
    }

    /// Queues `output` on the producer. The returned future resolves with the
    /// broker receipt.
    pub(super) fn enqueue(&self, output: &PulsarPrepared, transaction: Option<TxnId>) -> SendFut {
        let mut message = outgoing(output);
        message.txn_id = transaction;
        self.producer.send(message)
    }
}

fn outgoing(output: &PulsarPrepared) -> OutgoingMessage {
    let payload = output.value.clone().unwrap_or_default();
    let metadata = MessageMetadata {
        // Keys are arbitrary bytes, so they are always sent in Pulsar's
        // base64 representation.
        partition_key: output.key.as_deref().map(|key| BASE64.encode(key)),
        partition_key_b64_encoded: output.key.as_ref().map(|_| true),
        ordering_key: output.ordering_key.clone().map(Into::into),
        event_time: output.event_time,
        null_value: output.value.is_none().then_some(true),
        properties: output
            .properties
            .iter()
            .map(|(key, value)| KeyValue {
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
        ..MessageMetadata::default()
    };
    OutgoingMessage {
        uncompressed_size: u32::try_from(payload.len()).unwrap_or(u32::MAX),
        payload: payload.into(),
        metadata,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_keys_use_pulsars_base64_representation() {
        let output = PulsarPrepared {
            key: Some(vec![0, 1, 2, 253, 254, 255]),
            ..PulsarPrepared::default()
        };
        let message = outgoing(&output);
        assert_eq!(message.metadata.partition_key.as_deref(), Some("AAEC/f7/"));
        assert_eq!(message.metadata.partition_key_b64_encoded, Some(true));
    }

    #[test]
    fn a_null_value_is_marked_and_sent_as_an_empty_payload() {
        let tombstone = outgoing(&PulsarPrepared::default());
        assert!(tombstone.payload.is_empty());
        assert_eq!(tombstone.metadata.null_value, Some(true));

        let empty = outgoing(&PulsarPrepared {
            value: Some(Vec::new()),
            ..PulsarPrepared::default()
        });
        assert!(empty.payload.is_empty());
        assert_eq!(empty.metadata.null_value, None);
    }
}
