use super::{
    config::PulsarSourceConfig,
    consumer_task::{ConsumerCommand, RawDelivery, spawn_consumer},
    record::{PulsarMetadata, PulsarRecord},
};
use crate::{
    codec::Decoder,
    message::{OrderingKey, SourceMessage},
    source::{Receive, ReceiveError, Source},
};
use pulsar::{Consumer, Pulsar, SubType, TokioExecutor};
use std::{collections::HashMap, marker::PhantomData, sync::Arc};
use tokio::sync::{mpsc, oneshot};

/// Establishes its broker client and subscription on the first `receive` call.
pub struct PulsarSource<C, T> {
    config: PulsarSourceConfig,
    codec: Arc<C>,
    deliveries: Option<mpsc::Receiver<Result<RawDelivery, ReceiveError>>>,
    commands: Option<mpsc::Sender<ConsumerCommand>>,
    closed: bool,
    marker: PhantomData<T>,
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
            deliveries: None,
            commands: None,
            closed: false,
            marker: PhantomData,
        }
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        self.config.validate()?;
        let mut builder = Pulsar::builder(&self.config.service_url, TokioExecutor);
        if let Some(auth) = &self.config.authentication {
            builder = builder.with_auth(auth.provider());
        }
        let client: Pulsar<_> = builder.build().await?;
        let consumer: Consumer<Vec<u8>, _> = client
            .consumer()
            .with_topic(&self.config.topic)
            .with_subscription(&self.config.subscription)
            .with_subscription_type(self.config.subscription_type)
            .build()
            .await?;
        let (deliveries, commands) = spawn_consumer(consumer, self.config.buffer_size);
        self.deliveries = Some(deliveries);
        self.commands = Some(commands);
        Ok(())
    }
}

impl<C: Decoder<T>, T: Clone + Send + Sync + 'static> Source for PulsarSource<C, T> {
    type Message = PulsarMessage<C, T>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        if self.closed {
            return Ok(Receive::End);
        }
        if self.deliveries.is_none() {
            self.connect().await.map_err(ReceiveError::Fatal)?;
        }
        match self
            .deliveries
            .as_mut()
            .expect("connected source")
            .recv()
            .await
        {
            Some(Ok(raw)) => Ok(Receive::Message(PulsarMessage {
                ordering_key: ordering_key(self.config.subscription_type, &raw),
                payload: raw
                    .payload
                    .filter(|bytes| !(self.config.empty_payload_is_tombstone && bytes.is_empty())),
                key: raw.key,
                properties: raw.properties,
                event_time: raw.event_time,
                metadata: raw.metadata,
                codec: self.codec.clone(),
                commands: self.commands.as_ref().expect("connected source").clone(),
                marker: PhantomData,
            })),
            Some(Err(error)) => Err(error),
            None => Ok(Receive::End),
        }
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        if let Some(commands) = self.commands.take() {
            let (result_tx, result_rx) = oneshot::channel();
            if commands
                .send(ConsumerCommand::Close(result_tx))
                .await
                .is_ok()
            {
                result_rx.await??;
            }
        }
        self.deliveries.take();
        Ok(())
    }
}

/// The scope within which the broker delivers to one consumer in order.
///
/// Exclusive and Failover subscriptions deliver each topic partition in order.
/// Key_Shared delivers each ordering key, or message key when no ordering key is
/// set, in order. Shared subscriptions and keyless Key_Shared messages have no
/// ordering scope.
fn ordering_key(subscription_type: SubType, raw: &RawDelivery) -> Option<OrderingKey> {
    let partition = OrderingKey::new(
        raw.metadata.topic.as_str(),
        i64::from(raw.metadata.message_id.partition.unwrap_or(-1)),
    );
    match subscription_type {
        SubType::Exclusive | SubType::Failover => Some(partition),
        SubType::KeyShared => raw
            .ordering_key
            .as_deref()
            .or(raw.key.as_deref())
            .map(|key| partition.with_key(key)),
        SubType::Shared => None,
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
    commands: mpsc::Sender<ConsumerCommand>,
    marker: PhantomData<T>,
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
        let (result_tx, result_rx) = oneshot::channel();
        self.commands
            .send(ConsumerCommand::Ack {
                topic: self.metadata.topic,
                id: self.metadata.message_id,
                result: result_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("Pulsar consumer closed before acknowledgement"))?;
        result_rx
            .await
            .map_err(|_| anyhow::anyhow!("Pulsar consumer closed during acknowledgement"))??;
        Ok(())
    }

    fn ordering_key(&self) -> Option<OrderingKey> {
        self.ordering_key.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::pulsar::PulsarMetadata;
    use pulsar::message::proto::MessageIdData;

    fn raw(key: Option<&[u8]>, ordering_key: Option<&[u8]>) -> RawDelivery {
        RawDelivery {
            payload: Some(vec![]),
            key: key.map(<[u8]>::to_vec),
            ordering_key: ordering_key.map(<[u8]>::to_vec),
            properties: HashMap::new(),
            event_time: None,
            metadata: PulsarMetadata {
                topic: "persistent://public/default/orders-partition-2".into(),
                message_id: MessageIdData {
                    partition: Some(2),
                    ..Default::default()
                },
                publish_time: 0,
            },
        }
    }

    #[test]
    fn ordering_scope_follows_the_subscription_type() {
        let partition = OrderingKey::new("persistent://public/default/orders-partition-2", 2);
        let delivery = raw(Some(b"customer"), None);
        assert_eq!(
            ordering_key(SubType::Exclusive, &delivery),
            Some(partition.clone())
        );
        assert_eq!(
            ordering_key(SubType::Failover, &delivery),
            Some(partition.clone())
        );
        assert_eq!(ordering_key(SubType::Shared, &delivery), None);
        assert_eq!(
            ordering_key(SubType::KeyShared, &delivery),
            Some(partition.clone().with_key(b"customer".as_slice()))
        );
    }

    #[test]
    fn key_shared_prefers_the_ordering_key_and_ignores_keyless_messages() {
        let partition = OrderingKey::new("persistent://public/default/orders-partition-2", 2);
        assert_eq!(
            ordering_key(SubType::KeyShared, &raw(Some(b"customer"), Some(b"order"))),
            Some(partition.with_key(b"order".as_slice()))
        );
        assert_eq!(ordering_key(SubType::KeyShared, &raw(None, None)), None);
    }
}
