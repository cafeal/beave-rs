use super::{
    config::PulsarSourceConfig,
    consumer_task::{ConsumerCommand, RawDelivery, spawn_consumer},
    record::{PulsarMetadata, PulsarRecord},
};
use crate::{
    codec::Decoder,
    message::SourceMessage,
    source::{Receive, ReceiveError, Source},
};
use pulsar::{Consumer, Pulsar, TokioExecutor};
use std::{marker::PhantomData, sync::Arc};
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
                bytes: raw.bytes,
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

/// One delivery; dropping it leaves the broker message unacknowledged.
pub struct PulsarMessage<C, T> {
    bytes: Vec<u8>,
    key: Option<Vec<u8>>,
    properties: std::collections::HashMap<String, String>,
    event_time: Option<u64>,
    metadata: PulsarMetadata,
    codec: Arc<C>,
    commands: mpsc::Sender<ConsumerCommand>,
    marker: PhantomData<T>,
}

impl<C: Decoder<T>, T: Clone + Send + Sync + 'static> SourceMessage for PulsarMessage<C, T> {
    type Item = PulsarRecord<T>;

    fn decode(&self) -> anyhow::Result<Self::Item> {
        Ok(PulsarRecord {
            value: self.codec.decode(&self.bytes)?,
            key: self.key.clone(),
            properties: self.properties.clone(),
            event_time: self.event_time,
            metadata: self.metadata.clone(),
        })
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
}
