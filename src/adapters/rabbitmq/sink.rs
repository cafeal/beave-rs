use super::{
    config::RabbitMqSinkConfig,
    connection::{close, connect},
    convert::{properties_to_amqp, short_string},
    record::RabbitMqPublish,
};
use crate::{
    adapters::pending::PendingLimit,
    codec::Encoder,
    sink::{Completion, Sink},
};
use anyhow::Context as _;
use lapin::{
    BasicProperties, Channel, Confirmation, Connection,
    options::{BasicPublishOptions, ConfirmSelectOptions},
    types::ShortString,
};
use std::{
    fmt,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::Mutex;

/// An encoded RabbitMQ message with its routing key and AMQP properties.
/// Preparing a value performs all codec work and protocol validation once.
#[derive(Clone)]
pub struct RabbitMqPrepared {
    routing_key: ShortString,
    properties: BasicProperties,
    payload: Vec<u8>,
}

impl RabbitMqPrepared {
    pub fn routing_key(&self) -> &str {
        self.routing_key.as_str()
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl fmt::Debug for RabbitMqPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RabbitMqPrepared")
            .field("routing_key", &self.routing_key.as_str())
            .field("properties", &self.properties)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

/// A connection and its channel in confirm mode.
struct Publisher {
    connection: Connection,
    channel: Channel,
}

struct State {
    publisher: Mutex<Option<Publisher>>,
    closed: AtomicBool,
    pending: PendingLimit,
}

/// Publishes to one exchange and completes each message on its publisher
/// confirmation.
pub struct RabbitMqSink<C, T> {
    config: RabbitMqSinkConfig,
    codec: Arc<C>,
    state: Arc<State>,
    marker: PhantomData<fn(T)>,
}

impl<C, T> Clone for RabbitMqSink<C, T> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            codec: self.codec.clone(),
            state: self.state.clone(),
            marker: PhantomData,
        }
    }
}

impl<C: Default, T> RabbitMqSink<C, T> {
    pub fn new(config: RabbitMqSinkConfig) -> Self {
        Self::with_codec(config, C::default())
    }
}

impl<C, T> RabbitMqSink<C, T> {
    pub fn with_codec(config: RabbitMqSinkConfig, codec: C) -> Self {
        Self {
            codec: Arc::new(codec),
            state: Arc::new(State {
                publisher: Mutex::new(None),
                closed: AtomicBool::new(false),
                pending: PendingLimit::new(config.max_pending, "RabbitMQ"),
            }),
            config,
            marker: PhantomData,
        }
    }

    /// The channel to publish on, connecting on first use and again after the
    /// previous connection or channel was lost.
    async fn channel(&self) -> anyhow::Result<Channel> {
        let mut publisher = self.state.publisher.lock().await;
        anyhow::ensure!(
            !self.state.closed.load(Ordering::Acquire),
            "RabbitMQ sink is closed"
        );
        if let Some(current) = publisher.as_ref() {
            if current.channel.status().connected() {
                return Ok(current.channel.clone());
            }
            let _ = close(&current.connection).await;
            *publisher = None;
        }
        self.config.validate()?;
        let connection = connect(
            &self.config.uri,
            format!("beavers sink {}", self.config.exchange),
        )
        .await?;
        let channel = async {
            let channel = connection.create_channel().await?;
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await?;
            anyhow::Ok(channel)
        }
        .await
        .context("opening a RabbitMQ publisher channel");
        let channel = match channel {
            Ok(channel) => channel,
            Err(error) => {
                let _ = close(&connection).await;
                return Err(error);
            }
        };
        *publisher = Some(Publisher {
            connection,
            channel: channel.clone(),
        });
        Ok(channel)
    }
}

impl<C, T> Sink<RabbitMqPublish<T>> for RabbitMqSink<C, T>
where
    C: Encoder<T>,
    T: Send + Sync + 'static,
{
    type Prepared = RabbitMqPrepared;

    fn prepare(&self, output: RabbitMqPublish<T>) -> anyhow::Result<Self::Prepared> {
        let routing_key = output
            .routing_key
            .as_deref()
            .unwrap_or(&self.config.routing_key);
        Ok(RabbitMqPrepared {
            routing_key: short_string("routing key", routing_key)?,
            properties: properties_to_amqp(
                &output.properties,
                &output.headers,
                self.config.persistent,
            )?,
            payload: self.codec.encode(&output.value)?,
        })
    }

    async fn publish(&self, output: &Self::Prepared) -> anyhow::Result<()> {
        self.submit(output).await?.wait().await
    }

    /// Returns once the message is written to the channel. The completion
    /// resolves on its publisher confirmation.
    async fn submit(&self, output: &Self::Prepared) -> anyhow::Result<Completion> {
        let permit = self.state.pending.acquire().await?;
        let channel = self.channel().await?;
        let options = BasicPublishOptions {
            mandatory: self.config.mandatory,
            immediate: false,
        };
        let confirm = channel
            .basic_publish(
                short_string("exchange name", &self.config.exchange)?,
                output.routing_key.clone(),
                options,
                &output.payload,
                output.properties.clone(),
            )
            .await
            .context("publishing to RabbitMQ")?;
        Ok(Completion::pending(async move {
            let _permit = permit;
            confirmed(
                confirm
                    .await
                    .context("waiting for a RabbitMQ publisher confirmation")?,
            )
        }))
    }

    /// Stops new submissions, waits for outstanding confirmations, and closes
    /// the connection.
    async fn close(&self) -> anyhow::Result<()> {
        self.state.closed.store(true, Ordering::Release);
        self.state.pending.close();
        let publisher = self.state.publisher.lock().await.take();
        if let Some(publisher) = publisher {
            let _ = publisher.channel.wait_for_confirms().await;
            close(&publisher.connection).await?;
        }
        Ok(())
    }
}

fn confirmed(confirmation: Confirmation) -> anyhow::Result<()> {
    match confirmation {
        Confirmation::Ack(None) | Confirmation::NotRequested => Ok(()),
        Confirmation::Ack(Some(returned)) => anyhow::bail!(
            "RabbitMQ returned an unroutable message: {} {}",
            returned.reply_code,
            returned.reply_text
        ),
        Confirmation::Nack(_) => anyhow::bail!("RabbitMQ did not accept a published message"),
    }
}
