use super::{
    config::SinkConfig,
    connection::{RabbitMq, Role, close},
    convert::{properties_to_amqp, short_string},
    record::RabbitMqPublish,
};
use crate::{
    adapters::pending::PendingLimit,
    codec::Encoder,
    error::{BoxError, Context, Error, ensure},
    sink::{Completion, Sink},
};
use lapin::{
    BasicProperties, Channel, Confirmation,
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
    /// Routing key the message is published with: the output's own or the
    /// configured one.
    pub fn routing_key(&self) -> &str {
        self.routing_key.as_str()
    }

    /// Encoded message body.
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

struct State {
    /// The channel in confirm mode.
    channel: Mutex<Option<Channel>>,
    closed: AtomicBool,
    pending: PendingLimit,
}

impl State {
    fn new(max_pending: usize) -> Arc<Self> {
        Arc::new(Self {
            channel: Mutex::new(None),
            closed: AtomicBool::new(false),
            pending: PendingLimit::new(max_pending, "RabbitMQ"),
        })
    }
}

/// Publishes to one exchange on a channel of its own and completes each message
/// on its publisher confirmation.
///
/// Built with [`RabbitMq::sink`]. It opens its channel on the first
/// publication, which also validates the settings, and again after the channel
/// or connection is lost. Clones share the channel, closure state, and
/// `max_pending` limit.
pub struct RabbitMqSink<C, T> {
    rabbit: RabbitMq,
    config: SinkConfig,
    codec: Arc<C>,
    state: Arc<State>,
    marker: PhantomData<fn(T)>,
}

impl<C, T> Clone for RabbitMqSink<C, T> {
    fn clone(&self) -> Self {
        Self {
            rabbit: self.rabbit.clone(),
            config: self.config.clone(),
            codec: self.codec.clone(),
            state: self.state.clone(),
            marker: PhantomData,
        }
    }
}

impl<C, T> RabbitMqSink<C, T> {
    pub(super) fn new(rabbit: RabbitMq, config: SinkConfig, codec: C) -> Self {
        Self {
            rabbit,
            state: State::new(config.max_pending),
            config,
            codec: Arc::new(codec),
            marker: PhantomData,
        }
    }

    /// Whether messages use the persistent delivery mode, so durable queues
    /// keep them across a broker restart. Enabled by default.
    pub fn persistent(mut self, persistent: bool) -> Self {
        self.config.persistent = persistent;
        self
    }

    /// Whether the broker returns a message that no queue receives, which
    /// fails its completion. Enabled by default; when disabled, such a message
    /// is confirmed and discarded by the broker.
    pub fn mandatory(mut self, mandatory: bool) -> Self {
        self.config.mandatory = mandatory;
        self
    }

    /// Messages published by `submit` whose publisher confirmation has not
    /// arrived. `submit` waits while this many are outstanding. Defaults to
    /// 1000.
    pub fn max_pending(mut self, max_pending: usize) -> Self {
        self.config.max_pending = max_pending;
        self.state = State::new(max_pending);
        self
    }

    /// Checks the handle and sink settings without connecting.
    ///
    /// Fails when the handle is invalid, the exchange name or routing key is
    /// longer than 255 bytes, or `max_pending` is zero or too large.
    pub fn validate(&self) -> Result<(), Error> {
        self.rabbit.validate()?;
        self.config.validate()
    }

    /// The channel to publish on, opening it on first use and again after the
    /// previous channel or its connection was lost.
    async fn channel(&self) -> Result<Channel, BoxError> {
        let mut slot = self.state.channel.lock().await;
        ensure!(
            !self.state.closed.load(Ordering::Acquire),
            Error::closed,
            "RabbitMQ sink is closed"
        );
        if let Some(channel) = slot.as_ref() {
            if channel.status().connected() {
                return Ok(channel.clone());
            }
            *slot = None;
        }
        self.validate()?;
        let channel = self.rabbit.channel(Role::Publish).await?;
        if let Err(error) = channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .context("enabling RabbitMQ publisher confirms")
        {
            let _ = close(&channel).await;
            return Err(error.into());
        }
        *slot = Some(channel.clone());
        Ok(channel)
    }
}

impl<C, T> Sink<RabbitMqPublish<T>> for RabbitMqSink<C, T>
where
    C: Encoder<T>,
    T: Send + Sync + 'static,
{
    type Prepared = RabbitMqPrepared;

    fn prepare(&self, output: RabbitMqPublish<T>) -> Result<Self::Prepared, BoxError> {
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

    async fn publish(&self, output: &Self::Prepared) -> Result<(), BoxError> {
        self.submit(output).await?.wait().await
    }

    /// Returns once the message is written to the channel. The completion
    /// resolves on its publisher confirmation.
    async fn submit(&self, output: &Self::Prepared) -> Result<Completion, BoxError> {
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
    /// the channel. The connection stays open for the handle's other sinks.
    async fn close(&self) -> Result<(), BoxError> {
        self.state.closed.store(true, Ordering::Release);
        self.state.pending.close();
        let channel = self.state.channel.lock().await.take();
        if let Some(channel) = channel {
            let _ = channel.wait_for_confirms().await;
            close(&channel).await?;
        }
        Ok(())
    }
}

fn confirmed(confirmation: Confirmation) -> Result<(), BoxError> {
    match confirmation {
        Confirmation::Ack(None) | Confirmation::NotRequested => Ok(()),
        Confirmation::Ack(Some(returned)) => Err(Error::msg(format!(
            "RabbitMQ returned an unroutable message: {} {}",
            returned.reply_code, returned.reply_text
        ))
        .into()),
        Confirmation::Nack(_) => {
            Err(Error::msg("RabbitMQ did not accept a published message").into())
        }
    }
}
