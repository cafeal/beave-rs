//! Broker connections shared by the sources and sinks of a handle.
use super::{
    config::{SinkConfig, SourceConfig, redacted, validate_uri},
    sink::RabbitMqSink,
    source::RabbitMqSource,
};
use crate::error::{Context, Error};
use lapin::{Channel, Connection, ConnectionProperties};
use std::{fmt, sync::Arc};
use tokio::sync::Mutex;

/// AMQP reply code of a normal close.
const REPLY_SUCCESS: u16 = 200;

/// A RabbitMQ broker connection that the sources and sinks built from it share.
///
/// The handle opens one connection for its sources and one for its sinks, as
/// RabbitMQ recommends, because the broker blocks a publishing connection under
/// memory or disk alarms and consumers on it would stall too. Each source and
/// sink uses a channel of its own on them. A connection opens on the first use
/// that needs it, which also validates the settings, and is opened again after
/// it is lost. It closes once this handle and every source and sink built from
/// it have been dropped.
///
/// Each builder method returns a handle with connections of its own, so
/// configure the handle before building sources and sinks from it. `Debug`
/// output hides the password of the URI.
///
/// ```no_run
/// use beavers::{Json, adapters::rabbitmq::RabbitMq};
///
/// let rabbit = RabbitMq::new("amqp://guest:guest@localhost:5672/%2f");
/// let orders = rabbit.source::<Json, String>("orders").prefetch(32);
/// let invoices = rabbit.sink::<Json, String>("billing", "invoice.created");
/// ```
#[derive(Clone)]
pub struct RabbitMq {
    shared: Arc<Shared>,
}

struct Shared {
    uri: String,
    name: String,
    consuming: Mutex<Option<Arc<Connection>>>,
    publishing: Mutex<Option<Arc<Connection>>>,
}

/// Which of the handle's connections a channel is opened on.
#[derive(Clone, Copy)]
pub(super) enum Role {
    Consume,
    Publish,
}

impl RabbitMq {
    /// A handle for the broker at `uri`, such as
    /// `amqp://guest:guest@localhost:5672/%2f`.
    pub fn new(uri: impl Into<String>) -> Self {
        Self::from_parts(uri.into(), "beavers".into())
    }

    /// Names the connections `{name} consumer` and `{name} publisher` in the
    /// broker's management tools. Defaults to `beavers`.
    pub fn connection_name(self, name: impl Into<String>) -> Self {
        Self::from_parts(self.shared.uri.clone(), name.into())
    }

    /// A source that consumes `queue` and decodes with the codec's default
    /// value. The queue must already exist; the adapter declares no topology.
    ///
    /// Defaults: a `prefetch` of 100 and `ordered` disabled.
    pub fn source<C: Default, T>(&self, queue: impl Into<String>) -> RabbitMqSource<C, T> {
        self.source_with_codec(C::default(), queue)
    }

    /// A source like [`source`](Self::source) that decodes with `codec`.
    pub fn source_with_codec<C, T>(
        &self,
        codec: C,
        queue: impl Into<String>,
    ) -> RabbitMqSource<C, T> {
        RabbitMqSource::new(self.clone(), SourceConfig::new(queue.into()), codec)
    }

    /// A sink that publishes to `exchange` with `routing_key` for outputs that
    /// do not set their own, and encodes with the codec's default value. The
    /// empty exchange name is the default exchange, which routes a message to
    /// the queue named by its routing key.
    ///
    /// Defaults: `persistent` and `mandatory` enabled and a `max_pending` of
    /// 1000.
    pub fn sink<C: Default, T>(
        &self,
        exchange: impl Into<String>,
        routing_key: impl Into<String>,
    ) -> RabbitMqSink<C, T> {
        self.sink_with_codec(C::default(), exchange, routing_key)
    }

    /// A sink like [`sink`](Self::sink) that encodes with `codec`.
    pub fn sink_with_codec<C, T>(
        &self,
        codec: C,
        exchange: impl Into<String>,
        routing_key: impl Into<String>,
    ) -> RabbitMqSink<C, T> {
        RabbitMqSink::new(
            self.clone(),
            SinkConfig::new(exchange.into(), routing_key.into()),
            codec,
        )
    }

    /// Checks the URI without connecting.
    ///
    /// Fails when the URI does not parse or does not use `amqp://` or
    /// `amqps://`.
    pub fn validate(&self) -> Result<(), Error> {
        validate_uri(&self.shared.uri)
    }

    /// Opens a channel on the connection for `role`, connecting on first use
    /// and again after the connection was lost.
    pub(super) async fn channel(&self, role: Role) -> Result<Channel, Error> {
        let (slot, role) = match role {
            Role::Consume => (&self.shared.consuming, "consumer"),
            Role::Publish => (&self.shared.publishing, "publisher"),
        };
        let mut slot = slot.lock().await;
        if let Some(connection) = slot
            .as_ref()
            .filter(|connection| connection.status().connected())
        {
            return connection
                .create_channel()
                .await
                .context("opening a RabbitMQ channel");
        }
        self.validate()?;
        let name = format!("{} {role}", self.shared.name);
        let properties = ConnectionProperties::default().with_connection_name(name.into());
        let connection = Connection::connect(&self.shared.uri, properties)
            .await
            .context("connecting to RabbitMQ")?;
        let channel = connection
            .create_channel()
            .await
            .context("opening a RabbitMQ channel")?;
        *slot = Some(Arc::new(connection));
        Ok(channel)
    }

    fn from_parts(uri: String, name: String) -> Self {
        Self {
            shared: Arc::new(Shared {
                uri,
                name,
                consuming: Mutex::new(None),
                publishing: Mutex::new(None),
            }),
        }
    }
}

impl fmt::Debug for RabbitMq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RabbitMq")
            .field("uri", &redacted(&self.shared.uri))
            .field("connection_name", &self.shared.name)
            .finish_non_exhaustive()
    }
}

/// Closes a channel. The broker requeues the messages a closed channel had not
/// acknowledged.
pub(super) async fn close(channel: &Channel) -> Result<(), Error> {
    if !channel.status().connected() {
        return Ok(());
    }
    channel
        .close(REPLY_SUCCESS, "closing".into())
        .await
        .context("closing the RabbitMQ channel")
}
