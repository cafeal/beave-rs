use super::convert::short_string;
use crate::adapters::pending::PendingLimit;
use lapin::uri::AMQPUri;
use std::fmt;

/// Checks a broker connection URI without connecting.
fn validate_uri(uri: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        uri.starts_with("amqp://") || uri.starts_with("amqps://"),
        "RabbitMQ URI must use amqp:// or amqps://"
    );
    uri.parse::<AMQPUri>()
        .map_err(|error| anyhow::anyhow!("invalid RabbitMQ URI: {error}"))?;
    Ok(())
}

/// The URI with the password of its user information replaced.
fn redacted(uri: &str) -> String {
    let Some((scheme, rest)) = uri.split_once("://") else {
        return uri.to_owned();
    };
    let authority_end = rest.find('/').unwrap_or(rest.len());
    match rest[..authority_end].rfind('@') {
        Some(at) => match rest[..at].split_once(':') {
            Some((user, _)) => format!("{scheme}://{user}:***{}", &rest[at..]),
            None => uri.to_owned(),
        },
        None => uri.to_owned(),
    }
}

/// Configuration for a [`RabbitMqSource`](super::RabbitMqSource).
///
/// `Debug` output hides the password of the URI.
#[derive(Clone)]
pub struct RabbitMqSourceConfig {
    /// Broker connection URI, such as `amqp://guest:guest@localhost:5672/%2f`.
    pub uri: String,
    /// The queue to consume. It must already exist; the adapter declares no topology.
    pub queue: String,
    /// Unacknowledged messages the broker sends ahead of acknowledgements
    /// (`basic.qos` prefetch count). It bounds the source's buffered and
    /// in-flight deliveries together.
    pub prefetch: u16,
    /// Gives every delivery the queue as its ordering key, so the subscription
    /// processes them one at a time in delivery order under
    /// [`ProcessingOrder::PerKey`](crate::ProcessingOrder::PerKey). Use it with a
    /// queue that has a single active consumer.
    pub ordered: bool,
}

impl RabbitMqSourceConfig {
    /// Creates a configuration with a `prefetch` of 100 and `ordered` disabled.
    pub fn new(uri: impl Into<String>, queue: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            queue: queue.into(),
            prefetch: 100,
            ordered: false,
        }
    }

    /// Checks the configuration without connecting.
    ///
    /// Fails when the URI does not parse or does not use `amqp://` or
    /// `amqps://`, when the queue name is empty or longer than 255 bytes, or
    /// when `prefetch` is zero.
    pub fn validate(&self) -> anyhow::Result<()> {
        validate_uri(&self.uri)?;
        anyhow::ensure!(!self.queue.trim().is_empty(), "RabbitMQ queue is required");
        short_string("queue name", &self.queue)?;
        anyhow::ensure!(
            self.prefetch > 0,
            "RabbitMQ prefetch must be greater than zero"
        );
        Ok(())
    }
}

impl fmt::Debug for RabbitMqSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RabbitMqSourceConfig")
            .field("uri", &redacted(&self.uri))
            .field("queue", &self.queue)
            .field("prefetch", &self.prefetch)
            .field("ordered", &self.ordered)
            .finish()
    }
}

/// Configuration for a [`RabbitMqSink`](super::RabbitMqSink).
///
/// `Debug` output hides the password of the URI.
#[derive(Clone)]
pub struct RabbitMqSinkConfig {
    /// Broker connection URI, such as `amqp://guest:guest@localhost:5672/%2f`.
    pub uri: String,
    /// The exchange every output is published to. The empty name is the default
    /// exchange, which routes a message to the queue named by its routing key.
    pub exchange: String,
    /// Routing key of outputs that do not set their own.
    pub routing_key: String,
    /// Publish with the persistent delivery mode, so durable queues keep
    /// messages across a broker restart. Enabled by default.
    pub persistent: bool,
    /// Ask the broker to return a message that no queue receives, which fails
    /// its completion. Enabled by default; when disabled, such a message is
    /// confirmed and discarded by the broker.
    pub mandatory: bool,
    /// Maximum number of messages published by `submit` whose publisher
    /// confirmation has not arrived. `submit` waits while this many are
    /// outstanding.
    pub max_pending: usize,
}

impl RabbitMqSinkConfig {
    /// Creates a configuration with `persistent` and `mandatory` enabled and a
    /// `max_pending` of 1000.
    pub fn new(
        uri: impl Into<String>,
        exchange: impl Into<String>,
        routing_key: impl Into<String>,
    ) -> Self {
        Self {
            uri: uri.into(),
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            persistent: true,
            mandatory: true,
            max_pending: 1000,
        }
    }

    /// Checks the configuration without connecting.
    ///
    /// Fails when the URI does not parse or does not use `amqp://` or
    /// `amqps://`, when the exchange name or routing key is longer than 255
    /// bytes, or when `max_pending` is zero or too large.
    pub fn validate(&self) -> anyhow::Result<()> {
        validate_uri(&self.uri)?;
        short_string("exchange name", &self.exchange)?;
        short_string("routing key", &self.routing_key)?;
        PendingLimit::validate(self.max_pending, "RabbitMQ")
    }
}

impl fmt::Debug for RabbitMqSinkConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RabbitMqSinkConfig")
            .field("uri", &redacted(&self.uri))
            .field("exchange", &self.exchange)
            .field("routing_key", &self.routing_key)
            .field("persistent", &self.persistent)
            .field("mandatory", &self.mandatory)
            .field("max_pending", &self.max_pending)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_output_hides_the_password() {
        let config = RabbitMqSourceConfig::new("amqp://app:secret@broker:5672/%2f", "orders");
        let debug = format!("{config:?}");
        assert!(debug.contains("amqp://app:***@broker:5672/%2f"), "{debug}");
        assert!(!debug.contains("secret"));
        assert_eq!(redacted("amqp://localhost"), "amqp://localhost");
        assert_eq!(redacted("amqp://guest@localhost"), "amqp://guest@localhost");
    }

    #[test]
    fn validation_checks_values_locally() {
        let valid = RabbitMqSourceConfig::new("amqp://localhost:5672/%2f", "orders");
        valid.validate().unwrap();
        let mut config = valid.clone();
        config.uri = "http://localhost".into();
        assert!(config.validate().is_err());
        let mut config = valid.clone();
        config.queue = " ".into();
        assert!(config.validate().is_err());
        let mut config = valid;
        config.prefetch = 0;
        assert!(config.validate().is_err());

        let sink = RabbitMqSinkConfig::new("amqps://localhost/vhost", "", "orders");
        sink.validate().unwrap();
        let mut config = sink.clone();
        config.routing_key = "k".repeat(256);
        assert!(config.validate().is_err());
        let mut config = sink;
        config.max_pending = 0;
        assert!(config.validate().is_err());
    }
}
