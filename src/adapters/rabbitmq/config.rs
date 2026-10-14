use super::convert::short_string;
use crate::adapters::pending::PendingLimit;
use crate::error::{Error, ensure};
use lapin::uri::AMQPUri;

/// Checks a broker connection URI without connecting.
pub(super) fn validate_uri(uri: &str) -> Result<(), Error> {
    ensure!(
        uri.starts_with("amqp://") || uri.starts_with("amqps://"),
        Error::config,
        "RabbitMQ URI must use amqp:// or amqps://"
    );
    uri.parse::<AMQPUri>()
        .map_err(|error| Error::config(format!("invalid RabbitMQ URI: {error}")))?;
    Ok(())
}

/// The URI with the password of its user information replaced.
pub(super) fn redacted(uri: &str) -> String {
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

/// Settings of a [`RabbitMqSource`](super::RabbitMqSource).
#[derive(Clone, Debug)]
pub(super) struct SourceConfig {
    pub(super) queue: String,
    pub(super) prefetch: u16,
    pub(super) ordered: bool,
}

impl SourceConfig {
    pub(super) fn new(queue: String) -> Self {
        Self {
            queue,
            prefetch: 100,
            ordered: false,
        }
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
        ensure!(
            !self.queue.trim().is_empty(),
            Error::config,
            "RabbitMQ queue is required"
        );
        short_string("queue name", &self.queue)?;
        ensure!(
            self.prefetch > 0,
            Error::config,
            "RabbitMQ prefetch must be greater than zero"
        );
        Ok(())
    }
}

/// Settings of a [`RabbitMqSink`](super::RabbitMqSink).
#[derive(Clone, Debug)]
pub(super) struct SinkConfig {
    pub(super) exchange: String,
    pub(super) routing_key: String,
    pub(super) persistent: bool,
    pub(super) mandatory: bool,
    pub(super) max_pending: usize,
}

impl SinkConfig {
    pub(super) fn new(exchange: String, routing_key: String) -> Self {
        Self {
            exchange,
            routing_key,
            persistent: true,
            mandatory: true,
            max_pending: 1000,
        }
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
        short_string("exchange name", &self.exchange)?;
        short_string("routing key", &self.routing_key)?;
        PendingLimit::validate(self.max_pending, "RabbitMQ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redaction_hides_the_password() {
        assert_eq!(
            redacted("amqp://app:secret@broker:5672/%2f"),
            "amqp://app:***@broker:5672/%2f"
        );
        assert_eq!(redacted("amqp://localhost"), "amqp://localhost");
        assert_eq!(redacted("amqp://guest@localhost"), "amqp://guest@localhost");
    }

    #[test]
    fn validation_checks_values_locally() {
        validate_uri("amqp://localhost:5672/%2f").unwrap();
        validate_uri("amqps://localhost/vhost").unwrap();
        assert!(validate_uri("http://localhost").is_err());

        let valid = SourceConfig::new("orders".into());
        valid.validate().unwrap();
        let mut config = valid.clone();
        config.queue = " ".into();
        assert!(config.validate().is_err());
        let mut config = valid;
        config.prefetch = 0;
        assert!(config.validate().is_err());

        let sink = SinkConfig::new(String::new(), "orders".into());
        sink.validate().unwrap();
        let mut config = sink.clone();
        config.routing_key = "k".repeat(256);
        assert!(config.validate().is_err());
        let mut config = sink;
        config.max_pending = 0;
        assert!(config.validate().is_err());
    }
}
