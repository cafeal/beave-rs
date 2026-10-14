use crate::adapters::pending::PendingLimit;
use crate::error::{Error, ensure};
use rdkafka::ClientConfig;
use std::{collections::HashMap, time::Duration};

/// Settings of a [`KafkaSource`](super::KafkaSource).
#[derive(Clone, Debug)]
pub(super) struct SourceConfig {
    pub(super) group_id: String,
    pub(super) topics: Vec<String>,
    /// Consumer settings applied after the handle's properties.
    pub(super) properties: HashMap<String, String>,
    /// How long a revoke waits for started deliveries; `None` revokes at once.
    pub(super) handoff: Option<Duration>,
}

impl SourceConfig {
    pub(super) fn new(group_id: String, topics: Vec<String>) -> Self {
        Self {
            group_id,
            topics,
            properties: HashMap::new(),
            handoff: None,
        }
    }

    /// `handle` is the handle's client configuration, consulted for
    /// `max.poll.interval.ms` when the source does not set it.
    pub(super) fn validate(&self, handle: &ClientConfig) -> Result<(), Error> {
        ensure!(
            !self.group_id.trim().is_empty(),
            Error::config,
            "Kafka group ID is required"
        );
        ensure!(
            !self.topics.is_empty(),
            Error::config,
            "at least one Kafka topic is required"
        );
        for topic in &self.topics {
            ensure!(
                !topic.trim().is_empty(),
                Error::config,
                "Kafka topic names must not be empty"
            );
        }
        validate_properties(&self.properties)?;
        if let Some(handoff) = self.handoff {
            let poll_interval = self
                .properties
                .get(MAX_POLL_INTERVAL)
                .map(String::as_str)
                .or_else(|| handle.get(MAX_POLL_INTERVAL))
                .map(str::parse::<u64>)
                .transpose()
                .map_err(|_| Error::config("Kafka max.poll.interval.ms must be an integer"))?
                .map_or(DEFAULT_MAX_POLL_INTERVAL, Duration::from_millis);
            ensure!(
                !handoff.is_zero() && handoff < poll_interval,
                Error::config,
                "Kafka graceful handoff timeout must be positive and shorter than max.poll.interval.ms"
            );
        }
        Ok(())
    }
}

const MAX_POLL_INTERVAL: &str = "max.poll.interval.ms";
/// librdkafka's default `max.poll.interval.ms`.
const DEFAULT_MAX_POLL_INTERVAL: Duration = Duration::from_secs(300);

/// Settings of a [`KafkaSink`](super::KafkaSink).
#[derive(Clone, Debug)]
pub(super) struct SinkConfig {
    pub(super) topic: String,
    pub(super) max_pending: usize,
    pub(super) close_timeout: Duration,
}

impl SinkConfig {
    pub(super) fn new(topic: String) -> Self {
        Self {
            topic,
            max_pending: 1000,
            close_timeout: Duration::from_secs(30),
        }
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
        validate_topic(&self.topic)?;
        PendingLimit::validate(self.max_pending, "Kafka")
    }
}

/// Settings of a [`KafkaTransactionalSink`](super::KafkaTransactionalSink).
#[derive(Clone, Debug)]
pub(super) struct TransactionalConfig {
    pub(super) topic: String,
    pub(super) transactional_id: String,
    pub(super) close_timeout: Duration,
    pub(super) transaction_timeout: Duration,
}

impl TransactionalConfig {
    pub(super) fn validate(&self) -> Result<(), Error> {
        validate_topic(&self.topic)?;
        ensure!(
            !self.transactional_id.trim().is_empty(),
            Error::config,
            "Kafka transactional ID is required"
        );
        Ok(())
    }
}

pub(super) const TRANSACTIONAL_ID: &str = "transactional.id";

fn validate_topic(topic: &str) -> Result<(), Error> {
    ensure!(
        !topic.trim().is_empty(),
        Error::config,
        "Kafka topic is required"
    );
    Ok(())
}

pub(super) fn validate_properties(properties: &HashMap<String, String>) -> Result<(), Error> {
    for (key, value) in properties {
        ensure!(
            !key.trim().is_empty(),
            Error::config,
            "Kafka property names must not be empty"
        );
        ensure!(
            !value.contains('\0'),
            Error::config,
            "Kafka property values must not contain NUL bytes"
        );
    }
    Ok(())
}
