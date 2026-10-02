use crate::adapters::pending::PendingLimit;
use std::{collections::HashMap, time::Duration};

/// Configuration for a [`KafkaSource`](super::KafkaSource).
#[derive(Clone, Debug)]
pub struct KafkaSourceConfig {
    /// Bootstrap servers, set as librdkafka `bootstrap.servers`.
    pub brokers: String,
    /// Consumer group ID, set as librdkafka `group.id`.
    pub group_id: String,
    /// Topics the consumer subscribes to.
    pub topics: Vec<String>,
    /// Additional librdkafka consumer settings. Automatic offset storage and
    /// commits are always disabled; the source commits acknowledged offsets.
    pub properties: HashMap<String, String>,
}

impl KafkaSourceConfig {
    /// Creates a configuration with the required values and no additional properties.
    pub fn new(
        brokers: impl Into<String>,
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            brokers: brokers.into(),
            group_id: group_id.into(),
            topics: topics.into_iter().map(Into::into).collect(),
            properties: HashMap::new(),
        }
    }

    /// Checks the configuration without network access.
    ///
    /// Fails when the brokers, group ID, or topic list is empty, a topic name is
    /// blank, or a property has an empty name or a value containing a NUL byte.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.brokers.trim().is_empty(),
            "Kafka brokers are required"
        );
        anyhow::ensure!(
            !self.group_id.trim().is_empty(),
            "Kafka group ID is required"
        );
        anyhow::ensure!(
            !self.topics.is_empty(),
            "at least one Kafka topic is required"
        );
        for topic in &self.topics {
            anyhow::ensure!(
                !topic.trim().is_empty(),
                "Kafka topic names must not be empty"
            );
        }
        validate_properties(&self.properties)
    }
}

/// Configuration for a [`KafkaSink`](super::KafkaSink).
#[derive(Clone, Debug)]
pub struct KafkaSinkConfig {
    /// Bootstrap servers, set as librdkafka `bootstrap.servers`.
    pub brokers: String,
    /// Topic every record is published to.
    pub topic: String,
    /// Additional librdkafka producer settings.
    pub properties: HashMap<String, String>,
    /// Maximum number of records queued by `submit` whose delivery report
    /// has not arrived. `submit` waits while this many are outstanding.
    pub max_pending: usize,
    /// Maximum time `close` waits for queued delivery reports.
    pub close_timeout: Duration,
    /// Maximum time each blocking operation of a
    /// [`transactional`](super::KafkaSink::transactional) sink waits:
    /// initialization, sending consumer offsets, commit, and abort.
    pub transaction_timeout: Duration,
}

impl KafkaSinkConfig {
    /// Creates a configuration with the required values.
    ///
    /// Defaults: no additional properties, `max_pending` of 1000, a 30 second
    /// `close_timeout`, and a 60 second `transaction_timeout`.
    pub fn new(brokers: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            topic: topic.into(),
            properties: HashMap::new(),
            max_pending: 1000,
            close_timeout: Duration::from_secs(30),
            transaction_timeout: Duration::from_secs(60),
        }
    }

    /// Checks the configuration without network access.
    ///
    /// Fails when the brokers or topic is empty, `max_pending` is zero or larger
    /// than the semaphore limit, `transactional.id` is set as a property (use
    /// [`KafkaSink::transactional`](super::KafkaSink::transactional)), or a
    /// property has an empty name or a value containing a NUL byte.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.brokers.trim().is_empty(),
            "Kafka brokers are required"
        );
        anyhow::ensure!(!self.topic.trim().is_empty(), "Kafka topic is required");
        PendingLimit::validate(self.max_pending, "Kafka")?;
        anyhow::ensure!(
            !self.properties.contains_key(TRANSACTIONAL_ID),
            "set the Kafka transactional ID with KafkaSink::transactional"
        );
        validate_properties(&self.properties)
    }
}

const TRANSACTIONAL_ID: &str = "transactional.id";

fn validate_properties(properties: &HashMap<String, String>) -> anyhow::Result<()> {
    for (key, value) in properties {
        anyhow::ensure!(
            !key.trim().is_empty(),
            "Kafka property names must not be empty"
        );
        anyhow::ensure!(
            !value.contains('\0'),
            "Kafka property values must not contain NUL bytes"
        );
    }
    Ok(())
}
