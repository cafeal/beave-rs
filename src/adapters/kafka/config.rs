use std::{collections::HashMap, time::Duration};

#[derive(Clone, Debug)]
pub struct KafkaSourceConfig {
    pub brokers: String,
    pub group_id: String,
    pub topics: Vec<String>,
    /// Additional librdkafka settings. Offset storage and commits are always disabled.
    pub properties: HashMap<String, String>,
}

impl KafkaSourceConfig {
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

#[derive(Clone, Debug)]
pub struct KafkaSinkConfig {
    pub brokers: String,
    pub topic: String,
    /// Additional librdkafka producer settings.
    pub properties: HashMap<String, String>,
    /// Maximum time `close` waits for queued delivery reports.
    pub close_timeout: Duration,
}

impl KafkaSinkConfig {
    pub fn new(brokers: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            topic: topic.into(),
            properties: HashMap::new(),
            close_timeout: Duration::from_secs(30),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.brokers.trim().is_empty(),
            "Kafka brokers are required"
        );
        anyhow::ensure!(!self.topic.trim().is_empty(), "Kafka topic is required");
        validate_properties(&self.properties)
    }
}

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
