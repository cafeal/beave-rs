use crate::retry::RetryPolicy;
use bytes::Bytes;
use magnetar::proto::{AuthError, AuthProvider, pb::command_subscribe::SubType};
use std::{fmt, sync::Arc, time::Duration};

/// Static credentials sent with Pulsar's `CONNECT` command.
#[derive(Clone)]
pub struct PulsarAuthentication {
    pub name: String,
    pub data: Vec<u8>,
}

impl fmt::Debug for PulsarAuthentication {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PulsarAuthentication")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl PulsarAuthentication {
    pub fn token(token: impl Into<Vec<u8>>) -> Self {
        Self {
            name: "token".into(),
            data: token.into(),
        }
    }

    pub(super) fn provider(&self) -> Arc<dyn AuthProvider> {
        Arc::new(self.clone())
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.name.trim().is_empty(),
            "Pulsar auth method is required"
        );
        anyhow::ensure!(!self.data.is_empty(), "Pulsar auth data is required");
        Ok(())
    }
}

impl AuthProvider for PulsarAuthentication {
    fn method(&self) -> &str {
        &self.name
    }

    fn initial(&self) -> Result<Bytes, AuthError> {
        Ok(Bytes::copy_from_slice(&self.data))
    }
}

/// How the broker distributes a subscription's messages among its consumers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PulsarSubscriptionType {
    Exclusive,
    #[default]
    Shared,
    Failover,
    KeyShared,
}

impl From<PulsarSubscriptionType> for SubType {
    fn from(value: PulsarSubscriptionType) -> Self {
        match value {
            PulsarSubscriptionType::Exclusive => Self::Exclusive,
            PulsarSubscriptionType::Shared => Self::Shared,
            PulsarSubscriptionType::Failover => Self::Failover,
            PulsarSubscriptionType::KeyShared => Self::KeyShared,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PulsarSourceConfig {
    pub service_url: String,
    pub topic: String,
    pub subscription: String,
    pub subscription_type: PulsarSubscriptionType,
    pub authentication: Option<PulsarAuthentication>,
    /// Messages each partition's consumer prefetches from the broker.
    pub buffer_size: usize,
    /// Treat an empty payload as a null value, matching topic compaction,
    /// which deletes a key on an empty payload. Enabled by default.
    pub empty_payload_is_tombstone: bool,
    /// Attempts to acknowledge a delivery. An acknowledgement sent while the
    /// client reconnects after a topic unload or broker restart fails or
    /// times out, and a later attempt on the new session succeeds.
    pub ack_retry: RetryPolicy,
}

impl PulsarSourceConfig {
    pub fn new(
        service_url: impl Into<String>,
        topic: impl Into<String>,
        subscription: impl Into<String>,
    ) -> Self {
        Self {
            service_url: service_url.into(),
            topic: topic.into(),
            subscription: subscription.into(),
            subscription_type: PulsarSubscriptionType::Shared,
            authentication: None,
            buffer_size: 100,
            empty_payload_is_tombstone: true,
            ack_retry: RetryPolicy {
                max_attempts: 5,
                initial_delay: Duration::from_millis(100),
                max_delay: Duration::from_secs(2),
                ..RetryPolicy::default()
            },
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        validate_endpoint(&self.service_url, &self.topic)?;
        anyhow::ensure!(
            !self.subscription.trim().is_empty(),
            "Pulsar subscription is required"
        );
        anyhow::ensure!(
            self.buffer_size > 0,
            "Pulsar buffer size must be greater than zero"
        );
        self.ack_retry.validate()?;
        if let Some(authentication) = &self.authentication {
            authentication.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PulsarSinkConfig {
    pub service_url: String,
    pub topic: String,
    pub producer_name: Option<String>,
    pub authentication: Option<PulsarAuthentication>,
    /// Maximum number of messages queued by `submit` whose broker receipt has
    /// not arrived. `submit` waits while this many are outstanding.
    pub max_pending: usize,
    /// Sends of a message, including the first, when the broker rejects it.
    /// A broker that is shutting down or unloading the topic rejects the
    /// sends in flight, and a later send reaches the topic's next owner.
    pub send_retry: RetryPolicy,
}

impl PulsarSinkConfig {
    pub fn new(service_url: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            service_url: service_url.into(),
            topic: topic.into(),
            producer_name: None,
            authentication: None,
            max_pending: 1000,
            send_retry: RetryPolicy {
                max_attempts: 10,
                initial_delay: Duration::from_millis(100),
                max_delay: Duration::from_secs(5),
                ..RetryPolicy::default()
            },
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        validate_endpoint(&self.service_url, &self.topic)?;
        if let Some(authentication) = &self.authentication {
            authentication.validate()?;
        }
        if let Some(name) = &self.producer_name {
            anyhow::ensure!(
                !name.trim().is_empty(),
                "Pulsar producer name must not be empty"
            );
        }
        anyhow::ensure!(
            (1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&self.max_pending),
            "Pulsar sink max_pending must be between 1 and {}",
            tokio::sync::Semaphore::MAX_PERMITS
        );
        self.send_retry.validate()?;
        Ok(())
    }
}

fn validate_endpoint(service_url: &str, topic: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !service_url.trim().is_empty(),
        "Pulsar service URL is required"
    );
    anyhow::ensure!(
        !service_url.chars().any(char::is_whitespace),
        "Pulsar service URL must not contain whitespace"
    );
    anyhow::ensure!(
        service_url.starts_with("pulsar://") || service_url.starts_with("pulsar+ssl://"),
        "Pulsar service URL must use pulsar:// or pulsar+ssl://"
    );
    anyhow::ensure!(!topic.trim().is_empty(), "Pulsar topic is required");
    Ok(())
}
