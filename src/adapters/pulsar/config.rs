use crate::error::{Error, ensure};
use crate::{adapters::pending::PendingLimit, retry::RetryPolicy};
use bytes::Bytes;
use magnetar::proto::{AuthError, AuthProvider, pb::command_subscribe::SubType};
use std::{fmt, sync::Arc, time::Duration};

/// Static credentials sent with Pulsar's `CONNECT` command.
#[derive(Clone)]
pub struct PulsarAuthentication {
    /// Pulsar authentication method name, such as `token`.
    pub name: String,
    /// Credential bytes for the method. They are omitted from `Debug` output.
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
    /// Token authentication with a JWT, using the `token` method.
    pub fn token(token: impl Into<Vec<u8>>) -> Self {
        Self {
            name: "token".into(),
            data: token.into(),
        }
    }

    pub(super) fn provider(&self) -> Arc<dyn AuthProvider> {
        Arc::new(self.clone())
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
        ensure!(
            !self.name.trim().is_empty(),
            Error::config,
            "Pulsar auth method is required"
        );
        ensure!(
            !self.data.is_empty(),
            Error::config,
            "Pulsar auth data is required"
        );
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
    /// Only one consumer may attach to the subscription.
    Exclusive,
    /// Messages are distributed among all consumers. The default.
    #[default]
    Shared,
    /// One active consumer per partition; others take over when it disconnects.
    Failover,
    /// Messages with the same key or ordering key go to the same consumer.
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

/// Settings of a [`PulsarSource`](super::PulsarSource).
#[derive(Clone, Debug)]
pub(super) struct SourceConfig {
    pub(super) topic: String,
    pub(super) subscription: String,
    pub(super) subscription_type: PulsarSubscriptionType,
    pub(super) buffer_size: usize,
    pub(super) empty_payload_is_tombstone: bool,
    pub(super) ack_retry: RetryPolicy,
}

impl SourceConfig {
    pub(super) fn new(topic: String, subscription: String) -> Self {
        Self {
            topic,
            subscription,
            subscription_type: PulsarSubscriptionType::Shared,
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

    pub(super) fn validate(&self) -> Result<(), Error> {
        validate_topic(&self.topic)?;
        ensure!(
            !self.subscription.trim().is_empty(),
            Error::config,
            "Pulsar subscription is required"
        );
        ensure!(
            self.buffer_size > 0,
            Error::config,
            "Pulsar buffer size must be greater than zero"
        );
        self.ack_retry.validate()
    }
}

/// Settings of a [`PulsarSink`](super::PulsarSink).
#[derive(Clone, Debug)]
pub(super) struct SinkConfig {
    pub(super) topic: String,
    pub(super) producer_name: Option<String>,
    pub(super) max_pending: usize,
    pub(super) send_retry: RetryPolicy,
}

impl SinkConfig {
    pub(super) fn new(topic: String) -> Self {
        Self {
            topic,
            producer_name: None,
            max_pending: 1000,
            send_retry: RetryPolicy {
                max_attempts: 10,
                initial_delay: Duration::from_millis(100),
                max_delay: Duration::from_secs(5),
                ..RetryPolicy::default()
            },
        }
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
        validate_topic(&self.topic)?;
        if let Some(name) = &self.producer_name {
            ensure!(
                !name.trim().is_empty(),
                Error::config,
                "Pulsar producer name must not be empty"
            );
        }
        PendingLimit::validate(self.max_pending, "Pulsar")?;
        self.send_retry.validate()
    }
}

/// Checks a broker service URL.
pub(super) fn validate_service_url(service_url: &str) -> Result<(), Error> {
    ensure!(
        !service_url.trim().is_empty(),
        Error::config,
        "Pulsar service URL is required"
    );
    ensure!(
        !service_url.chars().any(char::is_whitespace),
        Error::config,
        "Pulsar service URL must not contain whitespace"
    );
    ensure!(
        service_url.starts_with("pulsar://") || service_url.starts_with("pulsar+ssl://"),
        Error::config,
        "Pulsar service URL must use pulsar:// or pulsar+ssl://"
    );
    Ok(())
}

fn validate_topic(topic: &str) -> Result<(), Error> {
    ensure!(
        !topic.trim().is_empty(),
        Error::config,
        "Pulsar topic is required"
    );
    Ok(())
}
