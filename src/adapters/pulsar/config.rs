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

/// Configuration for a [`PulsarSource`](super::PulsarSource).
#[derive(Clone, Debug)]
pub struct PulsarSourceConfig {
    /// Broker service URL, using the `pulsar://` or `pulsar+ssl://` scheme.
    pub service_url: String,
    /// Topic to consume. A partitioned topic is consumed through one consumer
    /// per partition.
    pub topic: String,
    /// Subscription name shared by every consumer of this source.
    pub subscription: String,
    /// How the broker distributes messages among the subscription's consumers.
    pub subscription_type: PulsarSubscriptionType,
    /// Static credentials, or `None` for an unauthenticated broker.
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
    /// Creates a configuration with the required values.
    ///
    /// Defaults: a [`Shared`](PulsarSubscriptionType::Shared) subscription, no
    /// authentication, a `buffer_size` of 100, empty payloads treated as
    /// tombstones, and an `ack_retry` of 5 attempts with delays from 100 ms up
    /// to 2 s.
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

    /// Checks the configuration without network access.
    ///
    /// Fails when the service URL is empty, contains whitespace, or does not
    /// use `pulsar://` or `pulsar+ssl://`, when the topic or subscription is
    /// empty, when `buffer_size` is zero, or when `ack_retry` or the
    /// authentication is invalid.
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

/// Configuration for a [`PulsarSink`](super::PulsarSink).
#[derive(Clone, Debug)]
pub struct PulsarSinkConfig {
    /// Broker service URL, using the `pulsar://` or `pulsar+ssl://` scheme.
    pub service_url: String,
    /// Topic every message is published to.
    pub topic: String,
    /// Name given to each partition's producer, or `None` to let the broker
    /// assign one.
    pub producer_name: Option<String>,
    /// Static credentials, or `None` for an unauthenticated broker.
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
    /// Creates a configuration with the required values.
    ///
    /// Defaults: no producer name or authentication, a `max_pending` of 1000,
    /// and a `send_retry` of 10 attempts with delays from 100 ms up to 5 s.
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

    /// Checks the configuration without network access.
    ///
    /// Fails when the service URL is empty, contains whitespace, or does not
    /// use `pulsar://` or `pulsar+ssl://`, when the topic or a set producer
    /// name is empty, when `max_pending` is zero or too large, or when
    /// `send_retry` or the authentication is invalid.
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
        PendingLimit::validate(self.max_pending, "Pulsar")?;
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
