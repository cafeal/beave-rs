use crate::error::{Error, ensure};
use std::time::Duration;

/// How the adapters authenticate to Pub/Sub.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum PubSubCredentials {
    /// Application Default Credentials: the `GOOGLE_APPLICATION_CREDENTIALS`
    /// key file, the gcloud user credentials, or the metadata server of the
    /// Google Cloud environment the application runs in.
    #[default]
    Default,
    /// No credentials, for the Pub/Sub emulator.
    Anonymous,
}

/// Where and how a source or sink reaches Pub/Sub.
pub(super) struct Endpoint<'a> {
    pub(super) endpoint: Option<&'a str>,
    pub(super) credentials: &'a PubSubCredentials,
}

impl Endpoint<'_> {
    fn validate(&self) -> Result<(), Error> {
        if let Some(url) = self.endpoint {
            ensure!(
                url.starts_with("https://") || url.starts_with("http://"),
                Error::config,
                "Pub/Sub endpoint must be an http or https URL"
            );
        }
        Ok(())
    }
}

/// Checks that `name` is `projects/{project}/{collection}/{id}`.
fn validate_name(name: &str, collection: &str) -> Result<(), Error> {
    let parts: Vec<_> = name.split('/').collect();
    ensure!(
        matches!(parts[..], ["projects", project, kind, id]
            if !project.is_empty() && kind == collection && !id.is_empty()),
        Error::config,
        "Pub/Sub {} must be named projects/{{project}}/{collection}/{{id}}, not {name:?}",
        collection.trim_end_matches('s')
    );
    Ok(())
}

/// Configuration of a [`PubSubSource`](crate::adapters::pubsub::PubSubSource).
#[derive(Clone, Debug)]
pub struct PubSubSourceConfig {
    /// The subscription to consume, such as
    /// `projects/my-project/subscriptions/orders`.
    pub subscription: String,
    /// Endpoint replacing `https://pubsub.googleapis.com`, such as a regional
    /// endpoint or the emulator at `http://localhost:8085`.
    pub endpoint: Option<String>,
    /// How the source authenticates.
    pub credentials: PubSubCredentials,
    /// Messages Pub/Sub hands to the source before any of them is acknowledged.
    /// Zero or less sets no limit.
    pub max_outstanding_messages: i64,
    /// Bytes of message data Pub/Sub hands to the source before any of it is
    /// acknowledged. Zero or less sets no limit.
    pub max_outstanding_bytes: i64,
    /// How far each lease extension moves a message's acknowledgement
    /// deadline, from 10 seconds to 10 minutes. Pub/Sub redelivers a message
    /// this long after the source stops extending it, for example because the
    /// process crashed.
    pub ack_deadline: Duration,
    /// How long the source extends a message's lease at most. A delivery
    /// held longer is revoked, because Pub/Sub redelivers it.
    pub max_lease: Duration,
}

impl PubSubSourceConfig {
    /// A configuration for `subscription` with the default settings.
    pub fn new(subscription: impl Into<String>) -> Self {
        Self {
            subscription: subscription.into(),
            endpoint: None,
            credentials: PubSubCredentials::Default,
            max_outstanding_messages: 1000,
            max_outstanding_bytes: 100 * 1024 * 1024,
            ack_deadline: Duration::from_secs(60),
            max_lease: Duration::from_secs(60 * 60),
        }
    }

    pub(super) fn endpoint(&self) -> Endpoint<'_> {
        Endpoint {
            endpoint: self.endpoint.as_deref(),
            credentials: &self.credentials,
        }
    }

    /// Checks the settings without contacting Pub/Sub.
    pub fn validate(&self) -> Result<(), Error> {
        validate_name(&self.subscription, "subscriptions")?;
        self.endpoint().validate()?;
        ensure!(
            (Duration::from_secs(10)..=Duration::from_secs(600)).contains(&self.ack_deadline),
            Error::config,
            "Pub/Sub ack_deadline must be between 10 seconds and 10 minutes"
        );
        ensure!(
            self.max_lease >= self.ack_deadline,
            Error::config,
            "Pub/Sub max_lease must be at least ack_deadline"
        );
        Ok(())
    }
}

/// Configuration of a [`PubSubSink`](crate::adapters::pubsub::PubSubSink).
#[derive(Clone, Debug)]
pub struct PubSubSinkConfig {
    /// The topic every output is published to, such as
    /// `projects/my-project/topics/orders`.
    pub topic: String,
    /// Endpoint replacing `https://pubsub.googleapis.com`, such as a regional
    /// endpoint or the emulator at `http://localhost:8085`.
    pub endpoint: Option<String>,
    /// How the sink authenticates.
    pub credentials: PubSubCredentials,
    /// How long the client collects messages into one `Publish` request.
    pub batch_delay: Duration,
}

impl PubSubSinkConfig {
    /// A configuration for `topic` with the default settings.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            endpoint: None,
            credentials: PubSubCredentials::Default,
            batch_delay: Duration::from_millis(10),
        }
    }

    pub(super) fn endpoint(&self) -> Endpoint<'_> {
        Endpoint {
            endpoint: self.endpoint.as_deref(),
            credentials: &self.credentials,
        }
    }

    /// Checks the settings without contacting Pub/Sub.
    pub fn validate(&self) -> Result<(), Error> {
        validate_name(&self.topic, "topics")?;
        self.endpoint().validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_checks_values_locally() {
        let valid = PubSubSourceConfig::new("projects/p/subscriptions/orders");
        valid.validate().unwrap();
        for invalid in [
            PubSubSourceConfig {
                subscription: "orders".into(),
                ..valid.clone()
            },
            PubSubSourceConfig {
                subscription: "projects/p/topics/orders".into(),
                ..valid.clone()
            },
            PubSubSourceConfig {
                subscription: "projects//subscriptions/orders".into(),
                ..valid.clone()
            },
            PubSubSourceConfig {
                endpoint: Some("localhost:8085".into()),
                ..valid.clone()
            },
            PubSubSourceConfig {
                ack_deadline: Duration::from_secs(5),
                ..valid.clone()
            },
            PubSubSourceConfig {
                max_lease: Duration::from_secs(30),
                ..valid.clone()
            },
        ] {
            assert!(invalid.validate().is_err(), "{invalid:?}");
        }
        PubSubSinkConfig::new("projects/p/topics/orders")
            .validate()
            .unwrap();
        assert!(
            PubSubSinkConfig::new("projects/p/subscriptions/orders")
                .validate()
                .is_err()
        );
    }
}
