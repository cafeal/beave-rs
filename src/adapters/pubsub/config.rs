use crate::error::{Error, ensure};
use std::time::Duration;

/// Checks an endpoint replacing `https://pubsub.googleapis.com`.
pub(super) fn validate_endpoint(url: &str) -> Result<(), Error> {
    ensure!(
        url.starts_with("https://") || url.starts_with("http://"),
        Error::config,
        "Pub/Sub endpoint must be an http or https URL"
    );
    Ok(())
}

/// Checks that `name` is `projects/{project}/{collection}/{id}`.
pub(super) fn validate_name(name: &str, collection: &str) -> Result<(), Error> {
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

/// Settings of a [`PubSubSource`](super::PubSubSource).
#[derive(Clone, Debug)]
pub(super) struct SourceConfig {
    pub(super) subscription: String,
    pub(super) max_outstanding_messages: i64,
    pub(super) max_outstanding_bytes: i64,
    pub(super) ack_deadline: Duration,
    pub(super) max_lease: Duration,
}

impl SourceConfig {
    pub(super) fn new(subscription: String) -> Self {
        Self {
            subscription,
            max_outstanding_messages: 1000,
            max_outstanding_bytes: 100 * 1024 * 1024,
            ack_deadline: Duration::from_secs(60),
            max_lease: Duration::from_secs(60 * 60),
        }
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
        validate_name(&self.subscription, "subscriptions")?;
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

/// Settings of a [`PubSubSink`](super::PubSubSink).
#[derive(Clone, Debug)]
pub(super) struct SinkConfig {
    pub(super) topic: String,
    pub(super) batch_delay: Duration,
}

impl SinkConfig {
    pub(super) fn new(topic: String) -> Self {
        Self {
            topic,
            batch_delay: Duration::from_millis(10),
        }
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
        validate_name(&self.topic, "topics")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_checks_values_locally() {
        let valid = SourceConfig::new("projects/p/subscriptions/orders".into());
        valid.validate().unwrap();
        for invalid in [
            SourceConfig {
                subscription: "orders".into(),
                ..valid.clone()
            },
            SourceConfig {
                subscription: "projects/p/topics/orders".into(),
                ..valid.clone()
            },
            SourceConfig {
                subscription: "projects//subscriptions/orders".into(),
                ..valid.clone()
            },
            SourceConfig {
                ack_deadline: Duration::from_secs(5),
                ..valid.clone()
            },
            SourceConfig {
                max_lease: Duration::from_secs(30),
                ..valid.clone()
            },
        ] {
            assert!(invalid.validate().is_err(), "{invalid:?}");
        }
        SinkConfig::new("projects/p/topics/orders".into())
            .validate()
            .unwrap();
        assert!(
            SinkConfig::new("projects/p/subscriptions/orders".into())
                .validate()
                .is_err()
        );
        validate_endpoint("http://localhost:8085").unwrap();
        assert!(validate_endpoint("localhost:8085").is_err());
    }
}
