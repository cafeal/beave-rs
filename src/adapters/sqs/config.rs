use crate::error::{Error, ensure};
use crate::retry::RetryPolicy;
use std::{fmt, time::Duration};

/// Longest time SQS keeps a received message invisible, counted from its receipt.
pub(super) const MAX_VISIBILITY: Duration = Duration::from_secs(12 * 60 * 60);

/// Static AWS credentials. Leave them unset to use the default provider chain:
/// environment variables, shared configuration files, web identity, and
/// container or instance metadata.
#[derive(Clone)]
pub struct SqsCredentials {
    /// The AWS access key ID.
    pub access_key_id: String,
    /// The secret access key.
    pub secret_access_key: String,
    /// The session token of temporary credentials.
    pub session_token: Option<String>,
}

impl SqsCredentials {
    /// Long-term credentials without a session token.
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token: None,
        }
    }
}

impl fmt::Debug for SqsCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqsCredentials")
            .field("access_key_id", &self.access_key_id)
            .finish_non_exhaustive()
    }
}

impl SqsCredentials {
    fn validate(&self) -> Result<(), Error> {
        ensure!(
            !self.access_key_id.is_empty() && !self.secret_access_key.is_empty(),
            Error::config,
            "SQS credentials need an access key ID and a secret access key"
        );
        Ok(())
    }
}

/// How an [`Sqs`](super::Sqs) handle reaches SQS.
#[derive(Clone, Debug, Default)]
pub(super) struct ClientConfig {
    pub(super) region: Option<String>,
    pub(super) endpoint_url: Option<String>,
    pub(super) credentials: Option<SqsCredentials>,
}

impl ClientConfig {
    pub(super) fn validate(&self) -> Result<(), Error> {
        if let Some(region) = &self.region {
            ensure!(
                !region.trim().is_empty(),
                Error::config,
                "SQS region must not be empty"
            );
        }
        if let Some(url) = &self.endpoint_url {
            ensure!(
                url.starts_with("https://") || url.starts_with("http://"),
                Error::config,
                "SQS endpoint URL must be an http or https URL"
            );
        }
        if let Some(credentials) = &self.credentials {
            credentials.validate()?;
        }
        Ok(())
    }
}

/// Settings of an [`SqsSource`](super::SqsSource).
#[derive(Clone, Debug)]
pub(super) struct SourceConfig {
    pub(super) queue_url: String,
    pub(super) max_messages: i32,
    pub(super) wait_time: Duration,
    pub(super) visibility_timeout: Duration,
    pub(super) ack_retry: RetryPolicy,
}

impl SourceConfig {
    pub(super) fn new(queue_url: String) -> Self {
        Self {
            queue_url,
            max_messages: 10,
            wait_time: Duration::from_secs(20),
            visibility_timeout: Duration::from_secs(30),
            ack_retry: RetryPolicy {
                max_attempts: 5,
                initial_delay: Duration::from_millis(100),
                max_delay: Duration::from_secs(2),
                ..RetryPolicy::default()
            },
        }
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
        validate_queue_url(&self.queue_url)?;
        ensure!(
            (1..=10).contains(&self.max_messages),
            Error::config,
            "SQS max_messages must be between 1 and 10"
        );
        ensure!(
            self.wait_time <= Duration::from_secs(20),
            Error::config,
            "SQS wait_time must be at most 20 seconds"
        );
        ensure!(
            (Duration::from_secs(1)..=MAX_VISIBILITY).contains(&self.visibility_timeout),
            Error::config,
            "SQS visibility_timeout must be between 1 second and 12 hours"
        );
        self.ack_retry.validate()
    }
}

/// Checks the URL of the queue a source or sink uses.
pub(super) fn validate_queue_url(queue_url: &str) -> Result<(), Error> {
    ensure!(
        queue_url.starts_with("https://") || queue_url.starts_with("http://"),
        Error::config,
        "SQS queue URL must be an http or https URL"
    );
    ensure!(
        !queue_url.chars().any(char::is_whitespace),
        Error::config,
        "SQS queue URL must not contain whitespace"
    );
    Ok(())
}

/// Whether the queue is a FIFO queue, whose name ends in `.fifo`.
pub(super) fn is_fifo(queue_url: &str) -> bool {
    queue_url.ends_with(".fifo")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_checks_values_locally() {
        let valid = SourceConfig::new("https://sqs.eu-west-1.amazonaws.com/1/orders".into());
        valid.validate().unwrap();
        for invalid in [
            SourceConfig {
                queue_url: "orders".into(),
                ..valid.clone()
            },
            SourceConfig {
                max_messages: 11,
                ..valid.clone()
            },
            SourceConfig {
                wait_time: Duration::from_secs(21),
                ..valid.clone()
            },
            SourceConfig {
                visibility_timeout: Duration::ZERO,
                ..valid.clone()
            },
        ] {
            assert!(invalid.validate().is_err(), "{invalid:?}");
        }
        for invalid in [
            ClientConfig {
                endpoint_url: Some("localhost:9324".into()),
                ..ClientConfig::default()
            },
            ClientConfig {
                credentials: Some(SqsCredentials::new("key", "")),
                ..ClientConfig::default()
            },
            ClientConfig {
                region: Some(" ".into()),
                ..ClientConfig::default()
            },
        ] {
            assert!(invalid.validate().is_err(), "{invalid:?}");
        }
        validate_queue_url("http://localhost:9324/000000000000/orders.fifo").unwrap();
    }

    #[test]
    fn debug_output_hides_the_secret() {
        let mut credentials = SqsCredentials::new("AKIDEXAMPLE", "secret-key");
        credentials.session_token = Some("token".into());
        let debug = format!("{credentials:?}");
        assert!(debug.contains("AKIDEXAMPLE"));
        assert!(!debug.contains("secret-key") && !debug.contains("token"));
    }

    #[test]
    fn fifo_queues_are_recognized_by_name() {
        assert!(is_fifo("http://q/1/orders.fifo"));
        assert!(!is_fifo("http://q/1/orders"));
    }
}
