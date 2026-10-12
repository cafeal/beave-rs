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

/// Where and how a source or sink reaches SQS.
pub(super) struct Endpoint<'a> {
    pub(super) queue_url: &'a str,
    pub(super) region: Option<&'a str>,
    pub(super) endpoint_url: Option<&'a str>,
    pub(super) credentials: Option<&'a SqsCredentials>,
}

impl Endpoint<'_> {
    fn validate(&self) -> Result<(), Error> {
        ensure!(
            self.queue_url.starts_with("https://") || self.queue_url.starts_with("http://"),
            Error::config,
            "SQS queue URL must be an http or https URL"
        );
        ensure!(
            !self.queue_url.chars().any(char::is_whitespace),
            Error::config,
            "SQS queue URL must not contain whitespace"
        );
        if let Some(region) = self.region {
            ensure!(
                !region.trim().is_empty(),
                Error::config,
                "SQS region must not be empty"
            );
        }
        if let Some(url) = self.endpoint_url {
            ensure!(
                url.starts_with("https://") || url.starts_with("http://"),
                Error::config,
                "SQS endpoint URL must be an http or https URL"
            );
        }
        if let Some(credentials) = self.credentials {
            ensure!(
                !credentials.access_key_id.is_empty() && !credentials.secret_access_key.is_empty(),
                Error::config,
                "SQS credentials need an access key ID and a secret access key"
            );
        }
        Ok(())
    }

    /// Whether the queue is a FIFO queue, whose name ends in `.fifo`.
    pub(super) fn is_fifo(&self) -> bool {
        self.queue_url.ends_with(".fifo")
    }
}

/// Configuration of an [`SqsSource`](crate::adapters::sqs::SqsSource).
#[derive(Clone, Debug)]
pub struct SqsSourceConfig {
    /// The queue to consume, such as
    /// `https://sqs.eu-west-1.amazonaws.com/123456789012/orders`.
    pub queue_url: String,
    /// AWS region; unset uses the default region provider chain.
    pub region: Option<String>,
    /// Endpoint replacing the regional SQS endpoint, such as a local emulator.
    pub endpoint_url: Option<String>,
    /// Static credentials; unset uses the default credential provider chain.
    pub credentials: Option<SqsCredentials>,
    /// Messages one `ReceiveMessage` call returns at most, from 1 to 10.
    pub max_messages: i32,
    /// How long `ReceiveMessage` waits for a message (long polling), up to 20 seconds.
    pub wait_time: Duration,
    /// How long a received message stays invisible to other consumers. The
    /// source extends it while the delivery is buffered or in flight, up to
    /// SQS's limit of 12 hours from its receipt.
    pub visibility_timeout: Duration,
    /// Attempts to delete a message when it is acknowledged.
    pub ack_retry: RetryPolicy,
}

impl SqsSourceConfig {
    /// A configuration for `queue_url` with the default settings.
    pub fn new(queue_url: impl Into<String>) -> Self {
        Self {
            queue_url: queue_url.into(),
            region: None,
            endpoint_url: None,
            credentials: None,
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

    pub(super) fn endpoint(&self) -> Endpoint<'_> {
        Endpoint {
            queue_url: &self.queue_url,
            region: self.region.as_deref(),
            endpoint_url: self.endpoint_url.as_deref(),
            credentials: self.credentials.as_ref(),
        }
    }

    /// Checks the settings without contacting SQS.
    pub fn validate(&self) -> Result<(), Error> {
        self.endpoint().validate()?;
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

/// Configuration of an [`SqsSink`](crate::adapters::sqs::SqsSink).
#[derive(Clone, Debug)]
pub struct SqsSinkConfig {
    /// The queue every output is sent to.
    pub queue_url: String,
    /// AWS region; unset uses the default region provider chain.
    pub region: Option<String>,
    /// Endpoint replacing the regional SQS endpoint, such as a local emulator.
    pub endpoint_url: Option<String>,
    /// Static credentials; unset uses the default credential provider chain.
    pub credentials: Option<SqsCredentials>,
}

impl SqsSinkConfig {
    /// A configuration for `queue_url` with the default settings.
    pub fn new(queue_url: impl Into<String>) -> Self {
        Self {
            queue_url: queue_url.into(),
            region: None,
            endpoint_url: None,
            credentials: None,
        }
    }

    pub(super) fn endpoint(&self) -> Endpoint<'_> {
        Endpoint {
            queue_url: &self.queue_url,
            region: self.region.as_deref(),
            endpoint_url: self.endpoint_url.as_deref(),
            credentials: self.credentials.as_ref(),
        }
    }

    /// Checks the settings without contacting SQS.
    pub fn validate(&self) -> Result<(), Error> {
        self.endpoint().validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_checks_values_locally() {
        let valid = SqsSourceConfig::new("https://sqs.eu-west-1.amazonaws.com/1/orders");
        valid.validate().unwrap();
        for invalid in [
            SqsSourceConfig {
                queue_url: "orders".into(),
                ..valid.clone()
            },
            SqsSourceConfig {
                max_messages: 11,
                ..valid.clone()
            },
            SqsSourceConfig {
                wait_time: Duration::from_secs(21),
                ..valid.clone()
            },
            SqsSourceConfig {
                visibility_timeout: Duration::ZERO,
                ..valid.clone()
            },
            SqsSourceConfig {
                endpoint_url: Some("localhost:9324".into()),
                ..valid.clone()
            },
            SqsSourceConfig {
                credentials: Some(SqsCredentials::new("key", "")),
                ..valid.clone()
            },
        ] {
            assert!(invalid.validate().is_err(), "{invalid:?}");
        }
        SqsSinkConfig::new("http://localhost:9324/000000000000/orders.fifo")
            .validate()
            .unwrap();
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
        assert!(
            SqsSinkConfig::new("http://q/1/orders.fifo")
                .endpoint()
                .is_fifo()
        );
        assert!(!SqsSinkConfig::new("http://q/1/orders").endpoint().is_fifo());
    }
}
