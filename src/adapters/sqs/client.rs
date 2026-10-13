//! The SQS client that the sources and sinks of a handle share.
use super::{
    config::{ClientConfig, SourceConfig, SqsCredentials},
    sink::SqsSink,
    source::SqsSource,
};
use crate::error::{BoxError, Error};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_sqs::{Client, config::Credentials};
use std::sync::Arc;
use tokio::sync::OnceCell;

/// An SQS client that the sources and sinks built from it share.
///
/// The client, with its credential provider and HTTP connection pool, is
/// created on the first `receive` or publication of any of them, which also
/// validates the settings. Without explicit settings it uses the default AWS
/// region and credential provider chains: environment variables, shared
/// configuration files, web identity, and container or instance metadata.
///
/// Each builder method returns a handle with a client of its own, so configure
/// the handle before building sources and sinks from it.
///
/// ```no_run
/// use beavers::{Json, adapters::sqs::Sqs};
///
/// let sqs = Sqs::new().region("eu-west-1");
/// let orders =
///     sqs.source::<Json, String>("https://sqs.eu-west-1.amazonaws.com/123456789012/orders");
/// let invoices =
///     sqs.sink::<Json, String>("https://sqs.eu-west-1.amazonaws.com/123456789012/invoices");
/// ```
#[derive(Clone, Debug)]
pub struct Sqs {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    config: ClientConfig,
    client: OnceCell<Client>,
}

impl Sqs {
    /// A handle using the default region and credential provider chains.
    pub fn new() -> Self {
        Self::from_config(ClientConfig::default())
    }

    /// The AWS region, replacing the default region provider chain.
    pub fn region(self, region: impl Into<String>) -> Self {
        self.configure(|config| config.region = Some(region.into()))
    }

    /// An endpoint replacing the regional SQS endpoint, such as a local
    /// emulator.
    pub fn endpoint_url(self, endpoint_url: impl Into<String>) -> Self {
        self.configure(|config| config.endpoint_url = Some(endpoint_url.into()))
    }

    /// Static credentials, replacing the default credential provider chain.
    pub fn credentials(self, credentials: SqsCredentials) -> Self {
        self.configure(|config| config.credentials = Some(credentials))
    }

    /// A source that consumes the queue at `queue_url`, such as
    /// `https://sqs.eu-west-1.amazonaws.com/123456789012/orders`, and decodes
    /// with the codec's default value.
    ///
    /// Defaults: a `max_messages` of 10, a 20 second `wait_time`, a 30 second
    /// `visibility_timeout`, and an `ack_retry` of 5 attempts with delays from
    /// 100 ms up to 2 s.
    pub fn source<C: Default, T>(&self, queue_url: impl Into<String>) -> SqsSource<C, T> {
        self.source_with_codec(C::default(), queue_url)
    }

    /// A source like [`source`](Self::source) that decodes with `codec`.
    pub fn source_with_codec<C, T>(
        &self,
        codec: C,
        queue_url: impl Into<String>,
    ) -> SqsSource<C, T> {
        SqsSource::new(self.clone(), SourceConfig::new(queue_url.into()), codec)
    }

    /// A sink that sends every output to the queue at `queue_url` and encodes
    /// with the codec's default value.
    pub fn sink<C: Default, T>(&self, queue_url: impl Into<String>) -> SqsSink<C, T> {
        self.sink_with_codec(C::default(), queue_url)
    }

    /// A sink like [`sink`](Self::sink) that encodes with `codec`.
    pub fn sink_with_codec<C, T>(&self, codec: C, queue_url: impl Into<String>) -> SqsSink<C, T> {
        SqsSink::new(self.clone(), queue_url.into(), codec)
    }

    /// Checks the settings without contacting SQS.
    ///
    /// Fails when the region is blank, the endpoint URL is not an http or https
    /// URL, or the credentials lack an access key ID or secret access key.
    pub fn validate(&self) -> Result<(), Error> {
        self.shared.config.validate()
    }

    /// The client, created on first use.
    pub(super) async fn client(&self) -> Result<&Client, BoxError> {
        self.shared
            .client
            .get_or_try_init(|| async {
                self.validate()?;
                let config = &self.shared.config;
                let mut loader = aws_config::defaults(BehaviorVersion::latest());
                if let Some(region) = &config.region {
                    loader = loader.region(Region::new(region.clone()));
                }
                if let Some(url) = &config.endpoint_url {
                    loader = loader.endpoint_url(url);
                }
                if let Some(credentials) = &config.credentials {
                    loader = loader.credentials_provider(Credentials::new(
                        &credentials.access_key_id,
                        &credentials.secret_access_key,
                        credentials.session_token.clone(),
                        None,
                        "beavers",
                    ));
                }
                Ok::<_, BoxError>(Client::new(&loader.load().await))
            })
            .await
    }

    fn configure(self, change: impl FnOnce(&mut ClientConfig)) -> Self {
        let mut config = self.shared.config.clone();
        change(&mut config);
        Self::from_config(config)
    }

    fn from_config(config: ClientConfig) -> Self {
        Self {
            shared: Arc::new(Shared {
                config,
                client: OnceCell::new(),
            }),
        }
    }
}

impl Default for Sqs {
    fn default() -> Self {
        Self::new()
    }
}
