//! The credentials and subscriber client that the sources and sinks of a
//! handle share.
use super::{
    config::{SinkConfig, SourceConfig, validate_endpoint},
    sink::PubSubSink,
    source::PubSubSource,
};
use crate::error::{BoxError, Context, Error};
use google_cloud_auth::credentials::{
    Builder as DefaultCredentials, Credentials, anonymous::Builder as Anonymous,
};
use google_cloud_pubsub::client::{Publisher, Subscriber};
use std::{fmt, sync::Arc, time::Duration};
use tokio::sync::OnceCell;

/// A connection to Pub/Sub that the sources and sinks built from it share.
///
/// The handle resolves its credentials once and keeps one subscriber client
/// for all of its sources. Each sink has a publisher client of its own,
/// because a Pub/Sub publisher batches messages for one topic, and it uses the
/// handle's credentials. Credentials and clients are created on the first
/// `receive` or publication that needs them, which also validates the
/// settings.
///
/// Without explicit settings the handle uses Application Default Credentials:
/// the `GOOGLE_APPLICATION_CREDENTIALS` key file, the gcloud user credentials,
/// or the metadata server of the Google Cloud environment the application runs
/// in. Each builder method returns a handle with credentials and clients of
/// its own, so configure the handle before building sources and sinks from it.
///
/// ```no_run
/// use beavers::{Json, adapters::pubsub::PubSub};
///
/// let pubsub = PubSub::new();
/// let orders = pubsub.source::<Json, String>("projects/my-project/subscriptions/billing");
/// let invoices = pubsub.sink::<Json, String>("projects/my-project/topics/invoices");
///
/// let emulator = PubSub::new().endpoint("http://localhost:8085").anonymous();
/// ```
#[derive(Clone)]
pub struct PubSub {
    shared: Arc<Shared>,
}

#[derive(Clone, Debug, Default)]
struct Settings {
    endpoint: Option<String>,
    anonymous: bool,
}

struct Shared {
    settings: Settings,
    credentials: OnceCell<Credentials>,
    subscriber: OnceCell<Subscriber>,
}

impl PubSub {
    /// A handle for `https://pubsub.googleapis.com` with Application Default
    /// Credentials.
    pub fn new() -> Self {
        Self::from_settings(Settings::default())
    }

    /// An endpoint replacing `https://pubsub.googleapis.com`, such as a
    /// regional endpoint or the emulator at `http://localhost:8085`.
    pub fn endpoint(self, endpoint: impl Into<String>) -> Self {
        self.configure(|settings| settings.endpoint = Some(endpoint.into()))
    }

    /// Sends no credentials, for the Pub/Sub emulator.
    pub fn anonymous(self) -> Self {
        self.configure(|settings| settings.anonymous = true)
    }

    /// A source that consumes `subscription`, such as
    /// `projects/my-project/subscriptions/orders`, and decodes with the codec's
    /// default value.
    ///
    /// Defaults: a `max_outstanding_messages` of 1000, a
    /// `max_outstanding_bytes` of 100 MiB, a 60 second `ack_deadline`, and a
    /// one hour `max_lease`.
    pub fn source<C: Default, T>(&self, subscription: impl Into<String>) -> PubSubSource<C, T> {
        self.source_with_codec(C::default(), subscription)
    }

    /// A source like [`source`](Self::source) that decodes with `codec`.
    pub fn source_with_codec<C, T>(
        &self,
        codec: C,
        subscription: impl Into<String>,
    ) -> PubSubSource<C, T> {
        PubSubSource::new(self.clone(), SourceConfig::new(subscription.into()), codec)
    }

    /// A sink that publishes every output to `topic`, such as
    /// `projects/my-project/topics/orders`, and encodes with the codec's
    /// default value.
    ///
    /// Defaults: a 10 ms `batch_delay`.
    pub fn sink<C: Default, T>(&self, topic: impl Into<String>) -> PubSubSink<C, T> {
        self.sink_with_codec(C::default(), topic)
    }

    /// A sink like [`sink`](Self::sink) that encodes with `codec`.
    pub fn sink_with_codec<C, T>(&self, codec: C, topic: impl Into<String>) -> PubSubSink<C, T> {
        PubSubSink::new(self.clone(), SinkConfig::new(topic.into()), codec)
    }

    /// Checks the settings without contacting Pub/Sub.
    ///
    /// Fails when the endpoint is not an http or https URL.
    pub fn validate(&self) -> Result<(), Error> {
        match &self.shared.settings.endpoint {
            Some(url) => validate_endpoint(url),
            None => Ok(()),
        }
    }

    /// The subscriber client, created on first use.
    pub(super) async fn subscriber(&self) -> Result<&Subscriber, BoxError> {
        self.shared
            .subscriber
            .get_or_try_init(|| async {
                let mut builder =
                    Subscriber::builder().with_credentials(self.credentials().await?.clone());
                if let Some(url) = &self.shared.settings.endpoint {
                    builder = builder.with_endpoint(url);
                }
                Ok::<_, BoxError>(
                    builder
                        .build()
                        .await
                        .context("creating the Pub/Sub subscriber client")?,
                )
            })
            .await
    }

    /// A publisher client for `topic` that batches for up to `batch_delay`.
    pub(super) async fn publisher(
        &self,
        topic: &str,
        batch_delay: Duration,
    ) -> Result<Publisher, BoxError> {
        let mut builder = Publisher::builder(topic)
            .set_delay_threshold(batch_delay)
            .with_credentials(self.credentials().await?.clone());
        if let Some(url) = &self.shared.settings.endpoint {
            builder = builder.with_endpoint(url);
        }
        Ok(builder
            .build()
            .await
            .context("creating the Pub/Sub publisher client")?)
    }

    /// The credentials, resolved on first use.
    async fn credentials(&self) -> Result<&Credentials, BoxError> {
        self.shared
            .credentials
            .get_or_try_init(|| async {
                self.validate()?;
                if self.shared.settings.anonymous {
                    return Ok(Anonymous::new().build());
                }
                Ok::<_, BoxError>(
                    DefaultCredentials::default()
                        .build()
                        .context("resolving Google Cloud credentials")?,
                )
            })
            .await
    }

    fn configure(self, change: impl FnOnce(&mut Settings)) -> Self {
        let mut settings = self.shared.settings.clone();
        change(&mut settings);
        Self::from_settings(settings)
    }

    fn from_settings(settings: Settings) -> Self {
        Self {
            shared: Arc::new(Shared {
                settings,
                credentials: OnceCell::new(),
                subscriber: OnceCell::new(),
            }),
        }
    }
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for PubSub {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PubSub")
            .field("endpoint", &self.shared.settings.endpoint)
            .field("anonymous", &self.shared.settings.anonymous)
            .finish_non_exhaustive()
    }
}
