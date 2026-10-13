use super::{
    config::{PulsarAuthentication, SinkConfig, SourceConfig, validate_service_url},
    sink::PulsarSink,
    source::PulsarSource,
};
use crate::error::{BoxError, Error};
use magnetar::{PulsarClient, proto::SupervisorConfig};
use std::{fmt, sync::Arc};
use tokio::{runtime::Handle, sync::OnceCell};

/// A connection to a Pulsar cluster that the sources and sinks built from it
/// share.
///
/// The client connects on the first `receive` or publication of any of them,
/// which also validates the settings, and reconnects their producers and
/// consumers after the broker connection drops. It closes once this handle,
/// every source and sink built from it, and every delivery that can still
/// acknowledge through it have been dropped.
///
/// Each builder method returns a handle with a connection of its own, so
/// configure the handle before building sources and sinks from it.
///
/// ```no_run
/// use beavers::{Json, adapters::pulsar::Pulsar};
///
/// let pulsar = Pulsar::new("pulsar://localhost:6650").token("eyJhbGciOi...");
/// let orders = pulsar.source::<Json, String>("orders", "billing");
/// let invoices = pulsar.sink::<Json, String>("invoices");
/// ```
#[derive(Clone)]
pub struct Pulsar {
    shared: Arc<Shared>,
}

struct Shared {
    service_url: String,
    authentication: Option<PulsarAuthentication>,
    client: OnceCell<PulsarClient>,
}

impl Pulsar {
    /// A handle for the broker at `service_url`, using the `pulsar://` or
    /// `pulsar+ssl://` scheme, without authentication.
    pub fn new(service_url: impl Into<String>) -> Self {
        Self::from_parts(service_url.into(), None)
    }

    /// Authenticates with a JWT, using the `token` method.
    pub fn token(self, token: impl Into<Vec<u8>>) -> Self {
        self.authentication(PulsarAuthentication::token(token))
    }

    /// Authenticates with static credentials sent with Pulsar's `CONNECT`
    /// command.
    pub fn authentication(self, authentication: PulsarAuthentication) -> Self {
        Self::from_parts(self.shared.service_url.clone(), Some(authentication))
    }

    /// A source that consumes `topic` through `subscription` and decodes with
    /// the codec's default value.
    ///
    /// Defaults: a [`Shared`](super::PulsarSubscriptionType::Shared)
    /// subscription, a `buffer_size` of 100, empty payloads treated as
    /// tombstones, and an `ack_retry` of 5 attempts with delays from 100 ms up
    /// to 2 s.
    pub fn source<C: Default, T>(
        &self,
        topic: impl Into<String>,
        subscription: impl Into<String>,
    ) -> PulsarSource<C, T> {
        self.source_with_codec(C::default(), topic, subscription)
    }

    /// A source like [`source`](Self::source) that decodes with `codec`.
    pub fn source_with_codec<C, T>(
        &self,
        codec: C,
        topic: impl Into<String>,
        subscription: impl Into<String>,
    ) -> PulsarSource<C, T> {
        PulsarSource::new(
            self.clone(),
            SourceConfig::new(topic.into(), subscription.into()),
            codec,
        )
    }

    /// A sink that publishes to `topic` and encodes with the codec's default
    /// value.
    ///
    /// Defaults: no producer name, a `max_pending` of 1000, and a `send_retry`
    /// of 10 attempts with delays from 100 ms up to 5 s.
    pub fn sink<C: Default, T>(&self, topic: impl Into<String>) -> PulsarSink<C, T> {
        self.sink_with_codec(C::default(), topic)
    }

    /// A sink like [`sink`](Self::sink) that encodes with `codec`.
    pub fn sink_with_codec<C, T>(&self, codec: C, topic: impl Into<String>) -> PulsarSink<C, T> {
        PulsarSink::new(self.clone(), SinkConfig::new(topic.into()), codec)
    }

    /// Checks the settings without network access.
    ///
    /// Fails when the service URL is empty, contains whitespace, or does not
    /// use `pulsar://` or `pulsar+ssl://`, or when the authentication has an
    /// empty method or no data.
    pub fn validate(&self) -> Result<(), Error> {
        validate_service_url(&self.shared.service_url)?;
        if let Some(authentication) = &self.shared.authentication {
            authentication.validate()?;
        }
        Ok(())
    }

    /// The connected client, connecting on first use.
    pub(super) async fn client(&self) -> Result<&PulsarClient, BoxError> {
        self.shared
            .client
            .get_or_try_init(|| async {
                self.validate()?;
                let mut builder = PulsarClient::builder()
                    .service_url(&self.shared.service_url)
                    .enable_reconnect(SupervisorConfig::default());
                if let Some(authentication) = &self.shared.authentication {
                    builder = builder.auth(authentication.provider());
                }
                Ok(builder.build().await?)
            })
            .await
    }

    fn from_parts(service_url: String, authentication: Option<PulsarAuthentication>) -> Self {
        Self {
            shared: Arc::new(Shared {
                service_url,
                authentication,
                client: OnceCell::new(),
            }),
        }
    }
}

impl fmt::Debug for Pulsar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pulsar")
            .field("service_url", &self.shared.service_url)
            .field("authentication", &self.shared.authentication)
            .finish_non_exhaustive()
    }
}

/// Acknowledging through a closed client never completes, so the client closes
/// only when nothing that can acknowledge or publish through it remains.
impl Drop for Shared {
    fn drop(&mut self) {
        if let (Some(client), Ok(runtime)) = (self.client.take(), Handle::try_current()) {
            runtime.spawn(client.close());
        }
    }
}

/// The topics that hold `topic`'s messages: each partition of a partitioned
/// topic, or the topic itself.
pub(super) async fn partition_topics(
    client: &PulsarClient,
    topic: &str,
) -> Result<Vec<String>, BoxError> {
    let partitions = client.partitions_for_topic(topic).await?;
    Ok(if partitions == 0 {
        vec![topic.to_owned()]
    } else {
        (0..partitions)
            .map(|partition| format!("{topic}-partition-{partition}"))
            .collect()
    })
}
