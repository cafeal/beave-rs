use super::{client::Pulsar, config::SinkConfig, producer::Producers, record::PulsarPublish};
use crate::{
    adapters::pending::PendingLimit,
    codec::Encoder,
    error::{BoxError, Error, ensure},
    retry::RetryPolicy,
    sink::{Completion, Sink},
};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

/// Encoded Pulsar output. Clones can be retried without rerunning the codec.
#[derive(Clone, Debug, Default)]
pub struct PulsarPrepared {
    pub(super) value: Option<Vec<u8>>,
    pub(super) properties: HashMap<String, String>,
    pub(super) key: Option<Vec<u8>>,
    pub(super) ordering_key: Option<Vec<u8>>,
    pub(super) event_time: Option<u64>,
}

impl PulsarPrepared {
    /// Encoded value bytes, or `None` for a null value.
    pub fn payload(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Message key bytes, or `None` for a keyless message.
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// User properties of the message.
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

/// The lazily connected producers and closure state of a sink.
pub(super) struct Connection {
    pulsar: Pulsar,
    config: SinkConfig,
    producers: Arc<RwLock<Option<Producers>>>,
    closed: AtomicBool,
}

impl Connection {
    pub(super) fn new(pulsar: Pulsar, config: SinkConfig) -> Self {
        Self {
            pulsar,
            config,
            producers: Arc::new(RwLock::new(None)),
            closed: AtomicBool::new(false),
        }
    }

    /// Connects on first use. The returned guard keeps `close` waiting until
    /// the publication that holds it has finished.
    pub(super) async fn producers(
        &self,
    ) -> Result<OwnedRwLockReadGuard<Option<Producers>, Producers>, BoxError> {
        loop {
            let producers = self.producers.clone().read_owned().await;
            ensure!(
                !self.closed.load(Ordering::Acquire),
                Error::closed,
                "Pulsar sink is closed"
            );
            if let Ok(producers) = OwnedRwLockReadGuard::try_map(producers, Option::as_ref) {
                return Ok(producers);
            }
            let mut producers = self.producers.write().await;
            ensure!(
                !self.closed.load(Ordering::Acquire),
                Error::closed,
                "Pulsar sink is closed"
            );
            if producers.is_none() {
                *producers = Some(Producers::connect(&self.pulsar, &self.config).await?);
            }
        }
    }

    pub(super) async fn close(&self) -> Result<(), BoxError> {
        self.closed.store(true, Ordering::Release);
        let producers = self.producers.write().await.take();
        match producers {
            Some(producers) => producers.close().await,
            None => Ok(()),
        }
    }
}

/// Queues prepared messages on the producers and completes each one on its
/// Pulsar broker receipt.
///
/// Built with [`Pulsar::sink`]. The producers are created on the first
/// publication, which also validates the settings. Closing the sink closes its
/// producers; the client stays open for the other sources and sinks of the
/// handle.
pub struct PulsarSink<C> {
    connection: Connection,
    codec: Arc<C>,
    /// One permit per message awaiting its broker receipt.
    pending: PendingLimit,
}

impl<C> PulsarSink<C> {
    pub(super) fn new(pulsar: Pulsar, config: SinkConfig, codec: C) -> Self {
        Self {
            pending: PendingLimit::new(config.max_pending, "Pulsar"),
            connection: Connection::new(pulsar, config),
            codec: Arc::new(codec),
        }
    }

    /// Names each partition's producer. By default the broker assigns names.
    pub fn producer_name(mut self, name: impl Into<String>) -> Self {
        self.connection.config.producer_name = Some(name.into());
        self
    }

    /// Messages queued by `submit` whose broker receipt has not arrived.
    /// `submit` waits while this many are outstanding. Defaults to 1000.
    pub fn max_pending(mut self, max_pending: usize) -> Self {
        self.connection.config.max_pending = max_pending;
        self.pending = PendingLimit::new(max_pending, "Pulsar");
        self
    }

    /// Sends of a message, including the first, when the broker rejects it.
    /// A broker that is shutting down or unloading the topic rejects the sends
    /// in flight, and a later send reaches the topic's next owner. Defaults to
    /// 10 attempts with delays from 100 ms up to 5 s.
    pub fn send_retry(mut self, send_retry: RetryPolicy) -> Self {
        self.connection.config.send_retry = send_retry;
        self
    }

    /// Checks the handle and sink settings without network access.
    ///
    /// Fails when the handle is invalid, when the topic or a set producer name
    /// is empty, when `max_pending` is zero or too large, or when `send_retry`
    /// is invalid.
    pub fn validate(&self) -> Result<(), Error> {
        self.connection.pulsar.validate()?;
        self.connection.config.validate()
    }
}

impl<C: Encoder<Value: Send + Sync + 'static>> Sink<PulsarPublish<C::Value>> for PulsarSink<C> {
    type Prepared = PulsarPrepared;

    fn prepare(&self, output: PulsarPublish<C::Value>) -> Result<Self::Prepared, BoxError> {
        prepare(&*self.codec, output)
    }

    async fn publish(&self, output: &Self::Prepared) -> Result<(), BoxError> {
        self.submit(output).await?.wait().await
    }

    /// Returns once a producer has queued the message. The completion resolves
    /// on its broker receipt and keeps `close` waiting until then.
    async fn submit(&self, output: &Self::Prepared) -> Result<Completion, BoxError> {
        let producers = self.connection.producers().await?;
        let permit = self.pending.acquire().await?;
        let receipt = producers.route(output).enqueue(output);
        Ok(Completion::pending(async move {
            let _held = (producers, permit);
            receipt.await
        }))
    }

    async fn close(&self) -> Result<(), BoxError> {
        self.pending.close();
        self.connection.close().await
    }
}

pub(super) fn prepare<C: Encoder>(
    codec: &C,
    output: PulsarPublish<C::Value>,
) -> Result<PulsarPrepared, BoxError> {
    Ok(PulsarPrepared {
        value: output
            .value
            .as_ref()
            .map(|value| codec.encode(value))
            .transpose()?,
        properties: output.properties,
        key: output.key,
        ordering_key: output.ordering_key,
        event_time: output.event_time,
    })
}
