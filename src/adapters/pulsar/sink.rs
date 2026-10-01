use super::{config::PulsarSinkConfig, producer::Producers, record::PulsarPublish};
use crate::{
    adapters::pending::PendingLimit,
    codec::Encoder,
    sink::{Completion, Sink},
};
use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

/// Encoded Pulsar output. Clones can be retried without rerunning the codec.
#[derive(Clone, Debug, Default)]
pub struct PulsarPrepared {
    /// `None` publishes a null value.
    pub value: Option<Vec<u8>>,
    pub properties: HashMap<String, String>,
    pub key: Option<Vec<u8>>,
    pub ordering_key: Option<Vec<u8>>,
    pub event_time: Option<u64>,
}

/// The lazily connected producers and closure state of a sink.
pub(super) struct Connection {
    config: PulsarSinkConfig,
    producers: Arc<RwLock<Option<Producers>>>,
    closed: AtomicBool,
}

impl Connection {
    pub(super) fn new(config: PulsarSinkConfig) -> Self {
        Self {
            config,
            producers: Arc::new(RwLock::new(None)),
            closed: AtomicBool::new(false),
        }
    }

    /// Connects on first use. The returned guard keeps `close` waiting until
    /// the publication that holds it has finished.
    pub(super) async fn producers(
        &self,
    ) -> anyhow::Result<OwnedRwLockReadGuard<Option<Producers>, Producers>> {
        loop {
            let producers = self.producers.clone().read_owned().await;
            anyhow::ensure!(
                !self.closed.load(Ordering::Acquire),
                "Pulsar sink is closed"
            );
            if let Ok(producers) = OwnedRwLockReadGuard::try_map(producers, Option::as_ref) {
                return Ok(producers);
            }
            let mut producers = self.producers.write().await;
            anyhow::ensure!(
                !self.closed.load(Ordering::Acquire),
                "Pulsar sink is closed"
            );
            if producers.is_none() {
                *producers = Some(Producers::connect(&self.config).await?);
            }
        }
    }

    pub(super) async fn close(&self) -> anyhow::Result<()> {
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
pub struct PulsarSink<C, T> {
    connection: Connection,
    codec: Arc<C>,
    /// One permit per message awaiting its broker receipt.
    pending: PendingLimit,
    marker: PhantomData<fn(T)>,
}

impl<C: Default, T> PulsarSink<C, T> {
    pub fn new(config: PulsarSinkConfig) -> Self {
        Self::with_codec(config, C::default())
    }
}

impl<C, T> PulsarSink<C, T> {
    pub fn with_codec(config: PulsarSinkConfig, codec: C) -> Self {
        Self {
            pending: PendingLimit::new(config.max_pending, "Pulsar"),
            connection: Connection::new(config),
            codec: Arc::new(codec),
            marker: PhantomData,
        }
    }
}

impl<C: Encoder<T>, T: Send + Sync + 'static> Sink<PulsarPublish<T>> for PulsarSink<C, T> {
    type Prepared = PulsarPrepared;

    fn prepare(&self, output: PulsarPublish<T>) -> anyhow::Result<Self::Prepared> {
        prepare(&*self.codec, output)
    }

    async fn publish(&self, output: &Self::Prepared) -> anyhow::Result<()> {
        self.submit(output).await?.wait().await
    }

    /// Returns once a producer has queued the message. The completion resolves
    /// on its broker receipt and keeps `close` waiting until then.
    async fn submit(&self, output: &Self::Prepared) -> anyhow::Result<Completion> {
        let producers = self.connection.producers().await?;
        let permit = self.pending.acquire().await?;
        let receipt = producers.route(output).enqueue(output);
        Ok(Completion::pending(async move {
            let _held = (producers, permit);
            receipt.await
        }))
    }

    async fn close(&self) -> anyhow::Result<()> {
        self.pending.close();
        self.connection.close().await
    }
}

pub(super) fn prepare<C: Encoder<T>, T>(
    codec: &C,
    output: PulsarPublish<T>,
) -> anyhow::Result<PulsarPrepared> {
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
