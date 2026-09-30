use super::{
    config::PulsarSinkConfig, producer::Producers, record::PulsarPublish,
    transaction::PulsarTransactionalSink,
};
use crate::{codec::Encoder, sink::Sink};
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

    pub(super) fn config(&self) -> &PulsarSinkConfig {
        &self.config
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

/// Publishes prepared messages and waits for the Pulsar broker receipt.
pub struct PulsarSink<C, T> {
    connection: Connection,
    codec: Arc<C>,
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
            connection: Connection::new(config),
            codec: Arc::new(codec),
            marker: PhantomData,
        }
    }

    /// Converts this sink into one that publishes in Pulsar transactions.
    ///
    /// Register the result with
    /// [`Subscription::transactional`](crate::Subscription::transactional)
    /// for exactly-once processing behind a `PulsarSource`. The broker must
    /// run with `transactionCoordinatorEnabled=true`.
    pub fn transactional(self) -> PulsarTransactionalSink<C, T> {
        PulsarTransactionalSink::new(self.connection, self.codec)
    }
}

impl<C: Encoder<T>, T: Send + Sync + 'static> Sink<PulsarPublish<T>> for PulsarSink<C, T> {
    type Prepared = PulsarPrepared;

    fn prepare(&self, output: PulsarPublish<T>) -> anyhow::Result<Self::Prepared> {
        prepare(&*self.codec, output)
    }

    async fn publish(&self, output: &Self::Prepared) -> anyhow::Result<()> {
        let producers = self.connection.producers().await?;
        producers.route(output).send(output, None).await
    }

    async fn close(&self) -> anyhow::Result<()> {
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
