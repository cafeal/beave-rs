use super::{config::KafkaSinkConfig, record::KafkaPublish, transaction::KafkaTransactionalSink};
use crate::{codec::Encoder, sink::Sink};
use rdkafka::{
    ClientConfig,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord, Producer},
    util::Timeout,
};
use std::{
    marker::PhantomData,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

/// An encoded Kafka record. Preparing a value performs all codec work once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaPrepared {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

struct State {
    producer: Mutex<Option<FutureProducer>>,
    closed: AtomicBool,
}

/// Publishes records only after Kafka reports successful delivery.
pub struct KafkaSink<C, T> {
    config: KafkaSinkConfig,
    codec: Arc<C>,
    state: Arc<State>,
    marker: PhantomData<fn(T)>,
}

impl<C, T> Clone for KafkaSink<C, T> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            codec: self.codec.clone(),
            state: self.state.clone(),
            marker: PhantomData,
        }
    }
}

impl<C: Default, T> KafkaSink<C, T> {
    pub fn new(config: KafkaSinkConfig) -> Self {
        Self::with_codec(config, C::default())
    }
}

impl<C, T> KafkaSink<C, T> {
    pub fn with_codec(config: KafkaSinkConfig, codec: C) -> Self {
        Self {
            config,
            codec: Arc::new(codec),
            state: Arc::new(State {
                producer: Mutex::new(None),
                closed: AtomicBool::new(false),
            }),
            marker: PhantomData,
        }
    }

    /// Converts this sink into one that publishes in producer transactions,
    /// using `transactional_id` as the producer's `transactional.id`.
    ///
    /// The ID must be unique among the application's running producers and
    /// should stay the same when one instance restarts, so that the new
    /// producer fences its predecessor. Register the result with
    /// [`Subscription::transactional`](crate::Subscription::transactional)
    /// for exactly-once processing behind a `KafkaSource`.
    pub fn transactional(
        self,
        transactional_id: impl Into<String>,
    ) -> KafkaTransactionalSink<C, T> {
        KafkaTransactionalSink::new(self.config, transactional_id.into(), self.codec)
    }

    fn producer(&self) -> anyhow::Result<FutureProducer> {
        anyhow::ensure!(
            !self.state.closed.load(Ordering::Acquire),
            "Kafka sink is closed"
        );
        let mut slot = self.state.producer.lock().unwrap();
        if slot.is_none() {
            self.config.validate()?;
            let mut config = ClientConfig::new();
            for (key, value) in &self.config.properties {
                config.set(key, value);
            }
            config.set("bootstrap.servers", &self.config.brokers);
            *slot = Some(config.create()?);
        }
        Ok(slot.as_ref().unwrap().clone())
    }
}

impl<C, T> Sink<KafkaPublish<T>> for KafkaSink<C, T>
where
    C: Encoder<T>,
    T: Send + Sync + 'static,
{
    type Prepared = KafkaPrepared;

    fn prepare(&self, record: KafkaPublish<T>) -> anyhow::Result<Self::Prepared> {
        prepare(&*self.codec, record)
    }

    async fn publish(&self, output: &Self::Prepared) -> anyhow::Result<()> {
        let producer = self.producer()?;
        send(&producer, &self.config.topic, output).await
    }

    async fn close(&self) -> anyhow::Result<()> {
        self.state.closed.store(true, Ordering::Release);
        let producer = self.state.producer.lock().unwrap().take();
        if let Some(producer) = producer {
            let timeout = self.config.close_timeout;
            tokio::task::spawn_blocking(move || producer.flush(Timeout::After(timeout))).await??;
        }
        Ok(())
    }
}

pub(super) fn prepare<C: Encoder<T>, T>(
    codec: &C,
    record: KafkaPublish<T>,
) -> anyhow::Result<KafkaPrepared> {
    Ok(KafkaPrepared {
        key: record.key,
        value: record
            .value
            .as_ref()
            .map(|value| codec.encode(value))
            .transpose()?,
        headers: record.headers,
    })
}

/// Sends one prepared record and waits for its delivery report.
pub(super) async fn send(
    producer: &FutureProducer,
    topic: &str,
    output: &KafkaPrepared,
) -> anyhow::Result<()> {
    let mut headers = OwnedHeaders::new_with_capacity(output.headers.len());
    for (key, value) in &output.headers {
        headers = headers.insert(Header {
            key,
            value: value.as_deref(),
        });
    }

    let mut record = FutureRecord::<[u8], [u8]>::to(topic);
    if let Some(key) = &output.key {
        record = record.key(key);
    }
    if let Some(value) = &output.value {
        record = record.payload(value);
    }
    if !output.headers.is_empty() {
        record = record.headers(headers);
    }

    producer
        .send(record, Timeout::Never)
        .await
        .map_err(|(error, _)| error)?;
    Ok(())
}
