use super::{
    config::KafkaSinkConfig,
    progress::{KafkaConsumer, Partition, Progress, TransactionGate},
    record::KafkaPublish,
    sink::{KafkaPrepared, prepare, send},
    source::KafkaMessage,
};
use crate::{
    codec::{Decoder, Encoder},
    error::{BoxError, Context, Error, causes, ensure},
    sink::Sink,
    transaction::{TransactionEntry, TransactionalSink},
};
use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    consumer::Consumer,
    error::{KafkaError, KafkaResult},
    message::Message,
    producer::{BaseProducer, FutureProducer, Producer},
    util::Timeout,
};
use std::{
    collections::BTreeMap,
    marker::PhantomData,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

/// Commit attempts for a transaction whose commit fails with a retriable error.
const COMMIT_ATTEMPTS: usize = 3;

struct State {
    /// A transactional producer has at most one open transaction, so this lock
    /// serializes transactions from every clone of the sink.
    producer: tokio::sync::Mutex<Option<FutureProducer>>,
    closed: AtomicBool,
}

/// Publishes Kafka records in producer transactions.
///
/// Created with [`KafkaSink::transactional`](super::KafkaSink::transactional).
/// With [`Subscription::transactional`](crate::Subscription::transactional)
/// and a [`KafkaSource`](super::KafkaSource), each transaction contains a
/// delivery's outputs and its consumer offset. Used as a plain [`Sink`], each
/// publication is a transaction of its own and becomes visible to
/// `read_committed` consumers when it commits.
pub struct KafkaTransactionalSink<C, T> {
    config: Arc<Config>,
    codec: Arc<C>,
    state: Arc<State>,
    marker: PhantomData<fn(T)>,
}

/// The sink configuration and the producer's `transactional.id`.
struct Config {
    sink: KafkaSinkConfig,
    transactional_id: String,
}

impl<C, T> Clone for KafkaTransactionalSink<C, T> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            codec: self.codec.clone(),
            state: self.state.clone(),
            marker: PhantomData,
        }
    }
}

impl<C, T> KafkaTransactionalSink<C, T> {
    pub(super) fn new(sink: KafkaSinkConfig, transactional_id: String, codec: Arc<C>) -> Self {
        Self {
            config: Arc::new(Config {
                sink,
                transactional_id,
            }),
            codec,
            state: Arc::new(State {
                producer: tokio::sync::Mutex::new(None),
                closed: AtomicBool::new(false),
            }),
            marker: PhantomData,
        }
    }

    /// Runs one transaction in its own task, so dropping the caller neither
    /// leaves the transaction open nor releases the producer while it is in use.
    async fn transact(
        &self,
        outputs: Vec<KafkaPrepared>,
        offsets: Option<SourceOffsets>,
    ) -> Result<(), BoxError> {
        let config = self.config.clone();
        let state = self.state.clone();
        tokio::spawn(async move {
            let mut slot = state.producer.lock().await;
            ensure!(
                !state.closed.load(Ordering::Acquire),
                Error::closed,
                "Kafka sink is closed"
            );
            if slot.is_none() {
                *slot = Some(connect(config.clone()).await?);
            }
            if let Some(offsets) = &offsets {
                offsets.ensure_assigned()?;
            }
            let producer = slot.clone().expect("connected producer");
            let result = run(&producer, &config, &outputs, offsets).await;
            if let Err(error) = &result
                && !recover(&producer, config.sink.transaction_timeout, error).await
            {
                // A new producer with the same transactional ID fences this one
                // and aborts its unfinished transaction during initialization.
                let stale = slot.take();
                tokio::task::spawn_blocking(move || drop(stale)).await?;
            }
            result
        })
        .await?
    }

    /// Fails unless `consumer` reports the cluster ID of this sink's brokers,
    /// because a transaction commits consumer offsets through the group
    /// coordinator of the producer's cluster. The sink's cluster is read with a
    /// separate non-transactional client, so a mismatch is reported before the
    /// transactional producer initializes.
    async fn verify_cluster(&self, consumer: Arc<KafkaConsumer>) -> Result<(), BoxError> {
        let config = self.config.clone();
        tokio::task::spawn_blocking(move || {
            config.sink.validate()?;
            let timeout = config.sink.transaction_timeout;
            let mut client = ClientConfig::new();
            for (key, value) in &config.sink.properties {
                client.set(key, value);
            }
            let sink: BaseProducer = client
                .set("bootstrap.servers", &config.sink.brokers)
                .create()?;
            let sink = sink
                .client()
                .fetch_cluster_id(timeout)
                .context("Kafka sink brokers did not report their cluster ID")?;
            let source = consumer
                .client()
                .fetch_cluster_id(timeout)
                .context("Kafka source did not report its cluster ID")?;
            ensure!(
                source == sink,
                Error::config,
                "Kafka source cluster {source} differs from sink cluster {sink}; \
                 a transactional sink must connect to the source's cluster"
            );
            Ok(())
        })
        .await?
    }
}

impl<C, T> Sink<KafkaPublish<T>> for KafkaTransactionalSink<C, T>
where
    C: Encoder<T>,
    T: Send + Sync + 'static,
{
    type Prepared = KafkaPrepared;

    fn prepare(&self, record: KafkaPublish<T>) -> Result<Self::Prepared, BoxError> {
        prepare(&*self.codec, record)
    }

    async fn publish(&self, output: &Self::Prepared) -> Result<(), BoxError> {
        self.transact(vec![output.clone()], None).await
    }

    async fn close(&self) -> Result<(), BoxError> {
        self.state.closed.store(true, Ordering::Release);
        // Waits for a running transaction to finish.
        let producer = self.state.producer.lock().await.take();
        if let Some(producer) = producer {
            let timeout = self.config.sink.close_timeout;
            tokio::task::spawn_blocking(move || producer.flush(Timeout::After(timeout))).await??;
        }
        Ok(())
    }
}

impl<C, T, D, U> TransactionalSink<KafkaMessage<D, U>, KafkaPublish<T>>
    for KafkaTransactionalSink<C, T>
where
    C: Encoder<T>,
    T: Send + Sync + 'static,
    D: Decoder<U> + Send + Sync + 'static,
    U: Clone + Send + Sync + 'static,
{
    async fn verify_source(&self, delivery: &KafkaMessage<D, U>) -> Result<(), BoxError> {
        self.verify_cluster(delivery.consumer.clone()).await
    }

    async fn commit(
        &self,
        batch: &[TransactionEntry<'_, KafkaMessage<D, U>, Self::Prepared>],
    ) -> Result<(), BoxError> {
        let outputs = batch
            .iter()
            .flat_map(|entry| entry.outputs.iter().cloned())
            .collect();
        let offsets = SourceOffsets::new(batch.iter().map(|entry| entry.delivery))?;
        self.transact(outputs, offsets).await
    }
}

/// The consumer positions a transaction commits for a batch of deliveries.
struct SourceOffsets {
    consumer: Arc<KafkaConsumer>,
    progress: Arc<Mutex<Progress>>,
    transactions: TransactionGate,
    /// The assignment generation of every delivery in the batch.
    deliveries: Vec<(u64, Partition)>,
    /// The offset after the last delivery of each partition in the batch.
    next: BTreeMap<Partition, (u64, i64)>,
}

impl SourceOffsets {
    /// Returns `None` for an empty batch. Fails when the deliveries come from
    /// different consumers, because a transaction commits offsets with one
    /// consumer's group metadata here.
    fn new<'a, D: 'a, U: 'a>(
        deliveries: impl IntoIterator<Item = &'a KafkaMessage<D, U>>,
    ) -> Result<Option<Self>, BoxError> {
        let mut offsets: Option<Self> = None;
        for delivery in deliveries {
            let offsets = offsets.get_or_insert_with(|| Self {
                consumer: delivery.consumer.clone(),
                progress: delivery.progress.clone(),
                transactions: delivery.transactions.clone(),
                deliveries: Vec::new(),
                next: BTreeMap::new(),
            });
            ensure!(
                Arc::ptr_eq(&offsets.consumer, &delivery.consumer),
                Error::msg,
                "a Kafka transaction batch contains deliveries of different consumers"
            );
            let partition = (delivery.raw.topic().to_owned(), delivery.raw.partition());
            let next = (delivery.generation, delivery.raw.offset() + 1);
            offsets
                .next
                .entry(partition.clone())
                .and_modify(|current| *current = (*current).max(next))
                .or_insert(next);
            offsets.deliveries.push((delivery.generation, partition));
        }
        Ok(offsets)
    }

    /// Fails when the partition assignment of any delivery has ended.
    fn ensure_assigned(&self) -> Result<(), BoxError> {
        if self.consumer.assignment_lost() {
            self.progress.lock().unwrap().revoke_all();
            return Err(Error::msg("Kafka assignment was lost").into());
        }
        let progress = self.progress.lock().unwrap();
        for (generation, partition) in &self.deliveries {
            ensure!(
                progress.is_current(*generation, partition),
                Error::msg,
                "Kafka delivery belongs to a revoked assignment"
            );
        }
        Ok(())
    }

    /// Adds the offset after the batch's last delivery of each partition to the
    /// open transaction and commits it while holding the transaction gate, so a
    /// revoke waits until the commit finishes and later commits see the
    /// revocation.
    fn commit(&self, producer: &FutureProducer, timeout: Duration) -> Result<(), BoxError> {
        let _transactions = self.transactions.lock().unwrap();
        self.ensure_assigned()?;
        let metadata = self
            .consumer
            .group_metadata()
            .context("Kafka consumer has no group metadata")?;
        let mut offsets = TopicPartitionList::new();
        for ((topic, partition), (_, next)) in &self.next {
            offsets.add_partition_offset(topic, *partition, Offset::Offset(*next))?;
        }
        producer.send_offsets_to_transaction(&offsets, &metadata, timeout)?;
        commit(producer, timeout)?;
        let mut progress = self.progress.lock().unwrap();
        for (partition, (generation, next)) in &self.next {
            progress.committed(*generation, partition, *next);
        }
        Ok(())
    }
}

async fn connect(config: Arc<Config>) -> Result<FutureProducer, BoxError> {
    config.sink.validate()?;
    ensure!(
        !config.transactional_id.trim().is_empty(),
        Error::config,
        "Kafka transactional ID is required"
    );
    tokio::task::spawn_blocking(move || {
        let mut client = ClientConfig::new();
        for (key, value) in &config.sink.properties {
            client.set(key, value);
        }
        client
            .set("bootstrap.servers", &config.sink.brokers)
            .set("transactional.id", &config.transactional_id);
        let producer: FutureProducer = client.create()?;
        producer.init_transactions(config.sink.transaction_timeout)?;
        Ok::<_, BoxError>(producer)
    })
    .await?
}

async fn run(
    producer: &FutureProducer,
    config: &Config,
    outputs: &[KafkaPrepared],
    offsets: Option<SourceOffsets>,
) -> Result<(), BoxError> {
    producer.begin_transaction()?;
    for output in outputs {
        send(producer, &config.sink.topic, output).await?;
    }
    let producer = producer.clone();
    let timeout = config.sink.transaction_timeout;
    tokio::task::spawn_blocking(move || match offsets {
        Some(offsets) => offsets.commit(&producer, timeout),
        None => Ok(commit(&producer, timeout)?),
    })
    .await?
}

fn commit(producer: &FutureProducer, timeout: Duration) -> KafkaResult<()> {
    let mut attempt = 1;
    loop {
        match producer.commit_transaction(timeout) {
            Err(KafkaError::Transaction(error))
                if error.is_retriable() && attempt < COMMIT_ATTEMPTS =>
            {
                attempt += 1;
            }
            result => return result,
        }
    }
}

/// Aborts the failed transaction. Returns `false` when the producer cannot
/// continue and must be replaced.
async fn recover(producer: &FutureProducer, timeout: Duration, error: &BoxError) -> bool {
    let fatal = causes(error.as_ref())
        .filter_map(|cause| cause.downcast_ref::<KafkaError>())
        .any(|error| matches!(error, KafkaError::Transaction(error) if error.is_fatal()));
    if fatal {
        return false;
    }
    let producer = producer.clone();
    tokio::task::spawn_blocking(move || producer.abort_transaction(timeout))
        .await
        .is_ok_and(|result| result.is_ok())
}
