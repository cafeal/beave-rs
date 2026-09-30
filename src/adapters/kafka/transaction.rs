use super::{
    config::KafkaSinkConfig,
    progress::{KafkaConsumer, Partition, Progress, TransactionGate},
    record::KafkaPublish,
    sink::{KafkaPrepared, prepare, send},
    source::KafkaMessage,
};
use crate::{
    codec::{Decoder, Encoder},
    sink::Sink,
    transaction::TransactionalSink,
};
use anyhow::Context as _;
use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    consumer::Consumer,
    error::{KafkaError, KafkaResult},
    message::Message,
    producer::{BaseProducer, FutureProducer, Producer},
    util::Timeout,
};
use std::{
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
        offset: Option<SourceOffset>,
    ) -> anyhow::Result<()> {
        let config = self.config.clone();
        let state = self.state.clone();
        tokio::spawn(async move {
            let mut slot = state.producer.lock().await;
            anyhow::ensure!(
                !state.closed.load(Ordering::Acquire),
                "Kafka sink is closed"
            );
            if slot.is_none() {
                *slot = Some(connect(config.clone()).await?);
            }
            if let Some(offset) = &offset {
                offset.ensure_assigned()?;
            }
            let producer = slot.clone().expect("connected producer");
            let result = run(&producer, &config, &outputs, offset).await;
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
    async fn verify_cluster(&self, consumer: Arc<KafkaConsumer>) -> anyhow::Result<()> {
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
            anyhow::ensure!(
                source == sink,
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

    fn prepare(&self, record: KafkaPublish<T>) -> anyhow::Result<Self::Prepared> {
        prepare(&*self.codec, record)
    }

    async fn publish(&self, output: &Self::Prepared) -> anyhow::Result<()> {
        self.transact(vec![output.clone()], None).await
    }

    async fn close(&self) -> anyhow::Result<()> {
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
    async fn verify_source(&self, delivery: &KafkaMessage<D, U>) -> anyhow::Result<()> {
        self.verify_cluster(delivery.consumer.clone()).await
    }

    async fn commit(
        &self,
        delivery: &KafkaMessage<D, U>,
        outputs: &[Self::Prepared],
    ) -> anyhow::Result<()> {
        self.transact(outputs.to_vec(), Some(SourceOffset::new(delivery)))
            .await
    }
}

/// The consumer position a transaction commits for one delivery.
struct SourceOffset {
    consumer: Arc<KafkaConsumer>,
    progress: Arc<Mutex<Progress>>,
    transactions: TransactionGate,
    generation: u64,
    partition: Partition,
    offset: i64,
}

impl SourceOffset {
    fn new<D, U>(delivery: &KafkaMessage<D, U>) -> Self {
        Self {
            consumer: delivery.consumer.clone(),
            progress: delivery.progress.clone(),
            transactions: delivery.transactions.clone(),
            generation: delivery.generation,
            partition: (delivery.raw.topic().to_owned(), delivery.raw.partition()),
            offset: delivery.raw.offset(),
        }
    }

    /// Fails when the delivery's partition assignment has ended.
    fn ensure_assigned(&self) -> anyhow::Result<()> {
        if self.consumer.assignment_lost() {
            self.progress.lock().unwrap().revoke_all();
            anyhow::bail!("Kafka assignment was lost");
        }
        anyhow::ensure!(
            self.progress
                .lock()
                .unwrap()
                .is_current(self.generation, &self.partition),
            "Kafka delivery belongs to a revoked assignment"
        );
        Ok(())
    }

    /// Adds the offset after this delivery to the open transaction and commits
    /// it while holding the transaction gate, so a revoke of the partition waits
    /// until the commit finishes and later commits see the revocation.
    fn commit(&self, producer: &FutureProducer, timeout: Duration) -> anyhow::Result<()> {
        let _transactions = self.transactions.lock().unwrap();
        self.ensure_assigned()?;
        let metadata = self
            .consumer
            .group_metadata()
            .context("Kafka consumer has no group metadata")?;
        let mut offsets = TopicPartitionList::new();
        offsets.add_partition_offset(
            &self.partition.0,
            self.partition.1,
            Offset::Offset(self.offset + 1),
        )?;
        producer.send_offsets_to_transaction(&offsets, &metadata, timeout)?;
        commit(producer, timeout)?;
        self.progress
            .lock()
            .unwrap()
            .committed(self.generation, &self.partition, self.offset + 1);
        Ok(())
    }
}

async fn connect(config: Arc<Config>) -> anyhow::Result<FutureProducer> {
    config.sink.validate()?;
    anyhow::ensure!(
        !config.transactional_id.trim().is_empty(),
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
        anyhow::Ok(producer)
    })
    .await?
}

async fn run(
    producer: &FutureProducer,
    config: &Config,
    outputs: &[KafkaPrepared],
    offset: Option<SourceOffset>,
) -> anyhow::Result<()> {
    producer.begin_transaction()?;
    for output in outputs {
        send(producer, &config.sink.topic, output).await?;
    }
    let producer = producer.clone();
    let timeout = config.sink.transaction_timeout;
    tokio::task::spawn_blocking(move || match offset {
        Some(offset) => offset.commit(&producer, timeout),
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
async fn recover(producer: &FutureProducer, timeout: Duration, error: &anyhow::Error) -> bool {
    let fatal = error
        .chain()
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
