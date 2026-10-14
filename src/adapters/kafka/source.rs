use super::{
    client::Kafka,
    config::SourceConfig,
    probe::{KafkaProbe, STATISTICS_INTERVAL_MS},
    progress::{Context, KafkaConsumer, Progress, TransactionGate},
    record::{KafkaMetadata, KafkaRecord, text_headers},
};
use crate::{
    codec::Decoder,
    error::{BoxError, Error},
    message::{MessagingAttributes, OrderingKey, SourceMessage},
    shutdown::CancellationToken,
    source::{Receive, ReceiveError, Source, SourceProbe},
};
use rdkafka::{
    Offset, TopicPartitionList,
    consumer::{CommitMode, Consumer},
    message::{Headers, Message, OwnedMessage},
};
use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
};

/// A Kafka source with a consumer of its own.
///
/// Built with [`Kafka::source`]. It creates its consumer and subscribes on the
/// first receive, which also validates the settings.
pub struct KafkaSource<C> {
    kafka: Kafka,
    config: SourceConfig,
    codec: Arc<C>,
    consumer: Option<Arc<KafkaConsumer>>,
    progress: Arc<Mutex<Progress>>,
    /// Serializes acknowledgement commits, while the progress mutex remains
    /// unlocked during librdkafka's blocking synchronous commit.
    commit_gate: Arc<tokio::sync::Mutex<()>>,
    transactions: TransactionGate,
    probe: Arc<KafkaProbe>,
    group_id: Arc<str>,
    closed: bool,
}

impl<C> KafkaSource<C> {
    pub(super) fn new(kafka: Kafka, config: SourceConfig, codec: C) -> Self {
        Self {
            kafka,
            group_id: config.group_id.as_str().into(),
            config,
            codec: Arc::new(codec),
            consumer: None,
            progress: Arc::default(),
            commit_gate: Arc::default(),
            transactions: Arc::default(),
            probe: Arc::default(),
            closed: false,
        }
    }

    /// Sets a librdkafka property on this source's consumer, after the
    /// handle's properties, such as `auto.offset.reset`. Automatic offset
    /// storage and commits are always disabled; the source commits
    /// acknowledged offsets.
    pub fn property(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.properties.insert(name.into(), value.into());
        self
    }

    /// Checks the handle and source settings without network access.
    ///
    /// Fails when the handle is invalid, the group ID or topic list is empty, a
    /// topic name is blank, or a property has an empty name or a value
    /// containing a NUL byte.
    pub fn validate(&self) -> Result<(), Error> {
        self.kafka.validate()?;
        self.config.validate()
    }

    fn connect(&mut self) -> Result<(), BoxError> {
        self.validate()?;
        let mut config = self.kafka.client_config();
        for (key, value) in &self.config.properties {
            config.set(key, value);
        }
        // The health probe reads lag and connection state from statistics.
        if config.get("statistics.interval.ms").is_none() {
            config.set("statistics.interval.ms", STATISTICS_INTERVAL_MS);
        }
        // The adapter owns offset progress, regardless of user properties.
        config
            .set("group.id", &self.config.group_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false");
        let consumer: KafkaConsumer = config.create_with_context(Context {
            progress: self.progress.clone(),
            transactions: self.transactions.clone(),
            probe: self.probe.clone(),
        })?;
        consumer.subscribe(
            &self
                .config
                .topics
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
        )?;
        self.consumer = Some(Arc::new(consumer));
        Ok(())
    }
}

impl<C: Decoder<Value: Clone + Send + Sync + 'static>> Source for KafkaSource<C> {
    type Message = KafkaMessage<C>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        if self.closed {
            return Ok(Receive::End);
        }
        if self.consumer.is_none() {
            self.connect().map_err(ReceiveError::Fatal)?;
        }
        let consumer = self.consumer.as_ref().expect("connected consumer");
        loop {
            // rdkafka 0.39 documents StreamConsumer::recv as cancellation-safe.
            let raw = consumer
                .recv()
                .await
                .map_err(|error| ReceiveError::Retry(error.into()))?
                .detach();
            let key = (raw.topic().to_owned(), raw.partition());
            // A record fetched before its partition was revoked belongs to the
            // next owner; skip it without processing or committing.
            let Some((generation, revoked)) =
                self.progress.lock().unwrap().register(&key, raw.offset())
            else {
                continue;
            };
            return Ok(Receive::Message(KafkaMessage {
                raw,
                codec: self.codec.clone(),
                consumer: consumer.clone(),
                progress: self.progress.clone(),
                commit_gate: self.commit_gate.clone(),
                transactions: self.transactions.clone(),
                group_id: self.group_id.clone(),
                generation,
                revoked,
            }));
        }
    }

    async fn close(&mut self) -> Result<(), BoxError> {
        self.closed = true;
        {
            let _transactions = self.transactions.lock().unwrap();
            self.progress.lock().unwrap().revoke_all();
        }
        if let Some(consumer) = self.consumer.take() {
            consumer.unsubscribe();
        }
        self.probe.clear();
        Ok(())
    }

    fn probe(&self) -> Option<Arc<dyn SourceProbe>> {
        Some(self.probe.clone())
    }
}

/// One Kafka delivery. Dropping it leaves its Kafka offset uncommitted.
pub struct KafkaMessage<C> {
    pub(super) raw: OwnedMessage,
    codec: Arc<C>,
    pub(super) consumer: Arc<KafkaConsumer>,
    pub(super) progress: Arc<Mutex<Progress>>,
    commit_gate: Arc<tokio::sync::Mutex<()>>,
    pub(super) transactions: TransactionGate,
    pub(super) generation: u64,
    group_id: Arc<str>,
    revoked: CancellationToken,
}

impl<C> KafkaMessage<C> {
    fn record<V, E>(&self, value: impl FnOnce(&[u8]) -> Result<V, E>) -> Result<KafkaRecord<V>, E> {
        Ok(KafkaRecord {
            key: self.raw.key().map(<[u8]>::to_vec),
            value: self.raw.payload().map(value).transpose()?,
            headers: self
                .raw
                .headers()
                .map(|headers| {
                    headers
                        .iter()
                        .map(|header| (header.key.to_owned(), header.value.map(<[u8]>::to_vec)))
                        .collect()
                })
                .unwrap_or_default(),
            metadata: self.metadata(),
        })
    }

    fn metadata(&self) -> KafkaMetadata {
        KafkaMetadata {
            topic: self.raw.topic().to_owned(),
            partition: self.raw.partition(),
            offset: self.raw.offset(),
            timestamp: self.raw.timestamp().to_millis(),
        }
    }
}

impl<C: Decoder<Value: Clone + Send + Sync + 'static>> SourceMessage for KafkaMessage<C> {
    type Item = KafkaRecord<C::Value>;
    type Raw = KafkaRecord<Vec<u8>>;

    fn decode(&self) -> Result<Self::Item, BoxError> {
        self.record(|bytes| self.codec.decode(bytes))
    }

    fn raw(&self) -> Self::Raw {
        let Ok(record) = self.record(|bytes| Ok::<_, Infallible>(bytes.to_vec()));
        record
    }

    async fn ack(self) -> Result<(), BoxError> {
        let _commit = self.commit_gate.lock().await;
        let key = (self.raw.topic().to_owned(), self.raw.partition());
        if self.consumer.assignment_lost() {
            self.progress.lock().unwrap().revoke_all();
            return Err(Error::msg("Kafka assignment was lost").into());
        }
        let next =
            self.progress
                .lock()
                .unwrap()
                .complete(self.generation, &key, self.raw.offset())?;
        let Some(next) = next else {
            return Ok(());
        };

        let consumer = self.consumer.clone();
        let commit_key = key.clone();
        tokio::task::spawn_blocking(move || {
            let mut offsets = TopicPartitionList::new();
            offsets.add_partition_offset(&commit_key.0, commit_key.1, Offset::Offset(next))?;
            consumer.commit(&offsets, CommitMode::Sync)?;
            Ok::<_, BoxError>(())
        })
        .await??;

        // A revoke can occur while commit is in flight. Never let a result for
        // an old assignment alter progress in a newer assignment.
        self.progress
            .lock()
            .unwrap()
            .committed(self.generation, &key, next);
        Ok(())
    }

    fn ordering_key(&self) -> Option<OrderingKey> {
        Some(OrderingKey::new(
            self.raw.topic(),
            i64::from(self.raw.partition()),
        ))
    }

    fn revocation(&self) -> Option<CancellationToken> {
        Some(self.revoked.clone())
    }

    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        self.raw
            .headers()
            .map(|headers| text_headers(headers.iter().map(|header| (header.key, header.value))))
            .unwrap_or_default()
    }

    fn messaging_attributes(&self) -> Option<MessagingAttributes<'_>> {
        Some(
            MessagingAttributes::new("kafka")
                .destination(self.raw.topic())
                .partition(i64::from(self.raw.partition()))
                .kafka_offset(self.raw.offset())
                .consumer_group(&self.group_id),
        )
    }
}
