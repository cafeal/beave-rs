use super::{
    config::KafkaSourceConfig,
    progress::{Context, KafkaConsumer, Progress},
    record::{KafkaMetadata, KafkaRecord},
};
use crate::{
    codec::Decoder,
    message::SourceMessage,
    source::{Receive, ReceiveError, Source},
};
use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    consumer::{CommitMode, Consumer},
    message::{Headers, Message, OwnedMessage},
};
use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

/// A Kafka source. It creates its consumer and subscribes on the first receive.
pub struct KafkaSource<C, T> {
    config: KafkaSourceConfig,
    codec: Arc<C>,
    consumer: Option<Arc<KafkaConsumer>>,
    progress: Arc<Mutex<Progress>>,
    /// Serializes acknowledgement commits, while the progress mutex remains
    /// unlocked during librdkafka's blocking synchronous commit.
    commit_gate: Arc<tokio::sync::Mutex<()>>,
    closed: bool,
    marker: PhantomData<T>,
}

impl<C: Default, T> KafkaSource<C, T> {
    pub fn new(config: KafkaSourceConfig) -> Self {
        Self::with_codec(config, C::default())
    }
}

impl<C, T> KafkaSource<C, T> {
    pub fn with_codec(config: KafkaSourceConfig, codec: C) -> Self {
        Self {
            config,
            codec: Arc::new(codec),
            consumer: None,
            progress: Arc::default(),
            commit_gate: Arc::default(),
            closed: false,
            marker: PhantomData,
        }
    }

    fn connect(&mut self) -> anyhow::Result<()> {
        self.config.validate()?;
        let mut config = ClientConfig::new();
        for (key, value) in &self.config.properties {
            config.set(key, value);
        }
        // The adapter owns offset progress, regardless of user properties.
        config
            .set("bootstrap.servers", &self.config.brokers)
            .set("group.id", &self.config.group_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false");
        let consumer: KafkaConsumer = config.create_with_context(Context(self.progress.clone()))?;
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

impl<C: Decoder<T>, T: Clone + Send + Sync + 'static> Source for KafkaSource<C, T> {
    type Message = KafkaMessage<C, T>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        if self.closed {
            return Ok(Receive::End);
        }
        if self.consumer.is_none() {
            self.connect().map_err(ReceiveError::Fatal)?;
        }
        let consumer = self.consumer.as_ref().expect("connected consumer");
        // rdkafka 0.39 documents StreamConsumer::recv as cancellation-safe.
        let raw = consumer
            .recv()
            .await
            .map_err(|error| ReceiveError::Retry(error.into()))?
            .detach();
        let key = (raw.topic().to_owned(), raw.partition());
        let generation = self.progress.lock().unwrap().register(key, raw.offset());
        Ok(Receive::Message(KafkaMessage {
            raw,
            codec: self.codec.clone(),
            consumer: consumer.clone(),
            progress: self.progress.clone(),
            commit_gate: self.commit_gate.clone(),
            generation,
            marker: PhantomData,
        }))
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.closed = true;
        self.progress.lock().unwrap().revoke_all();
        if let Some(consumer) = self.consumer.take() {
            consumer.unsubscribe();
        }
        Ok(())
    }
}

/// One Kafka delivery. Dropping it leaves its Kafka offset uncommitted.
pub struct KafkaMessage<C, T> {
    raw: OwnedMessage,
    codec: Arc<C>,
    consumer: Arc<KafkaConsumer>,
    progress: Arc<Mutex<Progress>>,
    commit_gate: Arc<tokio::sync::Mutex<()>>,
    generation: u64,
    marker: PhantomData<T>,
}

impl<C, T> KafkaMessage<C, T> {
    fn metadata(&self) -> KafkaMetadata {
        KafkaMetadata {
            topic: self.raw.topic().to_owned(),
            partition: self.raw.partition(),
            offset: self.raw.offset(),
            timestamp: self.raw.timestamp().to_millis(),
        }
    }
}

impl<C: Decoder<T>, T: Clone + Send + Sync + 'static> SourceMessage for KafkaMessage<C, T> {
    type Item = KafkaRecord<T>;

    fn decode(&self) -> anyhow::Result<Self::Item> {
        Ok(KafkaRecord {
            key: self.raw.key().map(<[u8]>::to_vec),
            value: self
                .raw
                .payload()
                .map(|bytes| self.codec.decode(bytes))
                .transpose()?,
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

    async fn ack(self) -> anyhow::Result<()> {
        let _commit = self.commit_gate.lock().await;
        let key = (self.raw.topic().to_owned(), self.raw.partition());
        if self.consumer.assignment_lost() {
            self.progress.lock().unwrap().revoke_all();
            anyhow::bail!("Kafka assignment was lost");
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
            anyhow::Ok(())
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
}
