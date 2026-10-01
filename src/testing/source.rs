use super::{deliveries::Deliveries, record::TestRecord};
use crate::{
    codec::Decoder,
    message::{OrderingKey, SourceMessage},
    source::{Receive, ReceiveError, Source},
};
use std::{collections::VecDeque, marker::PhantomData, sync::Arc};

#[cfg(feature = "kafka")]
use crate::adapters::kafka::KafkaRecord;
#[cfg(feature = "pulsar")]
use crate::adapters::pulsar::PulsarRecord;
#[cfg(feature = "rabbitmq")]
use crate::adapters::rabbitmq::RabbitMqRecord;

/// A finite source of fabricated records that decodes them with a codec.
///
/// Each record becomes one delivery whose handler input is the record decoded
/// by `C`, whose raw form is the record itself, and whose ordering key and
/// trace-context fields follow the adapter the record type belongs to. The
/// source ends after the last record. [`deliveries`](Self::deliveries) reports
/// which records were acknowledged.
///
/// ```
/// use beavers::{
///     App, DeadLetter, ErrorPolicy, FailureKind, HandlerError, InMemorySink, Json, Result,
///     Subscription, testing::TestSource,
/// };
///
/// async fn halve(value: u32) -> Result<u32> {
///     if value % 2 == 1 {
///         return Err(HandlerError::Reject(anyhow::anyhow!("odd value")));
///     }
///     Ok(value / 2)
/// }
///
/// # #[tokio::main] async fn main() -> anyhow::Result<()> {
/// let source = TestSource::<Json, u32, Vec<u8>>::new([b"4".to_vec(), b"3".to_vec(), b"x".to_vec()]);
/// let deliveries = source.deliveries();
/// let sink = InMemorySink::default();
/// let dlq = InMemorySink::<DeadLetter<u32, Vec<u8>>>::default();
/// App::new()
///     .subscription(
///         Subscription::new("halve", source, sink.clone(), halve)
///             .dlq(dlq.clone())
///             .error_policy(ErrorPolicy::dead_letter_all()),
///     )
///     .run()
///     .await?;
///
/// assert_eq!(sink.values(), [2]);
/// let failures: Vec<_> = dlq.values().into_iter().map(|dead| (dead.failure, dead.raw)).collect();
/// assert_eq!(
///     failures,
///     [(FailureKind::Rejected, b"3".to_vec()), (FailureKind::Decode, b"x".to_vec())]
/// );
/// assert!(deliveries.all_acknowledged());
/// # Ok(()) }
/// ```
pub struct TestSource<C, T, R> {
    records: VecDeque<(usize, R)>,
    codec: Arc<C>,
    deliveries: Deliveries<R>,
    marker: PhantomData<fn() -> T>,
}

/// A [`TestSource`] of Kafka records, delivering `KafkaRecord<T>` inputs.
#[cfg(feature = "kafka")]
pub type KafkaTestSource<C, T> = TestSource<C, T, KafkaRecord<Vec<u8>>>;

/// A [`TestSource`] of Pulsar messages, delivering `PulsarRecord<T>` inputs.
#[cfg(feature = "pulsar")]
pub type PulsarTestSource<C, T> = TestSource<C, T, PulsarRecord<Vec<u8>>>;

/// A [`TestSource`] of RabbitMQ messages, delivering `RabbitMqRecord<T>` inputs.
#[cfg(feature = "rabbitmq")]
pub type RabbitMqTestSource<C, T> = TestSource<C, T, RabbitMqRecord<Vec<u8>>>;

impl<C: Default, T, R: TestRecord> TestSource<C, T, R> {
    pub fn new(records: impl IntoIterator<Item = R>) -> Self {
        Self::with_codec(records, C::default())
    }
}

impl<C, T, R: TestRecord> TestSource<C, T, R> {
    pub fn with_codec(records: impl IntoIterator<Item = R>, codec: C) -> Self {
        let records: Vec<R> = records.into_iter().collect();
        Self {
            deliveries: Deliveries::new(records.clone()),
            records: records.into_iter().enumerate().collect(),
            codec: Arc::new(codec),
            marker: PhantomData,
        }
    }

    /// A shared view of each record's delivery state.
    pub fn deliveries(&self) -> Deliveries<R> {
        self.deliveries.clone()
    }
}

impl<C, T, R> Source for TestSource<C, T, R>
where
    C: Decoder<T>,
    T: Clone + Send + Sync + 'static,
    R: TestRecord,
{
    type Message = TestMessage<C, T, R>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        Ok(match self.records.pop_front() {
            Some((index, record)) => {
                self.deliveries.receive(index);
                Receive::Message(TestMessage {
                    record,
                    index,
                    codec: self.codec.clone(),
                    deliveries: self.deliveries.clone(),
                    marker: PhantomData,
                })
            }
            None => Receive::End,
        })
    }
}

/// A delivery of [`TestSource`]. Acknowledging it records the acknowledgement.
pub struct TestMessage<C, T, R> {
    record: R,
    index: usize,
    codec: Arc<C>,
    deliveries: Deliveries<R>,
    marker: PhantomData<fn() -> T>,
}

impl<C, T, R> SourceMessage for TestMessage<C, T, R>
where
    C: Decoder<T>,
    T: Clone + Send + Sync + 'static,
    R: TestRecord,
{
    type Item = R::Decoded<T>;
    type Raw = R;

    fn decode(&self) -> anyhow::Result<Self::Item> {
        self.record.decode(&*self.codec)
    }

    fn raw(&self) -> R {
        self.record.clone()
    }

    async fn ack(self) -> anyhow::Result<()> {
        self.deliveries.acknowledge(self.index);
        Ok(())
    }

    fn ordering_key(&self) -> Option<OrderingKey> {
        self.record.ordering_key()
    }

    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        self.record.propagation_fields()
    }
}
