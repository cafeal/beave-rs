//! Distributions of Kafka deliveries over a subscription's concurrency.
use super::source::KafkaMessage;
use crate::{
    codec::Decoder,
    distribution::Distribution,
    message::{OrderingKey, SourceMessage},
};

/// Which Kafka deliveries a subscription processes one at a time.
///
/// Kafka keeps the records of one partition in order, and a producer's
/// default partitioner sends the records of one key to one partition, so the
/// records of one key are ordered as well.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum KafkaDistribution {
    /// The records of one topic partition, one at a time in offset order.
    /// The same as the default [`SourceOrder`](crate::SourceOrder).
    #[default]
    Partition,
    /// The records of one key within a topic partition, one at a time in
    /// offset order. Records with different keys in one partition run in
    /// parallel, and records with a null key are processed one at a time per
    /// partition. Offsets are still committed only up to the first
    /// unfinished record of a partition.
    MessageKey,
}

impl<C: Decoder<Value: Clone + Send + Sync + 'static>> Distribution<KafkaMessage<C>>
    for KafkaDistribution
{
    type Key = OrderingKey;

    fn key(
        &self,
        message: &KafkaMessage<C>,
        _: &<KafkaMessage<C> as SourceMessage>::Item,
    ) -> Option<OrderingKey> {
        let partition = message.ordering_key()?;
        Some(match (self, message.message_key()) {
            (Self::MessageKey, Some(key)) => partition.with_key(key),
            _ => partition,
        })
    }

    fn keeps_source_order(&self) -> bool {
        matches!(self, Self::Partition)
    }
}
