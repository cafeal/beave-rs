//! Distributions of Pulsar deliveries over a subscription's concurrency.
use super::source::PulsarMessage;
use crate::{
    codec::Decoder,
    distribution::Distribution,
    message::{OrderingKey, SourceMessage},
};

/// Which Pulsar deliveries a subscription processes one at a time.
///
/// The default [`SourceOrder`](crate::SourceOrder) follows the
/// [subscription type](super::PulsarSubscriptionType): one partition at a time
/// for Exclusive and Failover, one key at a time for Key_Shared, and every
/// delivery independently for Shared. These choices apply the same way under
/// any subscription type, so a Shared subscription can still keep the
/// deliveries it receives of one key in order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum PulsarDistribution {
    /// The deliveries of one topic partition, one at a time in receive
    /// order. The same as the default for Exclusive and Failover
    /// subscriptions.
    #[default]
    Partition,
    /// The deliveries of one key within a topic partition, one at a time in
    /// receive order: the ordering key, or the message key when no ordering
    /// key is set, which is the key Key_Shared routes by. Deliveries with
    /// different keys in one partition run in parallel, and deliveries with
    /// neither key are processed one at a time per partition.
    MessageKey,
}

impl<C: Decoder<Value: Clone + Send + Sync + 'static>> Distribution<PulsarMessage<C>>
    for PulsarDistribution
{
    type Key = OrderingKey;

    fn key(
        &self,
        message: &PulsarMessage<C>,
        _: &<PulsarMessage<C> as SourceMessage>::Item,
    ) -> Option<OrderingKey> {
        match self {
            Self::Partition => Some(message.partition.clone()),
            Self::MessageKey => Some(
                message
                    .keyed_partition
                    .clone()
                    .unwrap_or_else(|| message.partition.clone()),
            ),
        }
    }

    fn keeps_source_order(&self) -> bool {
        matches!(self, Self::Partition)
    }
}
