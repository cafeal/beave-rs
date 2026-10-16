//! A delivery received through a channel.
use super::record::ChannelRaw;
use crate::{
    error::BoxError,
    message::{Delivery, KeyedMessage, OrderingKey, Position, SourceMessage},
    shutdown::CancellationToken,
};
use std::sync::Arc;

/// A delivery of a [`ChannelSource`](crate::ChannelSource).
///
/// It carries the message key of the upstream delivery that produced its
/// value, so a [stateful](crate::state) handler downstream keeps its state per
/// the upstream key, such as the Kafka record key. Values sent by application
/// code have no message key. The upstream position is not carried: one
/// upstream delivery can produce several values, which would share it.
pub struct ChannelMessage<T> {
    pub(super) delivery: Delivery<T, ChannelRaw>,
    pub(super) message_key: Option<Arc<[u8]>>,
}

impl<T: Clone + Send + Sync + 'static> SourceMessage for ChannelMessage<T> {
    type Item = T;
    type Raw = ChannelRaw;

    fn decode(&self) -> Result<T, BoxError> {
        self.delivery.decode()
    }

    fn raw(&self) -> ChannelRaw {
        self.delivery.raw()
    }

    async fn ack(self) -> Result<(), BoxError> {
        self.delivery.ack().await
    }

    fn ordering_key(&self) -> Option<OrderingKey> {
        self.delivery.ordering_key()
    }

    fn message_key(&self) -> Option<&[u8]> {
        self.message_key.as_deref()
    }

    fn position(&self) -> Option<Position> {
        None
    }

    fn revocation(&self) -> Option<CancellationToken> {
        self.delivery.revocation()
    }

    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        self.delivery.propagation_fields()
    }
}

impl<T: Clone + Send + Sync + 'static> KeyedMessage for ChannelMessage<T> {}
