//! Distributions of Service Bus deliveries over a subscription's concurrency.
use super::source::ServiceBusMessage;
use crate::{
    codec::Decoder,
    distribution::Distribution,
    message::{OrderingKey, SourceMessage},
};

/// Which Service Bus deliveries a subscription processes one at a time.
///
/// Under the default [`SourceOrder`](crate::SourceOrder), every delivery runs
/// independently, because the source receives without sessions and Service
/// Bus then delivers in no guaranteed order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ServiceBusDistribution {
    /// The deliveries of one session ID, one at a time in the order this
    /// source received them. Deliveries without a session ID run
    /// independently. Service Bus does not order a session's messages for a
    /// receiver without sessions, and may hand them to several receivers, so
    /// this orders only what one process receives.
    Session,
}

impl<C: Decoder<Value: Clone + Send + Sync + 'static>> Distribution<ServiceBusMessage<C>>
    for ServiceBusDistribution
{
    type Key = OrderingKey;

    fn key(
        &self,
        message: &ServiceBusMessage<C>,
        _: &<ServiceBusMessage<C> as SourceMessage>::Item,
    ) -> Option<OrderingKey> {
        let session = message.message_key()?;
        Some(OrderingKey::new(message.entity_path.as_str(), 0).with_key(session))
    }
}
