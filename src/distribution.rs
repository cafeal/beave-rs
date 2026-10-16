//! Which deliveries of a subscription are processed one at a time.
use crate::message::{OrderingKey, SourceMessage};
use std::hash::Hash;

/// Decides which deliveries of a subscription must not be processed at the
/// same time.
///
/// The subscription decodes deliveries in parallel and passes them to the
/// distribution in the order they were received. Deliveries for which
/// [`key`](Self::key) returns equal keys are processed one at a time, in that
/// order. Deliveries with different keys, or without a key, run in parallel up
/// to the subscription's [`concurrency`](crate::Subscription::concurrency).
/// A delivery that fails to decode has no key; it is routed by the
/// [`ErrorPolicy`](crate::ErrorPolicy) as soon as it is distributed.
///
/// [`SourceOrder`], the default, follows the order in which the source
/// delivers, such as a Kafka partition. [`Parallel`] processes every delivery
/// independently. Adapters can offer their own distributions, such as
/// [`KafkaDistribution`](crate::adapters::kafka::KafkaDistribution), and a
/// closure that takes the decoded value and returns a key distributes by a
/// value of the payload:
///
/// ```
/// use beavers::{InMemorySink, IterSource, Subscription};
///
/// #[derive(Clone)]
/// struct Order {
///     customer: u64,
///     amount: u64,
/// }
///
/// let orders = IterSource::new([Order { customer: 1, amount: 10 }]);
/// let subscription = Subscription::new(
///     "orders",
///     orders,
///     InMemorySink::<u64>::default(),
///     |order: Order| async move { Ok(order.amount) },
/// )
/// .concurrency(16)
/// .distribution(|order: &Order| order.customer);
/// # let _ = subscription;
/// ```
///
/// A key taken from the payload orders deliveries within one process only.
/// The broker does not read the payload, so it may deliver one key's messages
/// to several consumers.
pub trait Distribution<M: SourceMessage>: Send + Sync + 'static {
    /// Identifies deliveries that are processed one at a time.
    type Key: Hash;

    /// The key of a decoded delivery, or `None` when it may run in parallel
    /// with every other delivery.
    fn key(&self, message: &M, value: &M::Item) -> Option<Self::Key>;

    /// Whether this distribution processes the deliveries of each
    /// [ordering scope](SourceMessage::ordering_key) one at a time in receive
    /// order. A [transactional](crate::Subscription::transactional)
    /// subscription requires it. Defaults to `false`.
    fn keeps_source_order(&self) -> bool {
        false
    }
}

/// Processes the deliveries of each [ordering scope](SourceMessage::ordering_key)
/// one at a time, in the order the source delivers them, such as one Kafka
/// partition or one SQS FIFO message group. Deliveries without an ordering
/// scope run in parallel. The default [`Distribution`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SourceOrder;

impl<M: SourceMessage> Distribution<M> for SourceOrder {
    type Key = OrderingKey;

    fn key(&self, message: &M, _: &M::Item) -> Option<OrderingKey> {
        message.ordering_key()
    }

    fn keeps_source_order(&self) -> bool {
        true
    }
}

/// Processes every delivery independently of the others, up to the
/// subscription's [`concurrency`](crate::Subscription::concurrency).
///
/// Deliveries of one ordering scope may then complete out of order; source
/// adapters still acknowledge only the progress that is safe to record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Parallel;

impl<M: SourceMessage> Distribution<M> for Parallel {
    type Key = ();

    fn key(&self, _: &M, _: &M::Item) -> Option<()> {
        None
    }
}

impl<M, F, K> Distribution<M> for F
where
    M: SourceMessage,
    F: Fn(&M::Item) -> K + Send + Sync + 'static,
    K: Hash,
{
    type Key = K;

    fn key(&self, _: &M, value: &M::Item) -> Option<K> {
        Some(self(value))
    }
}
