//! Atomic publication and acknowledgement for compatible source and sink pairs.
use crate::{message::SourceMessage, sink::Sink};
use std::future::Future;

/// A sink that can publish a delivery's outputs and acknowledge that delivery
/// in one broker transaction.
///
/// Implemented by an adapter's transactional sink for the source message types
/// whose acknowledgement can join its transactions, such as Kafka consumer
/// offsets committed through a Kafka producer transaction. A pair without an
/// implementation cannot be registered with
/// [`Subscription::transactional`](crate::Subscription::transactional).
pub trait TransactionalSink<M: SourceMessage, T>: Sink<T> {
    /// Fails when `delivery`'s acknowledgement cannot join this sink's
    /// transactions, for example because the source and the sink connect to
    /// different clusters.
    ///
    /// The runtime calls it with the first delivery of a transactional
    /// subscription before processing that delivery, retrying under the
    /// subscription's `publish_retry` policy. A failure stops the subscription
    /// without processing or acknowledging anything.
    fn verify_source(&self, delivery: &M) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Publishes `outputs` and acknowledges `delivery` atomically: either every
    /// output becomes visible together with the acknowledgement, or neither
    /// takes effect. An empty `outputs` acknowledges the delivery alone.
    ///
    /// A failed commit leaves the delivery unacknowledged, and the runtime may
    /// retry it with the same prepared outputs. Dropping the returned future
    /// must not leave a transaction half-finished; the implementation either
    /// completes or aborts it.
    fn commit(
        &self,
        delivery: &M,
        outputs: &[Self::Prepared],
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}
