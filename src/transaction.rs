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
///
/// The runtime collects the deliveries of a transactional subscription into
/// batches, bounded by [`TransactionBatch`](crate::TransactionBatch), and
/// commits each batch in one transaction.
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

    /// Publishes the outputs of every entry and acknowledges every entry's
    /// delivery in one transaction: either all outputs become visible together
    /// with all acknowledgements, or none takes effect. An entry with empty
    /// `outputs` acknowledges its delivery alone.
    ///
    /// Entries are in completion order, so deliveries of one ordering scope
    /// appear in receive order and a later delivery of a scope acknowledges
    /// progress through every earlier one.
    ///
    /// A failed commit leaves every delivery unacknowledged, and the runtime
    /// may retry the batch with the same prepared outputs, without the entries
    /// whose deliveries were revoked in the meantime. Dropping the returned
    /// future must not leave a transaction half-finished; the implementation
    /// either completes or aborts it.
    fn commit(
        &self,
        batch: &[TransactionEntry<'_, M, Self::Prepared>],
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}

/// One delivery committed by [`TransactionalSink::commit`], with the prepared
/// outputs published in the same transaction.
pub struct TransactionEntry<'a, M, P> {
    pub delivery: &'a M,
    pub outputs: &'a [P],
}

impl<M, P> Clone for TransactionEntry<'_, M, P> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<M, P> Copy for TransactionEntry<'_, M, P> {}
