//! The contract between the runtime and a state store.
use crate::{error::BoxError, handler::Result, message::Position, shutdown::CancellationToken};
use std::{fmt, future::Future, sync::Arc};

/// Storage that a [stateful](super::stateful) handler reads and changes for
/// each delivery, with the change committed only after the delivery's outputs
/// are published and before the delivery is acknowledged.
///
/// For each handler attempt the runtime calls [`begin`](Self::begin), passes
/// the returned [`State`](Self::State) to the handler, and keeps the
/// [`Txn`](Self::Txn). When the handler fails, or the delivery is
/// dead-lettered or discarded instead of published, the runtime drops the
/// `Txn` and the change is not applied. Otherwise it publishes the outputs and
/// calls [`commit`](Self::commit), then acknowledges the delivery.
///
/// `State` is what the handler works with: [`State<T>`](super::State) for one
/// value per key, or a handle to the store's own client for stores that let
/// the handler use the backend's features directly. `I` is the handler's
/// input, from which a store may derive the state key.
pub trait StateStore<I>: Send + Sync + 'static {
    /// What the handler receives with each input.
    type State: Send + 'static;
    /// What the runtime keeps from `begin` until `commit`; dropping it
    /// abandons the change.
    type Txn: Send + 'static;

    /// Starts the state change for one handler attempt, or reports that the
    /// store already holds this delivery's change, as recorded by a commit
    /// at a [`Position`] that covers the delivery's. The handler then does not
    /// run and the delivery is acknowledged without output.
    ///
    /// Errors are classified like handler errors: `Retry` reruns the attempt
    /// under the handler retry policy, `Reject` routes the delivery by the
    /// error policy, and `Fatal` stops the subscription.
    fn begin(
        &self,
        input: &I,
        context: &StateContext,
    ) -> impl Future<Output = Result<Begin<Self::State, Self::Txn>>> + Send;

    /// Applies the change the handler made through `State`. A failure stops
    /// the subscription without acknowledging the delivery, so it is
    /// redelivered after a restart and processed against the stored state.
    fn commit(
        &self,
        txn: Self::Txn,
    ) -> impl Future<Output = std::result::Result<(), BoxError>> + Send;
}

/// Outcome of [`StateStore::begin`].
#[derive(Debug)]
pub enum Begin<S, T> {
    /// Run the handler with `S`, then commit `T`.
    Ready(S, T),
    /// The store already holds this delivery's change: acknowledge it
    /// without running the handler.
    Applied,
}

/// What a [`StateStore`] knows about the delivery being processed.
#[derive(Clone)]
pub struct StateContext {
    subscription: Arc<str>,
    message_key: Option<Arc<[u8]>>,
    position: Option<Position>,
    revocation: Option<CancellationToken>,
}

impl StateContext {
    pub(crate) fn new(
        subscription: Arc<str>,
        message_key: Option<&[u8]>,
        position: Option<Position>,
        revocation: Option<CancellationToken>,
    ) -> Self {
        Self {
            subscription,
            message_key: message_key.map(Arc::from),
            position,
            revocation,
        }
    }

    /// Name of the subscription, which a store shared by several
    /// subscriptions can use to keep their state apart.
    pub fn subscription(&self) -> &str {
        &self.subscription
    }

    /// The delivery's [message key](crate::SourceMessage::message_key).
    pub fn message_key(&self) -> Option<&[u8]> {
        self.message_key.as_deref()
    }

    /// The delivery's [position](crate::SourceMessage::position), to record
    /// with the change so that a redelivery is recognized as applied.
    pub fn position(&self) -> Option<&Position> {
        self.position.as_ref()
    }

    /// A token cancelled once the source no longer owns the delivery's
    /// partition, after which another consumer may change the state. A store
    /// that caches state locally stops trusting what it cached under this
    /// token.
    pub fn revocation(&self) -> Option<&CancellationToken> {
        self.revocation.as_ref()
    }
}

impl fmt::Debug for StateContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateContext")
            .field("subscription", &self.subscription)
            .field("message_key", &self.message_key)
            .field("position", &self.position)
            .finish_non_exhaustive()
    }
}
