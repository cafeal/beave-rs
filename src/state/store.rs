//! The contract between the runtime and a state store.
use crate::{
    error::BoxError,
    handler::Result,
    message::{Position, SourceMessage},
    shutdown::CancellationToken,
};
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
/// input, from which a store may derive the state key, and `M` the source's
/// message type, which a store can require to have message keys.
pub trait StateStore<I, M>: Send + Sync + 'static {
    /// [`Keyed`] for a store that keeps state per key and implements
    /// [`KeyedStateStore`], or [`Unkeyed`] for a store whose handler finds its
    /// state itself, such as through a database transaction.
    type Keying: Keying;
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

/// A [`StateStore`] that keeps state per key.
///
/// A subscription with a keyed store distributes its deliveries by the state
/// key: deliveries with equal keys are processed one at a time, in the order
/// they were received, so each sees the state committed by the one before.
/// Such a subscription takes no
/// [`distribution`](crate::Subscription::distribution) of its own.
pub trait KeyedStateStore<I, M>: StateStore<I, M, Keying = Keyed> {
    /// The key `input`'s state is kept under. The runtime calls it when it
    /// distributes the delivery and again before [`begin`](StateStore::begin);
    /// an error leaves the delivery undistributed, and `begin` reports it.
    fn key(&self, input: &I, context: &StateContext) -> Result<Vec<u8>>;
}

/// Whether a [`StateStore`] keeps state per key: [`Keyed`] or [`Unkeyed`].
pub trait Keying: sealed::Sealed + Send + Sync + 'static {}

/// [`Keying`] of a [`KeyedStateStore`], and of a subscription whose
/// deliveries are distributed by their state key.
#[derive(Debug)]
pub enum Keyed {}

/// [`Keying`] of a store without state keys, and of a subscription that
/// takes any [`Distribution`](crate::Distribution), such as one with a
/// stateless handler.
///
/// The runtime does not order the deliveries of such a store beyond the
/// subscription's distribution, so concurrent changes are the store's to
/// detect, for example with database transactions.
#[derive(Debug)]
pub enum Unkeyed {}

impl Keying for Keyed {}
impl Keying for Unkeyed {}

/// The [`Keying`] of a subscription that takes a
/// [`distribution`](crate::Subscription::distribution): [`Unkeyed`].
#[diagnostic::on_unimplemented(
    message = "a subscription with keyed state distributes by its state key and takes no distribution",
    label = "the stateful handler's store keeps keyed state",
    note = "deliveries with one state key are already processed one at a time, in receive order; remove `.distribution(..)`"
)]
pub trait Distributable: Keying {}
impl Distributable for Unkeyed {}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Keyed {}
    impl Sealed for super::Unkeyed {}
}

/// The state key of a delivery, as a store of keying `Self` reports it.
#[doc(hidden)]
pub trait KeyOf<St, I, M>: Keying {
    /// Whether the store has keys to distribute by.
    const KEYED: bool;
    /// The key, or `None` when the store has no keys or fails to report one.
    fn key(store: &St, input: &I, context: &StateContext) -> Option<Vec<u8>>;
}

impl<St: KeyedStateStore<I, M>, I, M> KeyOf<St, I, M> for Keyed {
    const KEYED: bool = true;
    fn key(store: &St, input: &I, context: &StateContext) -> Option<Vec<u8>> {
        store.key(input, context).ok()
    }
}

impl<St: StateStore<I, M, Keying = Unkeyed>, I, M> KeyOf<St, I, M> for Unkeyed {
    const KEYED: bool = false;
    fn key(_: &St, _: &I, _: &StateContext) -> Option<Vec<u8>> {
        None
    }
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
    /// The context of `delivery` in the subscription named `subscription`.
    pub(crate) fn of<M: SourceMessage>(subscription: &str, delivery: &M) -> Self {
        Self {
            subscription: Arc::from(subscription),
            message_key: delivery.message_key().map(Arc::from),
            position: delivery.position(),
            revocation: delivery.revocation(),
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
