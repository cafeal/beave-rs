//! How a store chooses the key a delivery's state is kept under.
use super::store::StateContext;
use crate::{
    handler::{HandlerError, Result},
    message::KeyedMessage,
};

/// Chooses the key under which a delivery's state is stored, for handler
/// input `I` from source messages `M`.
///
/// [`MessageKey`] uses the delivery's message key, for sources whose messages
/// implement [`KeyedMessage`]. A closure taking the handler's input chooses
/// any other key:
///
/// ```
/// use beavers::state::InMemoryState;
///
/// #[derive(Clone)]
/// struct Order {
///     user_id: u64,
/// }
///
/// let counts = InMemoryState::<u64>::new().key(|order: &Order| order.user_id.to_string());
/// # let _ = counts;
/// ```
///
/// Deliveries with the same key share state. The runtime processes them one
/// at a time per store, but only deliveries the subscription's
/// [`Distribution`](crate::Distribution) gives the same key are processed in
/// the order they were received; distribute by the state key to keep that
/// order.
pub trait StateKey<I, M>: Send + Sync + 'static {
    /// The key for `input`. An error is classified like a handler error.
    fn key(&self, input: &I, context: &StateContext) -> Result<Vec<u8>>;
}

/// Keeps state per [message key](crate::SourceMessage::message_key), such as
/// the Kafka record key. Only sources whose messages implement
/// [`KeyedMessage`] accept it, and a single delivery without a key, such as a
/// Kafka record with a null key, is rejected.
///
/// A source without message keys, such as [`IterSource`](crate::IterSource),
/// fails to compile with it:
///
/// ```compile_fail
/// use beavers::state::{InMemoryState, State, stateful};
/// use beavers::{App, InMemorySink, IterSource};
///
/// async fn total(value: i32, mut total: State<i32>) -> beavers::Result<i32> {
///     *total += value;
///     Ok(*total)
/// }
///
/// App::new().subscribe(
///     "totals",
///     IterSource::new([1, 2]),
///     InMemorySink::<i32>::default(),
///     stateful(InMemoryState::<i32>::new(), total),
/// );
/// ```
///
/// Such a source needs a key chosen from the input:
///
/// ```
/// # use beavers::state::{InMemoryState, State, stateful};
/// # use beavers::{App, InMemorySink, IterSource};
/// # async fn total(value: i32, mut total: State<i32>) -> beavers::Result<i32> {
/// #     *total += value;
/// #     Ok(*total)
/// # }
/// App::new().subscribe(
///     "totals",
///     IterSource::new([1, 2]),
///     InMemorySink::<i32>::default(),
///     stateful(InMemoryState::<i32>::new().key(|_: &i32| "all"), total),
/// );
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct MessageKey;

impl<I, M: KeyedMessage> StateKey<I, M> for MessageKey {
    fn key(&self, _input: &I, context: &StateContext) -> Result<Vec<u8>> {
        context.message_key().map(<[u8]>::to_vec).ok_or_else(|| {
            HandlerError::Reject("the delivery has no message key to keep its state under".into())
        })
    }
}

impl<I, M, F, K> StateKey<I, M> for F
where
    F: Fn(&I) -> K + Send + Sync + 'static,
    K: AsRef<[u8]>,
{
    fn key(&self, input: &I, _context: &StateContext) -> Result<Vec<u8>> {
        Ok(self(input).as_ref().to_vec())
    }
}
