//! How a store chooses the key a delivery's state is kept under.
use super::store::StateContext;
use crate::handler::{HandlerError, Result};

/// Chooses the key under which a delivery's state is stored.
///
/// [`MessageKey`] uses the delivery's message key. A closure taking the
/// handler's input chooses any other key:
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
/// at a time per store, but only deliveries with the same
/// [ordering key](crate::SourceMessage::ordering_key) are processed in the
/// order they were received.
pub trait StateKey<I>: Send + Sync + 'static {
    /// The key for `input`. An error is classified like a handler error.
    fn key(&self, input: &I, context: &StateContext) -> Result<Vec<u8>>;
}

/// Keeps state per [message key](crate::SourceMessage::message_key), such as
/// the Kafka record key. A delivery without a message key is rejected, so
/// sources without message keys need a key chosen from the input.
#[derive(Clone, Copy, Debug, Default)]
pub struct MessageKey;

impl<I> StateKey<I> for MessageKey {
    fn key(&self, _input: &I, context: &StateContext) -> Result<Vec<u8>> {
        context.message_key().map(<[u8]>::to_vec).ok_or_else(|| {
            HandlerError::Reject(
                "the delivery has no message key to keep its state under; choose a state key from the input"
                    .into(),
            )
        })
    }
}

impl<I, F, K> StateKey<I> for F
where
    F: Fn(&I) -> K + Send + Sync + 'static,
    K: AsRef<[u8]>,
{
    fn key(&self, input: &I, _context: &StateContext) -> Result<Vec<u8>> {
        Ok(self(input).as_ref().to_vec())
    }
}
