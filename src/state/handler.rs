//! Handlers that take state with their input.
use crate::handler::Result;
use std::future::Future;

/// Async transformation of one input with the state the store keeps for it.
/// Ordinary async functions and closures taking `(input, state)` implement
/// this.
///
/// Register it with [`stateful`], which pairs it with its [`StateStore`](super::StateStore).
pub trait StatefulHandler<I, St>: Send + Sync + 'static {
    /// Value published to the sink for each successfully handled input.
    type Output: Send + Sync + 'static;
    /// Process one decoded input with its state.
    fn handle(&self, input: I, state: St) -> impl Future<Output = Result<Self::Output>> + Send;
}

impl<I, St, O, F, Fut> StatefulHandler<I, St> for F
where
    F: Fn(I, St) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O>> + Send,
    O: Send + Sync + 'static,
{
    type Output = O;
    fn handle(&self, input: I, state: St) -> impl Future<Output = Result<O>> + Send {
        self(input, state)
    }
}

/// A [`StatefulHandler`] paired with its [`StateStore`](super::StateStore), built by
/// [`stateful`] and accepted wherever a handler is.
#[derive(Debug)]
pub struct Stateful<St, H> {
    pub(crate) store: St,
    pub(crate) handler: H,
}

/// Pairs `handler` with `store`, so the handler receives the store's state
/// for each input and its change is committed after the delivery's outputs
/// are published.
///
/// The result registers like any handler, with
/// [`App::subscribe`](crate::App::subscribe),
/// [`Subscription::new`](crate::Subscription::new), or
/// [`Subscription::new_emitting`](crate::Subscription::new_emitting), and in
/// the same input and output shapes. A stateful subscription cannot be
/// [transactional](crate::Subscription::transactional).
///
/// ```
/// use beavers::{
///     App, Emit, Result, StdoutSink, Json,
///     state::{InMemoryState, State, stateful},
/// };
/// # use beavers::IterSource;
///
/// // Publish each word the first time it is seen.
/// async fn first_seen(word: String, mut seen: State<bool>) -> Result<Emit<String>> {
///     if *seen {
///         return Ok(Emit::None);
///     }
///     *seen = true;
///     Ok(Emit::One(word))
/// }
///
/// let seen = InMemoryState::<bool>::new().key(|word: &String| word.clone());
/// let words = IterSource::new(["a".to_owned(), "b".to_owned(), "a".to_owned()]);
/// App::new().subscription(beavers::Subscription::new_emitting(
///     "first-seen",
///     words,
///     StdoutSink::<Json<String>>::new(),
///     stateful(seen, first_seen),
/// ));
/// ```
pub fn stateful<St, H>(store: St, handler: H) -> Stateful<St, H> {
    Stateful { store, handler }
}
