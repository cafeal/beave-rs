//! Typed subscription middleware around the handler.
use crate::handler::{Emit, Result};
use std::{future::Future, marker::PhantomData};

/// The outcome of [`Middleware::pre_handler`].
#[derive(Debug)]
pub enum Flow<I, O> {
    /// Continue with this input, which may have been transformed. The next
    /// middleware's `pre_handler`, and finally the handler, receive it.
    Continue(I),
    /// Skip the remaining `pre_handler` hooks and the handler. The values
    /// become the delivery's outputs; `Emit::None` completes it without output.
    Intercept(Emit<O>),
}

/// Middleware that runs before and after the handler.
///
/// Both hooks are asynchronous, so a middleware can await external lookups
/// such as a schema registry, a cache, or a metadata service. Implement them
/// with `async fn`; the returned futures must be `Send`. The runtime awaits
/// each hook before starting the next one, so hooks of one delivery never run
/// concurrently. A hook without `.await` points runs to completion in its
/// first poll.
///
/// `pre_handler` runs once per delivery, in registration order, before the
/// handler. It receives the input produced by the previous middleware and
/// either continues with a possibly transformed input or intercepts the
/// delivery. Handler retries reuse the transformed input without rerunning
/// `pre_handler`.
///
/// `post_handler` runs in registration order once per output, including
/// outputs from an intercepting middleware, after the handler succeeds and
/// before the sink prepares any output. It receives the input as decoded from
/// the source, before any `pre_handler` transformation, so metadata can be
/// inherited from the received record. Publish retries reuse the prepared
/// output, so a middleware never runs twice for the same value.
///
/// Input and output types are checked at compile time: a middleware written
/// for one broker's record and publish types cannot be registered on a
/// subscription with a different source or sink. Both hooks default to doing
/// nothing.
///
/// Error classification for both hooks: `Reject` routes the input as decoded
/// from the source to the subscription's dead-letter sink and then
/// acknowledges it, like a handler rejection. `Retry` and `Fatal` stop
/// processing without ACK; a middleware error never reruns the handler or the
/// middleware.
pub trait Middleware<I, O>: Send + Sync + 'static
where
    I: Send + Sync,
    O: Send,
{
    /// Inspect, transform, or intercept the input before the handler runs.
    fn pre_handler(&self, input: I) -> impl Future<Output = Result<Flow<I, O>>> + Send {
        async { Ok(Flow::Continue(input)) }
    }

    /// Adjust one output, such as its metadata, before the sink prepares it.
    fn post_handler(&self, _input: &I, output: O) -> impl Future<Output = Result<O>> + Send {
        async { Ok(output) }
    }
}

/// Middleware backed by a function or closure `Fn(&Input, Output) -> Result<Output>`
/// used as its `post_handler`.
///
/// Use it for synchronous output mappings that the built-in adapter middleware
/// does not cover, including cross-platform conversions between broker record
/// and publish types. Implement [`Middleware`] directly when the mapping needs
/// to await.
pub struct MapMetadata<F, I, O> {
    map: F,
    marker: PhantomData<fn(&I, O) -> O>,
}

impl<F, I, O> MapMetadata<F, I, O>
where
    F: Fn(&I, O) -> Result<O>,
{
    /// Middleware whose `post_handler` applies `map`.
    pub fn new(map: F) -> Self {
        Self {
            map,
            marker: PhantomData,
        }
    }
}

impl<F, I, O> Middleware<I, O> for MapMetadata<F, I, O>
where
    F: Fn(&I, O) -> Result<O> + Send + Sync + 'static,
    I: Send + Sync + 'static,
    O: Send + 'static,
{
    async fn post_handler(&self, input: &I, output: O) -> Result<O> {
        (self.map)(input, output)
    }
}
