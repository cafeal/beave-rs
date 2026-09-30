//! Typed output middleware for explicit metadata mapping.
use crate::handler::{Emit, Result};
use std::marker::PhantomData;

/// Intercepts decoded input before the handler and maps each handler output
/// using the input that produced it.
///
/// `intercept` runs once per delivery, before the handler. The first
/// registered middleware returning `Some` replaces the handler for that
/// delivery: its values become the outputs, and `Emit::None` completes the
/// delivery without output. Intercepted outputs still pass through every
/// middleware's `map`. The default implementation never intercepts.
///
/// `map` runs once per emitted value, after the handler succeeds and before
/// the sink prepares any output. Publish retries reuse the mapped and
/// prepared output, so a middleware never runs twice for the same value.
/// Input and output types are checked at compile time: a middleware written
/// for one broker's record and publish types cannot be registered on a
/// subscription with a different source or sink.
///
/// Error classification for both hooks: `Reject` routes the original input to
/// the subscription's dead-letter sink and then acknowledges it, like a handler
/// rejection. `Retry` and `Fatal` stop processing without ACK; a middleware
/// error never reruns the handler or the middleware.
pub trait Middleware<I, O>: Send + Sync + 'static {
    fn intercept(&self, _input: &I) -> Result<Option<Emit<O>>> {
        Ok(None)
    }

    fn map(&self, _input: &I, output: O) -> Result<O> {
        Ok(output)
    }
}

/// Middleware backed by a function or closure `Fn(&Input, Output) -> Result<Output>`.
///
/// Use it for mappings that the built-in adapter middleware does not cover,
/// including cross-platform conversions between broker record and publish
/// types.
pub struct MapMetadata<F, I, O> {
    map: F,
    marker: PhantomData<fn(&I, O) -> O>,
}

impl<F, I, O> MapMetadata<F, I, O>
where
    F: Fn(&I, O) -> Result<O>,
{
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
    I: 'static,
    O: 'static,
{
    fn map(&self, input: &I, output: O) -> Result<O> {
        (self.map)(input, output)
    }
}
