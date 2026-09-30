//! Object-safe form of [`Middleware`] stored by a subscription.
use super::completion::BoxFuture;
use crate::{
    handler::Result,
    middleware::{Flow, Middleware},
};

/// Boxes the futures of a registered middleware's hooks so a subscription can
/// hold middleware of different types in one list.
pub(super) trait DynMiddleware<I, O>: Send + Sync + 'static {
    fn pre_handler(&self, input: I) -> BoxFuture<'_, Result<Flow<I, O>>>;
    fn post_handler<'a>(&'a self, input: &'a I, output: O) -> BoxFuture<'a, Result<O>>;
}

impl<I, O, M> DynMiddleware<I, O> for M
where
    M: Middleware<I, O>,
    I: Send + Sync + 'static,
    O: Send + 'static,
{
    fn pre_handler(&self, input: I) -> BoxFuture<'_, Result<Flow<I, O>>> {
        Box::pin(Middleware::pre_handler(self, input))
    }

    fn post_handler<'a>(&'a self, input: &'a I, output: O) -> BoxFuture<'a, Result<O>> {
        Box::pin(Middleware::post_handler(self, input, output))
    }
}
