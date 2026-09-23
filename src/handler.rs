//! Handler results, error classification, and output cardinality.
use std::{future::Future, result::Result as StdResult};

pub type Result<T> = StdResult<T, HandlerError>;

#[derive(Debug)]
pub enum Emit<T> {
    None,
    One(T),
    Many(Vec<T>),
}
impl<T> Emit<T> {
    pub(crate) fn values(self) -> Vec<T> {
        match self {
            Self::None => vec![],
            Self::One(v) => vec![v],
            Self::Many(v) => v,
        }
    }
}

#[derive(Debug)]
pub enum HandlerError {
    Retry(anyhow::Error),
    Reject(anyhow::Error),
    Fatal(anyhow::Error),
}
impl<E: Into<anyhow::Error>> From<E> for HandlerError {
    fn from(error: E) -> Self {
        Self::Fatal(error.into())
    }
}

/// Async domain transformation. Ordinary async functions and closures implement this.
/// Use `Emit<T>` as the output with `Subscription::new_emitting` for 0/1/N output.
pub trait Handler<I>: Send + Sync + 'static {
    type Output: Send + Sync + 'static;
    fn handle(&self, input: I) -> impl Future<Output = Result<Self::Output>> + Send;
}
impl<I, O, F, Fut> Handler<I> for F
where
    F: Fn(I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O>> + Send,
    O: Send + Sync + 'static,
{
    type Output = O;
    fn handle(&self, input: I) -> impl Future<Output = Result<O>> + Send {
        self(input)
    }
}
