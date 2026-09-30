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
/// Ordinary errors propagated with `?` reject the input: it is dead-lettered without handler
/// retries under the default error policy. Use [`Classify`] to request a retry or to stop.
impl<E: Into<anyhow::Error>> From<E> for HandlerError {
    fn from(error: E) -> Self {
        Self::Reject(error.into())
    }
}

/// Classifies an ordinary error at the call site. `?` alone rejects; `reject` states that
/// choice explicitly.
///
/// ```
/// use beavers::{Classify, Result};
///
/// async fn handle(line: String) -> Result<u32> {
///     // A transient failure is retried under the handler retry policy.
///     let amount: u32 = line.parse().retry()?;
///     Ok(amount)
/// }
/// ```
pub trait Classify<T> {
    /// Classify the error as [`HandlerError::Reject`]: dead-letter the input without retrying.
    fn reject(self) -> Result<T>;
    /// Classify the error as [`HandlerError::Retry`]: rerun the handler under its retry policy.
    fn retry(self) -> Result<T>;
    /// Classify the error as [`HandlerError::Fatal`]: stop the subscription without ACK.
    fn fatal(self) -> Result<T>;
}

impl<T, E: Into<anyhow::Error>> Classify<T> for StdResult<T, E> {
    fn reject(self) -> Result<T> {
        self.map_err(|error| HandlerError::Reject(error.into()))
    }

    fn retry(self) -> Result<T> {
        self.map_err(|error| HandlerError::Retry(error.into()))
    }

    fn fatal(self) -> Result<T> {
        self.map_err(|error| HandlerError::Fatal(error.into()))
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
