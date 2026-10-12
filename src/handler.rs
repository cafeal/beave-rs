//! Handler results, error classification, and output cardinality.
use crate::error::BoxError;
use std::{future::Future, result::Result as StdResult};

/// Result returned by handlers and middleware.
pub type Result<T> = StdResult<T, HandlerError>;

/// Zero, one, or many outputs produced from one input by an emitting handler.
///
/// The input is acknowledged only after every emitted output is published.
#[derive(Debug)]
pub enum Emit<T> {
    /// Produce no output; the input is acknowledged without publishing.
    None,
    /// Produce exactly one output.
    One(T),
    /// Produce each value as a separate output, in order.
    Many(Vec<T>),
}
impl<T> Emit<T> {
    /// Apply `f` to each output, keeping the cardinality.
    pub fn map<U>(self, f: impl FnMut(T) -> U) -> Emit<U> {
        let mut f = f;
        match self {
            Self::None => Emit::None,
            Self::One(v) => Emit::One(f(v)),
            Self::Many(v) => Emit::Many(v.into_iter().map(f).collect()),
        }
    }
    pub(crate) fn values(self) -> Vec<T> {
        match self {
            Self::None => vec![],
            Self::One(v) => vec![v],
            Self::Many(v) => v,
        }
    }
}

/// How the runtime treats a failed delivery.
///
/// The subscription's error policy decides the final outcome; see
/// [`ErrorPolicy`](crate::ErrorPolicy).
#[derive(Debug)]
pub enum HandlerError {
    /// A transient failure: rerun the handler under its retry policy. A failure on the last
    /// permitted attempt is routed as [`FailureKind::RetryExhausted`](crate::FailureKind).
    Retry(BoxError),
    /// The input cannot be processed: route it as [`FailureKind::Rejected`](crate::FailureKind)
    /// without retrying, which dead-letters it by default.
    Reject(BoxError),
    /// An unrecoverable failure: stop the subscription without acknowledging the input.
    Fatal(BoxError),
}
/// Ordinary errors propagated with `?` reject the input: it is dead-lettered without handler
/// retries under the default error policy. Use [`Classify`] to request a retry or to stop.
impl<E: Into<BoxError>> From<E> for HandlerError {
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

impl<T, E: Into<BoxError>> Classify<T> for StdResult<T, E> {
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
///
/// Subscriptions accept a handler whose input is the received record or its value,
/// and whose output is the sink's type or a plain value; see
/// [`IntoHandler`](crate::IntoHandler).
pub trait Handler<I>: Send + Sync + 'static {
    /// Value published to the sink for each successfully handled input.
    type Output: Send + Sync + 'static;
    /// Process one decoded input.
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
