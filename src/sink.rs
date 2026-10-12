//! Preparing output is separate from transport publication and its retries.
use crate::error::{BoxError, causes};
use crate::message::SourceMessage;
use std::{error::Error as StdError, fmt, future::Future, pin::Pin};

type CompletionFuture = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;

/// The outstanding part of a submitted publication, returned by [`Sink::submit`].
///
/// It resolves when the output reaches the sink's acknowledgement boundary.
/// Dropping it abandons that wait; a sink may then withdraw the output where its
/// transport allows it.
#[must_use = "a pending completion must be awaited before acknowledging the input"]
pub struct Completion(Option<CompletionFuture>);

impl Completion {
    /// The output already reached the acknowledgement boundary.
    pub fn done() -> Self {
        Self(None)
    }

    /// The output reaches the acknowledgement boundary when `future` succeeds.
    pub fn pending<F>(future: F) -> Self
    where
        F: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        Self(Some(Box::pin(future)))
    }

    /// Whether the output already reached the acknowledgement boundary.
    pub fn is_done(&self) -> bool {
        self.0.is_none()
    }

    /// Wait until the output reaches the acknowledgement boundary.
    pub async fn wait(self) -> Result<(), BoxError> {
        match self.0 {
            Some(future) => future.await,
            None => Ok(()),
        }
    }
}

/// Marks a publication error as a permanent refusal of the output by its
/// destination, such as an HTTP `400 Bad Request`.
///
/// A sink marks an error with [`PublishRejected::wrap`]. The runtime
/// does not retry a rejected output and routes the delivery through the
/// error policy as [`FailureKind::PublishRejected`](crate::FailureKind::PublishRejected)
/// instead of stopping the subscription. It is recognized on errors returned by
/// [`Sink::publish`] and [`Sink::submit`], not on a failed [`Completion`]. A
/// rejected [`TransactionalSink::commit`](crate::TransactionalSink::commit)
/// fails its whole batch instead.
#[derive(Debug)]
pub struct PublishRejected(BoxError);

impl PublishRejected {
    /// Marks `error` as a rejection, keeping it as the cause.
    pub fn wrap(error: impl Into<BoxError>) -> BoxError {
        Box::new(Self(error.into()))
    }

    /// Whether `error` or one of its causes marks a rejection.
    pub fn is(error: &(dyn StdError + 'static)) -> bool {
        causes(error).any(|cause| cause.is::<Self>())
    }
}

impl fmt::Display for PublishRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("output rejected by its destination")
    }
}

impl StdError for PublishRejected {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Sink metadata belongs in `T` or `Prepared`, not in a universal broker envelope.
pub trait Sink<T>: Send + Sync + 'static {
    /// Encoded and routed output, ready to publish.
    type Prepared: Send + Sync + 'static;
    /// Validate and encode once, before any publish attempts. No publishing here.
    fn prepare(&self, value: T) -> Result<Self::Prepared, BoxError>;
    /// Like [`prepare`](Self::prepare), with the delivery whose handler produced
    /// `value`. The runtime prepares a delivery's outputs with this method; dead
    /// letters and direct callers use `prepare`.
    ///
    /// The default ignores the delivery. A sink overrides it when an output keeps
    /// facts of its delivery that the handler does not see, as a
    /// [`ChannelSink`](crate::ChannelSink) keeps the ordering key and raw form for
    /// the next subscription.
    fn prepare_from<M: SourceMessage>(
        &self,
        value: T,
        delivery: &M,
    ) -> Result<Self::Prepared, BoxError> {
        let _ = delivery;
        self.prepare(value)
    }
    /// Success means the output reached this sink's acknowledgement boundary.
    /// Retrying the same prepared output must not rerun encoding or routing.
    /// An error marked with [`PublishRejected`] is not retried.
    fn publish(&self, output: &Self::Prepared)
    -> impl Future<Output = Result<(), BoxError>> + Send;
    /// Hand the output to the sink and return once the sink has accepted it, with
    /// the [`Completion`] that resolves at the acknowledgement boundary. The runtime
    /// submits outputs, frees the job's concurrency slot, and acknowledges the input
    /// after every completion succeeds. Publish retries apply to submission only.
    ///
    /// The default publishes and returns [`Completion::done`]. A sink whose
    /// acceptance precedes its acknowledgement boundary, such as a queue drained by
    /// another consumer, overrides it; `publish` must then equal `submit` followed
    /// by waiting for the completion.
    ///
    /// Pending completions do not count toward the subscription's `max_in_flight`,
    /// so a sink that returns them must bound how many are outstanding by making
    /// `submit` wait, for example for queue capacity.
    fn submit(
        &self,
        output: &Self::Prepared,
    ) -> impl Future<Output = Result<Completion, BoxError>> + Send {
        async move {
            self.publish(output).await?;
            Ok(Completion::done())
        }
    }
    /// Flush and release the sink after the subscription has stopped publishing to it.
    fn close(&self) -> impl Future<Output = Result<(), BoxError>> + Send {
        async { Ok(()) }
    }
}
