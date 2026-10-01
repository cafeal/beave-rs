//! Preparing output is separate from transport publication and its retries.
use std::{fmt, future::Future, pin::Pin};

type CompletionFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

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
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        Self(Some(Box::pin(future)))
    }

    pub fn is_done(&self) -> bool {
        self.0.is_none()
    }

    pub async fn wait(self) -> anyhow::Result<()> {
        match self.0 {
            Some(future) => future.await,
            None => Ok(()),
        }
    }
}

/// Marks a publication error as a permanent refusal of the output by its
/// destination, such as an HTTP `400 Bad Request`.
///
/// A sink attaches it as context with [`PublishRejected::wrap`]. The runtime
/// does not retry a rejected output and routes the delivery through the
/// error policy as [`FailureKind::PublishRejected`](crate::FailureKind::PublishRejected)
/// instead of stopping the subscription. It is recognized on errors returned by
/// [`Sink::publish`] and [`Sink::submit`], not on a failed [`Completion`]. A
/// rejected [`TransactionalSink::commit`](crate::TransactionalSink::commit)
/// fails its whole batch instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublishRejected;

impl PublishRejected {
    /// Marks `error` as a rejection, keeping it as the displayed cause.
    pub fn wrap(error: anyhow::Error) -> anyhow::Error {
        error.context(Self)
    }

    /// Whether `error` or a context it was wrapped in marks a rejection.
    pub fn is(error: &anyhow::Error) -> bool {
        error.downcast_ref::<Self>().is_some()
    }
}

impl fmt::Display for PublishRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("output rejected by its destination")
    }
}

/// Sink metadata belongs in `T` or `Prepared`, not in a universal broker envelope.
pub trait Sink<T>: Send + Sync + 'static {
    type Prepared: Send + Sync + 'static;
    /// Validate and encode once, before any publish attempts. No publishing here.
    fn prepare(&self, value: T) -> anyhow::Result<Self::Prepared>;
    /// Success means the output reached this sink's acknowledgement boundary.
    /// Retrying the same prepared output must not rerun encoding or routing.
    /// An error marked with [`PublishRejected`] is not retried.
    fn publish(&self, output: &Self::Prepared) -> impl Future<Output = anyhow::Result<()>> + Send;
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
    ) -> impl Future<Output = anyhow::Result<Completion>> + Send {
        async move {
            self.publish(output).await?;
            Ok(Completion::done())
        }
    }
    fn close(&self) -> impl Future<Output = anyhow::Result<()>> + Send {
        async { Ok(()) }
    }
}
