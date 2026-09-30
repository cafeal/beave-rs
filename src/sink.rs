//! Preparing output is separate from transport publication and its retries.
use std::{future::Future, pin::Pin};

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

/// Sink metadata belongs in `T` or `Prepared`, not in a universal broker envelope.
pub trait Sink<T>: Send + Sync + 'static {
    type Prepared: Send + Sync + 'static;
    /// Validate and encode once, before any publish attempts. No publishing here.
    fn prepare(&self, value: T) -> anyhow::Result<Self::Prepared>;
    /// Success means the output reached this sink's acknowledgement boundary.
    /// Retrying the same prepared output must not rerun encoding or routing.
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
