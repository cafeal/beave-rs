//! Preparing output is separate from transport publication and its retries.
use std::future::Future;

/// Sink metadata belongs in `T` or `Prepared`, not in a universal broker envelope.
pub trait Sink<T>: Send + Sync + 'static {
    type Prepared: Send + Sync + 'static;
    /// Validate and encode once, before any publish attempts. No publishing here.
    fn prepare(&self, value: T) -> anyhow::Result<Self::Prepared>;
    /// Success means the output reached this sink's acknowledgement boundary.
    /// Retrying the same prepared output must not rerun encoding or routing.
    fn publish(&self, output: &Self::Prepared) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn close(&self) -> impl Future<Output = anyhow::Result<()>> + Send {
        async { Ok(()) }
    }
}
