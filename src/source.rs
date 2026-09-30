//! Receive lifecycle and source resource ownership.
use crate::message::SourceMessage;
use std::future::Future;

/// Decoded handler input associated with a source.
pub type SourceItem<S> = <<S as Source>::Message as SourceMessage>::Item;

/// Undecoded delivery form associated with a source, carried by dead letters.
pub type SourceRaw<S> = <<S as Source>::Message as SourceMessage>::Raw;

#[derive(Debug)]
pub enum Receive<M> {
    Message(M),
    End,
}
#[derive(Debug)]
pub enum ReceiveError {
    Retry(anyhow::Error),
    Fatal(anyhow::Error),
}

/// `receive` must be cancellation-safe: dropping it must not silently lose a delivery.
pub trait Source: Send + 'static {
    type Message: SourceMessage;
    fn receive(
        &mut self,
    ) -> impl Future<Output = Result<Receive<Self::Message>, ReceiveError>> + Send;
    /// Whether application shutdown stops receiving from this source. A source fed by
    /// another subscription of the same application returns `false`: it keeps
    /// delivering until that subscription closes its sink, so work the upstream is
    /// still draining can complete. A subscription failure stops it either way.
    fn stops_on_shutdown(&self) -> bool {
        true
    }
    /// Called once when the subscription stops receiving before the source ended,
    /// on shutdown or failure, before received deliveries drain. A source that
    /// accepts input from clients stops accepting it here, so new input is refused
    /// instead of waiting for a `receive` that will not come. Deliveries already
    /// received still complete, and `close` follows after draining.
    fn stop_receiving(&mut self) {}
    fn close(&mut self) -> impl Future<Output = anyhow::Result<()>> + Send {
        async { Ok(()) }
    }
}
