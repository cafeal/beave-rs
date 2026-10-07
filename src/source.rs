//! Receive lifecycle and source resource ownership.
use crate::error::BoxError;
use crate::message::SourceMessage;
use std::future::Future;

/// Decoded handler input associated with a source.
pub type SourceItem<S> = <<S as Source>::Message as SourceMessage>::Item;

/// Undecoded delivery form associated with a source, carried by dead letters.
pub type SourceRaw<S> = <<S as Source>::Message as SourceMessage>::Raw;

/// Outcome of a successful [`Source::receive`].
#[derive(Debug)]
pub enum Receive<M> {
    /// One received delivery.
    Message(M),
    /// No more messages will ever arrive. A temporarily empty source waits instead.
    End,
}

/// Failure of a [`Source::receive`] call.
#[derive(Debug)]
pub enum ReceiveError {
    /// A transient failure. The subscription calls `receive` again after the receive retry
    /// delay and fails once the receive retry attempts are exhausted.
    Retry(BoxError),
    /// An unrecoverable failure that stops the subscription.
    Fatal(BoxError),
}

/// `receive` must be cancellation-safe: dropping it must not silently lose a delivery.
pub trait Source: Send + 'static {
    /// Delivery type produced by this source.
    type Message: SourceMessage;
    /// Wait for the next delivery, the end of input, or a failure.
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
    /// Release the source's resources after every received delivery has finished.
    fn close(&mut self) -> impl Future<Output = Result<(), BoxError>> + Send {
        async { Ok(()) }
    }
}
