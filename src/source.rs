//! Receive lifecycle and source resource ownership.
use crate::error::BoxError;
use crate::message::SourceMessage;
use std::{future::Future, sync::Arc};

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
    /// A handle through which the runtime reads the source's connection state
    /// and consumer lag for health reports and metrics. Called once, before the
    /// first `receive`; a source that connects lazily returns a handle that
    /// reports unknown values until it connects. Sources that cannot observe
    /// either return `None`.
    fn probe(&self) -> Option<Arc<dyn SourceProbe>> {
        None
    }
}

/// Connection state and consumer lag observed by a source.
///
/// Health reports read the probe on every request, so both methods must
/// return immediately from state the source already holds, without network
/// access. `None` means the value is unknown, for example before the source
/// connects.
pub trait SourceProbe: Send + Sync + 'static {
    /// Whether the source is connected to its broker. A source that reports
    /// `Some(false)` makes its subscription unready.
    fn connected(&self) -> Option<bool> {
        None
    }
    /// Messages available to the subscription that it has not acknowledged
    /// yet, such as the distance from a Kafka partition's committed offset to
    /// its end, summed over the partitions the source consumes.
    fn lag(&self) -> Option<u64> {
        None
    }
}
