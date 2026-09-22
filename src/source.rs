//! Receive lifecycle and source resource ownership.
use crate::message::SourceMessage;

/// Decoded handler input associated with a source.
pub type SourceItem<S> = <<S as Source>::Message as SourceMessage>::Item;

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
    ) -> impl std::future::Future<Output = std::result::Result<Receive<Self::Message>, ReceiveError>>
    + Send;
    fn close(&mut self) -> impl std::future::Future<Output = anyhow::Result<()>> + Send {
        async { Ok(()) }
    }
}
