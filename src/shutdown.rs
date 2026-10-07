//! Cooperative cancellation and process termination signals.
use crate::error::{Context, Error};
use std::io;

/// Cloneable cooperative shutdown signal.
#[derive(Clone)]
pub struct CancellationToken(tokio::sync::watch::Sender<bool>);
impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}
impl CancellationToken {
    /// A token that is not cancelled.
    pub fn new() -> Self {
        Self(tokio::sync::watch::channel(false).0)
    }
    /// Cancel this token and every clone of it. Cancelling again has no effect.
    pub fn cancel(&self) {
        self.0.send_replace(true);
    }
    /// Whether the token has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }
    /// Wait until the token is cancelled.
    pub async fn cancelled(&self) {
        let mut receiver = self.0.subscribe();
        let _ = receiver.wait_for(|cancelled| *cancelled).await;
    }
}

pub(crate) async fn termination_signal() -> Result<(), Error> {
    wait_for_signal()
        .await
        .context("listening for termination signals")
}

async fn wait_for_signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result = tokio::signal::ctrl_c() => { result?; }, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}
