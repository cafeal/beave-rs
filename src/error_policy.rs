//! Per-failure outcomes for deliveries that cannot complete normally.
use serde::Serialize;
use std::fmt;

/// Processing failure that an [`ErrorPolicy`] can route.
///
/// Handler `Fatal` errors, receive failures, output-mapping failures, publish exhaustion, and
/// ACK failures are not routable: they always stop the subscription without ACK.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    /// `SourceMessage::decode` failed; no typed input exists.
    Decode,
    /// The handler returned `HandlerError::Reject`.
    Rejected,
    /// The handler returned `HandlerError::Retry` on its final permitted attempt.
    RetryExhausted,
    /// `Sink::prepare` failed for an emitted output; nothing was published.
    Encode,
}

impl fmt::Display for FailureKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Decode => "decode failed",
            Self::Rejected => "handler rejected input",
            Self::RetryExhausted => "handler retry exhausted",
            Self::Encode => "prepare output failed",
        })
    }
}

/// What the runtime does with a delivery after a routable failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureAction {
    /// Stop the subscription with an error and leave the delivery unacknowledged.
    Stop,
    /// Publish a [`DeadLetter`](crate::dead_letter::DeadLetter) to the subscription's
    /// dead-letter sink, then ACK. A failed dead-letter publication stops without ACK.
    DeadLetter,
    /// ACK without publishing anything. The delivery is intentionally lost.
    Discard,
}

/// Failure routing for one subscription.
///
/// Defaults preserve delivery: only handler rejections are dead-lettered, and a rejection
/// without a configured dead-letter sink stops without ACK. Choosing `DeadLetter` for any other
/// failure requires a dead-letter sink; the subscription fails validation otherwise.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorPolicy {
    pub decode: FailureAction,
    pub rejected: FailureAction,
    pub retry_exhausted: FailureAction,
    pub encode: FailureAction,
}

impl Default for ErrorPolicy {
    fn default() -> Self {
        Self {
            decode: FailureAction::Stop,
            rejected: FailureAction::DeadLetter,
            retry_exhausted: FailureAction::Stop,
            encode: FailureAction::Stop,
        }
    }
}

impl ErrorPolicy {
    /// Route every routable failure to the dead-letter sink.
    pub fn dead_letter_all() -> Self {
        Self {
            decode: FailureAction::DeadLetter,
            rejected: FailureAction::DeadLetter,
            retry_exhausted: FailureAction::DeadLetter,
            encode: FailureAction::DeadLetter,
        }
    }

    pub fn action(&self, kind: FailureKind) -> FailureAction {
        match kind {
            FailureKind::Decode => self.decode,
            FailureKind::Rejected => self.rejected,
            FailureKind::RetryExhausted => self.retry_exhausted,
            FailureKind::Encode => self.encode,
        }
    }

    pub(crate) fn validate(&self, has_dead_letter_sink: bool) -> anyhow::Result<()> {
        if has_dead_letter_sink {
            return Ok(());
        }
        for kind in [
            FailureKind::Decode,
            FailureKind::RetryExhausted,
            FailureKind::Encode,
        ] {
            anyhow::ensure!(
                self.action(kind) != FailureAction::DeadLetter,
                "error policy dead-letters {kind:?} failures but no dead-letter sink is configured"
            );
        }
        Ok(())
    }
}
