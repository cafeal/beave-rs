//! Per-failure outcomes for deliveries that cannot complete normally.
use crate::error::{Error, ensure};
use serde::Serialize;
use std::{fmt, str::FromStr};

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
    /// The sink's destination refused an output permanently, marked with
    /// [`PublishRejected`](crate::sink::PublishRejected). Outputs submitted before
    /// it have completed.
    PublishRejected,
}

impl FailureKind {
    /// The snake_case name used in metric labels, serialized dead letters, and dead-letter
    /// headers.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Decode => "decode",
            Self::Rejected => "rejected",
            Self::RetryExhausted => "retry_exhausted",
            Self::Encode => "encode",
            Self::PublishRejected => "publish_rejected",
        }
    }
}

impl FromStr for FailureKind {
    type Err = Error;

    /// Parses the name returned by [`as_str`](Self::as_str).
    fn from_str(name: &str) -> Result<Self, Error> {
        [
            Self::Decode,
            Self::Rejected,
            Self::RetryExhausted,
            Self::Encode,
            Self::PublishRejected,
        ]
        .into_iter()
        .find(|kind| kind.as_str() == name)
        .ok_or_else(|| Error::invalid_record(format!("unknown failure kind {name:?}")))
    }
}

impl fmt::Display for FailureKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Decode => "decode failed",
            Self::Rejected => "handler rejected input",
            Self::RetryExhausted => "handler retry exhausted",
            Self::Encode => "prepare output failed",
            Self::PublishRejected => "sink rejected output",
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
/// Defaults never lose a delivery: handler failures (rejection and retry exhaustion) and
/// rejected publications are dead-lettered, and without a configured dead-letter sink they
/// stop without ACK. Decode and
/// encode failures stop by default; choosing `DeadLetter` for them requires a dead-letter sink,
/// and the subscription fails validation otherwise.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorPolicy {
    /// Action after [`FailureKind::Decode`]. Defaults to [`FailureAction::Stop`].
    pub decode: FailureAction,
    /// Action after [`FailureKind::Rejected`]. Defaults to [`FailureAction::DeadLetter`].
    pub rejected: FailureAction,
    /// Action after [`FailureKind::RetryExhausted`]. Defaults to [`FailureAction::DeadLetter`].
    pub retry_exhausted: FailureAction,
    /// Action after [`FailureKind::Encode`]. Defaults to [`FailureAction::Stop`].
    pub encode: FailureAction,
    /// Action after [`FailureKind::PublishRejected`]. Defaults to [`FailureAction::DeadLetter`].
    pub publish_rejected: FailureAction,
}

impl Default for ErrorPolicy {
    fn default() -> Self {
        Self {
            decode: FailureAction::Stop,
            rejected: FailureAction::DeadLetter,
            retry_exhausted: FailureAction::DeadLetter,
            encode: FailureAction::Stop,
            publish_rejected: FailureAction::DeadLetter,
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
            publish_rejected: FailureAction::DeadLetter,
        }
    }

    /// The configured action for `kind`.
    pub fn action(&self, kind: FailureKind) -> FailureAction {
        match kind {
            FailureKind::Decode => self.decode,
            FailureKind::Rejected => self.rejected,
            FailureKind::RetryExhausted => self.retry_exhausted,
            FailureKind::Encode => self.encode,
            FailureKind::PublishRejected => self.publish_rejected,
        }
    }

    pub(crate) fn validate(&self, has_dead_letter_sink: bool) -> Result<(), Error> {
        if has_dead_letter_sink {
            return Ok(());
        }
        for kind in [FailureKind::Decode, FailureKind::Encode] {
            ensure!(
                self.action(kind) != FailureAction::DeadLetter,
                Error::config,
                "error policy dead-letters {kind:?} failures but no dead-letter sink is configured"
            );
        }
        Ok(())
    }
}
