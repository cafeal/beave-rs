//! Envelope published to a dead-letter sink.
use crate::{error_policy::FailureKind, message::RawPayload};
use serde::Serialize;

/// A delivery that could not be processed, with structured failure context.
///
/// Handlers never see this type; only dead-letter sinks receive it. `input` is the decoded
/// handler input when decoding succeeded. `raw` is the received payload when the source
/// retains it (see [`SourceMessage::raw_payload`](crate::message::SourceMessage::raw_payload)).
#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct DeadLetter<I> {
    /// Name of the subscription that produced the dead letter.
    pub subscription: String,
    pub failure: FailureKind,
    /// Error message including its context chain.
    pub error: String,
    /// Handler attempts made before the failure; zero for decode failures.
    pub attempts: usize,
    pub input: Option<I>,
    pub raw: Option<RawPayload>,
}

impl<I> DeadLetter<I> {
    pub(crate) fn new(
        subscription: String,
        failure: FailureKind,
        error: &anyhow::Error,
        attempts: usize,
        input: Option<I>,
        raw: Option<RawPayload>,
    ) -> Self {
        Self {
            subscription,
            failure,
            error: format!("{error:#}"),
            attempts,
            input,
            raw,
        }
    }
}
