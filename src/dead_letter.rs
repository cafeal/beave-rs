//! Envelope published to a dead-letter sink.
use crate::error_policy::FailureKind;
use serde::Serialize;

/// A delivery that could not be processed, with structured failure context.
///
/// Handlers never see this type; only dead-letter sinks receive it. `input` is the decoded
/// handler input when decoding succeeded. `raw` is the source's undecoded delivery form
/// ([`SourceMessage::Raw`](crate::message::SourceMessage::Raw)), such as a Kafka record with
/// its value bytes, key, headers, and delivery metadata.
#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct DeadLetter<I, R> {
    /// Name of the subscription that produced the dead letter.
    pub subscription: String,
    pub failure: FailureKind,
    /// Error message including its context chain.
    pub error: String,
    /// Handler attempts made before the failure; zero for decode failures.
    pub attempts: usize,
    pub input: Option<I>,
    pub raw: R,
}

impl<I, R> DeadLetter<I, R> {
    pub(crate) fn new(
        subscription: String,
        failure: FailureKind,
        error: &anyhow::Error,
        attempts: usize,
        input: Option<I>,
        raw: R,
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
