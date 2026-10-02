//! Envelope published to a dead-letter sink, and the failure details broker adapters
//! forward with the original payload.
use crate::error_policy::FailureKind;
use anyhow::Context;
use serde::Serialize;
use std::str::FromStr;

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
    /// Which routable failure produced the dead letter.
    pub failure: FailureKind,
    /// Error message including its context chain.
    pub error: String,
    /// Handler attempts made before the failure; zero for decode failures.
    pub attempts: usize,
    /// Decoded handler input; `None` after a decode failure.
    pub input: Option<I>,
    /// Undecoded delivery as received from the source.
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

    /// Replaces the raw delivery, keeping the failure context. A dead-letter conversion uses
    /// it to read a type-erased raw form as its concrete type, such as the upstream Kafka
    /// record a [`ChannelRaw`](crate::ChannelRaw) holds.
    pub fn try_map_raw<T, E>(
        self,
        map: impl FnOnce(R) -> Result<T, E>,
    ) -> Result<DeadLetter<I, T>, E> {
        Ok(DeadLetter {
            subscription: self.subscription,
            failure: self.failure,
            error: self.error,
            attempts: self.attempts,
            input: self.input,
            raw: map(self.raw)?,
        })
    }
}

/// Prefix of the header or property names that broker adapters write next to a dead letter's
/// original payload.
///
/// Header inheritance middleware skips names with this prefix, so a subscription that reads
/// a dead-letter topic does not copy failure details into its outputs.
pub const DEAD_LETTER_HEADER_PREFIX: &str = "beavers-dlq-";

const SUBSCRIPTION: &str = "beavers-dlq-subscription";
const FAILURE: &str = "beavers-dlq-failure";
const ERROR: &str = "beavers-dlq-error";
const ATTEMPTS: &str = "beavers-dlq-attempts";
const COUNT: &str = "beavers-dlq-count";

/// Failure details of a dead letter that a broker adapter forwards with its original payload,
/// stored as text headers or properties.
///
/// Adapters pair it with the broker location where the payload was first received, such as
/// `KafkaDeadLetter` and `PulsarDeadLetter`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DeadLetterDetails {
    /// Name of the subscription that dead-lettered the payload most recently.
    pub subscription: String,
    /// Which routable failure dead-lettered the payload most recently.
    pub failure: FailureKind,
    /// Error message including its context chain.
    pub error: String,
    /// Handler attempts made before the most recent failure.
    pub attempts: usize,
    /// How many times the payload has been dead-lettered, including the most recent time.
    pub count: u32,
}

#[cfg_attr(
    not(any(
        feature = "kafka",
        feature = "pulsar",
        feature = "rabbitmq",
        feature = "sqs"
    )),
    allow(dead_code)
)]
impl DeadLetterDetails {
    /// Details for `dead_letter`, counting one more than a `previous` dead-lettering of the
    /// same payload.
    pub(crate) fn of<I, R>(dead_letter: &DeadLetter<I, R>, previous: Option<&Self>) -> Self {
        Self {
            subscription: dead_letter.subscription.clone(),
            failure: dead_letter.failure,
            error: dead_letter.error.clone(),
            attempts: dead_letter.attempts,
            count: previous.map_or(1, |previous| previous.count.saturating_add(1)),
        }
    }

    /// Header names and text values.
    pub(crate) fn fields(&self) -> [(&'static str, String); 5] {
        [
            (SUBSCRIPTION, self.subscription.clone()),
            (FAILURE, self.failure.as_str().to_owned()),
            (ERROR, self.error.clone()),
            (ATTEMPTS, self.attempts.to_string()),
            (COUNT, self.count.to_string()),
        ]
    }

    /// Reads details through `field`, which returns a header value by name. Returns `None`
    /// when the failure header is absent.
    pub(crate) fn parse<'a>(
        field: impl Fn(&str) -> anyhow::Result<Option<&'a str>>,
    ) -> anyhow::Result<Option<Self>> {
        let Some(failure) = field(FAILURE)? else {
            return Ok(None);
        };
        let required = |name: &str| {
            field(name)?.ok_or_else(|| anyhow::anyhow!("dead-letter header {name} is missing"))
        };
        Ok(Some(Self {
            subscription: required(SUBSCRIPTION)?.to_owned(),
            failure: failure.parse()?,
            error: required(ERROR)?.to_owned(),
            attempts: parse_number(ATTEMPTS, required(ATTEMPTS)?)?,
            count: parse_number(COUNT, required(COUNT)?)?,
        }))
    }
}

/// Parses a numeric dead-letter header.
#[cfg_attr(
    not(any(
        feature = "kafka",
        feature = "pulsar",
        feature = "rabbitmq",
        feature = "sqs"
    )),
    allow(dead_code)
)]
pub(crate) fn parse_number<T>(name: &str, value: &str) -> anyhow::Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    value
        .parse()
        .with_context(|| format!("dead-letter header {name} is not a number: {value:?}"))
}
