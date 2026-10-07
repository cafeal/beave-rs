//! Dead letters forwarded to an SQS queue with their original message.
use super::record::{SqsAttributeValue, SqsAttributes, SqsPublish, SqsRecord};
use crate::dead_letter::{DEAD_LETTER_HEADER_PREFIX, DeadLetter, DeadLetterDetails};
use crate::error::Error;
use std::collections::BTreeMap;

/// The one attribute that holds every failure detail, because SQS accepts at
/// most 10 attributes per message.
const DETAILS: &str = "beavers-dlq-details";
const ORIGIN_QUEUE_URL: &str = "beavers-dlq-origin-queue-url";
const ORIGIN_MESSAGE_ID: &str = "beavers-dlq-origin-message-id";

/// Where a dead-lettered payload was first received.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SqsOrigin {
    /// The queue the payload was first received from.
    pub queue_url: String,
    /// The message ID of that first receipt.
    pub message_id: String,
}

/// Failure details and origin that [`SqsPublish::from_dead_letter`] writes into the
/// `beavers-dlq-details` attribute.
///
/// Read them from a record received from a dead-letter queue with
/// [`from_record`](Self::from_record), for example to select which dead letters a
/// subscription reprocesses or to stop after a number of dead-letterings.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SqsDeadLetter {
    /// The latest failure and how often the payload was dead-lettered.
    pub details: DeadLetterDetails,
    /// Where the payload was first received.
    pub origin: SqsOrigin,
}

impl SqsDeadLetter {
    /// Reads the details attribute. Returns `None` for a record without it and an error
    /// when it is not a JSON object of strings or lacks a detail.
    pub fn from_record<T>(record: &SqsRecord<T>) -> Result<Option<Self>, Error> {
        let Some(value) = record.attributes.get(DETAILS) else {
            return Ok(None);
        };
        let text = value.as_str().ok_or_else(|| {
            Error::invalid_record(format!("dead-letter attribute {DETAILS} is not a string"))
        })?;
        let fields: BTreeMap<String, String> = serde_json::from_str(text).map_err(|error| {
            Error::invalid_record(format!(
                "dead-letter attribute {DETAILS} is malformed: {error}"
            ))
        })?;
        let field = |name: &str| Ok(fields.get(name).map(String::as_str));
        let details = DeadLetterDetails::parse(field)?.ok_or_else(|| {
            Error::invalid_record(format!("dead-letter attribute {DETAILS} has no failure"))
        })?;
        let required = |name: &str| {
            fields.get(name).cloned().ok_or_else(|| {
                Error::invalid_record(format!("dead-letter detail {name} is missing"))
            })
        };
        Ok(Some(Self {
            details,
            origin: SqsOrigin {
                queue_url: required(ORIGIN_QUEUE_URL)?,
                message_id: required(ORIGIN_MESSAGE_ID)?,
            },
        }))
    }
}

impl SqsPublish<Vec<u8>> {
    /// Converts a dead letter into an SQS message that carries the original body and
    /// attributes, plus a `beavers-dlq-details` attribute: a JSON object of the failure
    /// details and origin, keyed by the names other adapters use for headers.
    ///
    /// Pass it to [`Subscription::dlq_with`](crate::Subscription::dlq_with) with a sink that
    /// sends raw bytes. When the original message already carries the attribute, because
    /// it was received from a dead-letter queue, its origin is kept and its count is
    /// incremented; a malformed one is replaced as if the message had never been
    /// dead-lettered. A message that already has 10 attributes cannot take another, and
    /// SQS rejects it. The FIFO message group is kept, so a FIFO dead-letter queue
    /// accepts it.
    pub fn from_dead_letter<I>(dead_letter: DeadLetter<I, SqsRecord<Vec<u8>>>) -> Self {
        let previous = SqsDeadLetter::from_record(&dead_letter.raw).ok().flatten();
        let details = DeadLetterDetails::of(&dead_letter, previous.as_ref().map(|p| &p.details));
        let SqsRecord {
            value,
            attributes,
            metadata,
        } = dead_letter.raw;
        let origin = previous.map_or(
            SqsOrigin {
                queue_url: metadata.queue_url,
                message_id: metadata.message_id,
            },
            |previous| previous.origin,
        );
        let fields: BTreeMap<&str, String> = details
            .fields()
            .into_iter()
            .chain([
                (ORIGIN_QUEUE_URL, origin.queue_url),
                (ORIGIN_MESSAGE_ID, origin.message_id),
            ])
            .collect();
        let mut attributes: SqsAttributes = attributes
            .into_iter()
            .filter(|(name, _)| !name.starts_with(DEAD_LETTER_HEADER_PREFIX))
            .collect();
        attributes.insert(
            DETAILS.to_owned(),
            SqsAttributeValue::String(
                serde_json::to_string(&fields).expect("string maps serialize"),
            ),
        );
        Self {
            value,
            attributes,
            message_group_id: metadata.message_group_id,
            deduplication_id: None,
            delay: None,
        }
    }
}
