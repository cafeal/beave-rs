//! Dead letters forwarded to a Pulsar topic with their original message.
use super::record::{PulsarMessageId, PulsarMetadata, PulsarPublish, PulsarRecord};
use crate::dead_letter::{DEAD_LETTER_HEADER_PREFIX, DeadLetter, DeadLetterDetails, parse_number};
use crate::error::Error;
use std::collections::HashMap;

const ORIGIN_TOPIC: &str = "beavers-dlq-origin-topic";
const ORIGIN_MESSAGE_ID: &str = "beavers-dlq-origin-message-id";
const ORIGIN_PUBLISH_TIME: &str = "beavers-dlq-origin-publish-time";

/// Failure details and origin that [`PulsarPublish::from_dead_letter`] writes as properties.
///
/// Read them from a message received from a dead-letter topic with
/// [`from_record`](Self::from_record), for example to select which dead letters a
/// subscription reprocesses or to stop after a number of dead-letterings.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct PulsarDeadLetter {
    /// Failure details of the most recent dead-lettering.
    pub details: DeadLetterDetails,
    /// Where the payload was received before it was first dead-lettered. The publish time is
    /// that message's; the dead-letter message's own publish time is when it was
    /// dead-lettered.
    pub origin: PulsarMetadata,
}

impl PulsarDeadLetter {
    /// Reads dead-letter properties. Returns `None` for a message without them and an error
    /// when they are incomplete or malformed.
    pub fn from_record<T>(record: &PulsarRecord<T>) -> Result<Option<Self>, Error> {
        let field = |name: &str| Ok(record.properties.get(name).map(String::as_str));
        let Some(details) = DeadLetterDetails::parse(field)? else {
            return Ok(None);
        };
        let required = |name: &str| {
            record
                .properties
                .get(name)
                .map(String::as_str)
                .ok_or_else(|| {
                    Error::invalid_record(format!("dead-letter property {name} is missing"))
                })
        };
        Ok(Some(Self {
            details,
            origin: PulsarMetadata {
                topic: required(ORIGIN_TOPIC)?.to_owned(),
                message_id: parse_message_id(required(ORIGIN_MESSAGE_ID)?)?,
                publish_time: parse_number(ORIGIN_PUBLISH_TIME, required(ORIGIN_PUBLISH_TIME)?)?,
            },
        }))
    }
}

impl PulsarPublish<Vec<u8>> {
    /// Converts a dead letter into a Pulsar message that carries the original key, value,
    /// properties, and event time, plus failure-detail properties named with
    /// [`DEAD_LETTER_HEADER_PREFIX`].
    ///
    /// Pass it to [`Subscription::dlq_with`](crate::Subscription::dlq_with) with a sink that
    /// publishes raw bytes. When the original message already carries dead-letter
    /// properties, because it was received from a dead-letter topic, its origin is kept and
    /// its count is incremented. Malformed dead-letter properties are replaced as if the
    /// message had never been dead-lettered. A null value stays null.
    pub fn from_dead_letter<I>(dead_letter: DeadLetter<I, PulsarRecord<Vec<u8>>>) -> Self {
        let previous = PulsarDeadLetter::from_record(&dead_letter.raw)
            .ok()
            .flatten();
        let details = DeadLetterDetails::of(&dead_letter, previous.as_ref().map(|p| &p.details));
        let PulsarRecord {
            value,
            key,
            properties,
            event_time,
            metadata,
        } = dead_letter.raw;
        let origin = previous.map_or(metadata, |previous| previous.origin);
        let mut properties: HashMap<_, _> = properties
            .into_iter()
            .filter(|(name, _)| !name.starts_with(DEAD_LETTER_HEADER_PREFIX))
            .collect();
        let id = origin.message_id;
        let origin_fields = [
            (ORIGIN_TOPIC, origin.topic),
            (
                ORIGIN_MESSAGE_ID,
                format!(
                    "{}:{}:{}:{}",
                    id.ledger_id, id.entry_id, id.partition, id.batch_index
                ),
            ),
            (ORIGIN_PUBLISH_TIME, origin.publish_time.to_string()),
        ];
        properties.extend(
            details
                .fields()
                .into_iter()
                .chain(origin_fields)
                .map(|(name, value)| (name.to_owned(), value)),
        );
        Self {
            value,
            properties,
            key,
            ordering_key: None,
            event_time,
        }
    }
}

/// Parses `ledger:entry:partition:batch`.
fn parse_message_id(value: &str) -> Result<PulsarMessageId, Error> {
    let parts: Vec<_> = value.split(':').collect();
    let [ledger_id, entry_id, partition, batch_index] = parts[..] else {
        return Err(Error::invalid_record(format!(
            "dead-letter property {ORIGIN_MESSAGE_ID} is malformed: {value:?}"
        )));
    };
    Ok(PulsarMessageId {
        ledger_id: parse_number(ORIGIN_MESSAGE_ID, ledger_id)?,
        entry_id: parse_number(ORIGIN_MESSAGE_ID, entry_id)?,
        partition: parse_number(ORIGIN_MESSAGE_ID, partition)?,
        batch_index: parse_number(ORIGIN_MESSAGE_ID, batch_index)?,
    })
}
