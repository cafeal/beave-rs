//! Dead letters forwarded to a Service Bus entity with their original message.
use super::record::{
    ServiceBusApplicationProperties, ServiceBusPublish, ServiceBusRecord, ServiceBusValue,
};
use crate::dead_letter::{DEAD_LETTER_HEADER_PREFIX, DeadLetter, DeadLetterDetails, parse_number};
use crate::error::Error;

const ORIGIN_ENTITY_PATH: &str = "beavers-dlq-origin-entity-path";
const ORIGIN_SEQUENCE_NUMBER: &str = "beavers-dlq-origin-sequence-number";

/// Where a dead-lettered payload was first received.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServiceBusOrigin {
    /// The queue or subscription the payload was received from.
    pub entity_path: String,
    /// The sequence number of that message in the entity.
    pub sequence_number: i64,
}

/// Failure details and origin that [`ServiceBusPublish::from_dead_letter`] writes as
/// application properties.
///
/// Read them from a record received from a dead-letter entity with
/// [`from_record`](Self::from_record), for example to select which dead letters a
/// subscription reprocesses or to stop after a number of dead-letterings.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServiceBusDeadLetter {
    /// Failure details of the most recent dead-lettering.
    pub details: DeadLetterDetails,
    /// Where the payload was received before it was first dead-lettered.
    pub origin: ServiceBusOrigin,
}

impl ServiceBusDeadLetter {
    /// Reads dead-letter application properties. Returns `None` for a record without them
    /// and an error when they are incomplete, malformed, or not strings.
    pub fn from_record<T>(record: &ServiceBusRecord<T>) -> Result<Option<Self>, Error> {
        let field = |name: &str| match record.application_properties.get(name) {
            None => Ok(None),
            Some(value) => value.as_str().map(Some).ok_or_else(|| {
                Error::invalid_record(format!(
                    "dead-letter application property {name} is not a string"
                ))
            }),
        };
        let Some(details) = DeadLetterDetails::parse(field)? else {
            return Ok(None);
        };
        let required = |name: &str| {
            field(name)?.ok_or_else(|| {
                Error::invalid_record(format!(
                    "dead-letter application property {name} is missing"
                ))
            })
        };
        Ok(Some(Self {
            details,
            origin: ServiceBusOrigin {
                entity_path: required(ORIGIN_ENTITY_PATH)?.to_owned(),
                sequence_number: parse_number(
                    ORIGIN_SEQUENCE_NUMBER,
                    required(ORIGIN_SEQUENCE_NUMBER)?,
                )?,
            },
        }))
    }
}

impl ServiceBusPublish<Vec<u8>> {
    /// Converts a dead letter into a Service Bus message that carries the original body,
    /// application properties, and message properties, plus failure-detail string
    /// application properties named with [`DEAD_LETTER_HEADER_PREFIX`].
    ///
    /// Pass it to [`Subscription::dlq_with`](crate::Subscription::dlq_with) with a sink that
    /// sends raw bytes. When the original message already carries dead-letter properties,
    /// because it was received from a dead-letter entity, its origin is kept and its count is
    /// incremented. Malformed dead-letter properties are replaced as if the message had never
    /// been dead-lettered. The message ID and time to live are cleared, so duplicate
    /// detection on the dead-letter entity does not drop a payload dead-lettered again and
    /// the dead letter does not expire with the original's time to live.
    pub fn from_dead_letter<I>(dead_letter: DeadLetter<I, ServiceBusRecord<Vec<u8>>>) -> Self {
        let previous = ServiceBusDeadLetter::from_record(&dead_letter.raw)
            .ok()
            .flatten();
        let details = DeadLetterDetails::of(&dead_letter, previous.as_ref().map(|p| &p.details));
        let ServiceBusRecord {
            value,
            application_properties,
            mut properties,
            metadata,
        } = dead_letter.raw;
        let origin = previous.map_or(
            ServiceBusOrigin {
                entity_path: metadata.entity_path,
                sequence_number: metadata.sequence_number,
            },
            |previous| previous.origin,
        );
        let mut application_properties: ServiceBusApplicationProperties = application_properties
            .into_iter()
            .filter(|(name, _)| !name.starts_with(DEAD_LETTER_HEADER_PREFIX))
            .collect();
        let origin_fields = [
            (ORIGIN_ENTITY_PATH, origin.entity_path),
            (ORIGIN_SEQUENCE_NUMBER, origin.sequence_number.to_string()),
        ];
        application_properties.extend(
            details
                .fields()
                .into_iter()
                .chain(origin_fields)
                .map(|(name, value)| (name.to_owned(), ServiceBusValue::String(value))),
        );
        properties.message_id = None;
        properties.time_to_live = None;
        Self {
            value,
            application_properties,
            properties,
            scheduled_enqueue_time: None,
        }
    }
}
