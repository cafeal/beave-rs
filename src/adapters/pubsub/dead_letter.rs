//! Dead letters forwarded to a Pub/Sub topic with their original message.
use super::{
    record::{PubSubAttributes, PubSubPublish, PubSubRecord},
    sink::MAX_ATTRIBUTE_VALUE,
};
use crate::{
    dead_letter::{DEAD_LETTER_HEADER_PREFIX, DeadLetter, DeadLetterDetails},
    error::Error,
};

const ORIGIN_SUBSCRIPTION: &str = "beavers-dlq-origin-subscription";
const ORIGIN_MESSAGE_ID: &str = "beavers-dlq-origin-message-id";

/// Where a dead-lettered payload was first received.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct PubSubOrigin {
    /// The subscription the payload was first received from.
    pub subscription: String,
    /// The message ID of that first receipt.
    pub message_id: String,
}

/// Failure details and origin that [`PubSubPublish::from_dead_letter`] writes as attributes.
///
/// Read them from a record received from a dead-letter topic with
/// [`from_record`](Self::from_record), for example to select which dead letters a
/// subscription reprocesses or to stop after a number of dead-letterings.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct PubSubDeadLetter {
    /// The latest failure and how often the payload was dead-lettered.
    pub details: DeadLetterDetails,
    /// Where the payload was first received.
    pub origin: PubSubOrigin,
}

impl PubSubDeadLetter {
    /// Reads dead-letter attributes. Returns `None` for a record without them and an error
    /// when they are incomplete or malformed.
    pub fn from_record<T>(record: &PubSubRecord<T>) -> Result<Option<Self>, Error> {
        let field = |name: &str| Ok(record.attributes.get(name).map(String::as_str));
        let Some(details) = DeadLetterDetails::parse(field)? else {
            return Ok(None);
        };
        let required = |name: &str| {
            record.attributes.get(name).cloned().ok_or_else(|| {
                Error::invalid_record(format!("dead-letter attribute {name} is missing"))
            })
        };
        Ok(Some(Self {
            details,
            origin: PubSubOrigin {
                subscription: required(ORIGIN_SUBSCRIPTION)?,
                message_id: required(ORIGIN_MESSAGE_ID)?,
            },
        }))
    }
}

impl PubSubPublish<Vec<u8>> {
    /// Converts a dead letter into a Pub/Sub message that carries the original data,
    /// attributes, and ordering key, plus failure-detail attributes named with
    /// [`DEAD_LETTER_HEADER_PREFIX`].
    ///
    /// Pass it to [`Subscription::dlq_with`](crate::Subscription::dlq_with) with a sink that
    /// publishes raw bytes. When the original message already carries dead-letter
    /// attributes, because it was received from a dead-letter topic, its origin is kept and
    /// its count is incremented; malformed ones are replaced as if the message had never
    /// been dead-lettered. Pub/Sub limits attribute values to 1024 bytes, so a longer error
    /// message is cut at a character boundary.
    pub fn from_dead_letter<I>(dead_letter: DeadLetter<I, PubSubRecord<Vec<u8>>>) -> Self {
        let previous = PubSubDeadLetter::from_record(&dead_letter.raw)
            .ok()
            .flatten();
        let details = DeadLetterDetails::of(&dead_letter, previous.as_ref().map(|p| &p.details));
        let PubSubRecord {
            value,
            attributes,
            ordering_key,
            metadata,
        } = dead_letter.raw;
        let origin = previous.map_or(
            PubSubOrigin {
                subscription: metadata.subscription,
                message_id: metadata.message_id,
            },
            |previous| previous.origin,
        );
        let mut attributes: PubSubAttributes = attributes
            .into_iter()
            .filter(|(name, _)| !name.starts_with(DEAD_LETTER_HEADER_PREFIX))
            .collect();
        let fields = details.fields().into_iter().chain([
            (ORIGIN_SUBSCRIPTION, origin.subscription),
            (ORIGIN_MESSAGE_ID, origin.message_id),
        ]);
        for (name, mut value) in fields {
            truncate(&mut value, MAX_ATTRIBUTE_VALUE);
            attributes.insert(name.to_owned(), value);
        }
        Self {
            value,
            attributes,
            ordering_key,
        }
    }
}

/// Cuts `text` to at most `max` bytes at a character boundary.
fn truncate(text: &mut String, max: usize) {
    text.truncate(text.floor_char_boundary(max));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncation_keeps_whole_characters() {
        let mut text = "éa".repeat(400);
        truncate(&mut text, 1024);
        assert_eq!(text.len(), 1023);
        let mut short = "short".to_owned();
        truncate(&mut short, 1024);
        assert_eq!(short, "short");
    }
}
