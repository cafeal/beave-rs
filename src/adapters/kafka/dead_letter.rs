//! Dead letters forwarded to a Kafka topic with their original record.
use super::record::{KafkaMetadata, KafkaPublish, KafkaRecord};
use crate::dead_letter::{DEAD_LETTER_HEADER_PREFIX, DeadLetter, DeadLetterDetails, parse_number};

const ORIGIN_TOPIC: &str = "beavers-dlq-origin-topic";
const ORIGIN_PARTITION: &str = "beavers-dlq-origin-partition";
const ORIGIN_OFFSET: &str = "beavers-dlq-origin-offset";
const ORIGIN_TIMESTAMP: &str = "beavers-dlq-origin-timestamp";

/// Failure details and origin that [`KafkaPublish::from_dead_letter`] writes as headers.
///
/// Read them from a record received from a dead-letter topic with
/// [`from_record`](Self::from_record), for example to select which dead letters a
/// subscription reprocesses or to stop after a number of dead-letterings.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct KafkaDeadLetter {
    pub details: DeadLetterDetails,
    /// Where the payload was received before it was first dead-lettered. The timestamp is
    /// that record's timestamp; the dead-letter record's own timestamp is when it was
    /// dead-lettered.
    pub origin: KafkaMetadata,
}

impl KafkaDeadLetter {
    /// Reads dead-letter headers. Returns `None` for a record without them and an error when
    /// they are incomplete or malformed. When a header repeats, the last value is used.
    pub fn from_record<T>(record: &KafkaRecord<T>) -> anyhow::Result<Option<Self>> {
        let field = |name: &str| header(&record.headers, name);
        let Some(details) = DeadLetterDetails::parse(field)? else {
            return Ok(None);
        };
        let required = |name: &str| {
            field(name)?.ok_or_else(|| anyhow::anyhow!("dead-letter header {name} is missing"))
        };
        Ok(Some(Self {
            details,
            origin: KafkaMetadata {
                topic: required(ORIGIN_TOPIC)?.to_owned(),
                partition: parse_number(ORIGIN_PARTITION, required(ORIGIN_PARTITION)?)?,
                offset: parse_number(ORIGIN_OFFSET, required(ORIGIN_OFFSET)?)?,
                timestamp: field(ORIGIN_TIMESTAMP)?
                    .map(|value| parse_number(ORIGIN_TIMESTAMP, value))
                    .transpose()?,
            },
        }))
    }
}

impl KafkaPublish<Vec<u8>> {
    /// Converts a dead letter into a Kafka record that carries the original key, value, and
    /// headers, followed by failure-detail headers named with
    /// [`DEAD_LETTER_HEADER_PREFIX`].
    ///
    /// Pass it to [`Subscription::dlq_with`](crate::Subscription::dlq_with) with a sink that
    /// publishes raw bytes. When the original record already carries dead-letter headers,
    /// because it was received from a dead-letter topic, its origin is kept and its count is
    /// incremented. Malformed dead-letter headers are replaced as if the record had never
    /// been dead-lettered. A null value stays null.
    pub fn from_dead_letter<I>(dead_letter: DeadLetter<I, KafkaRecord<Vec<u8>>>) -> Self {
        let previous = KafkaDeadLetter::from_record(&dead_letter.raw)
            .ok()
            .flatten();
        let details = DeadLetterDetails::of(&dead_letter, previous.as_ref().map(|p| &p.details));
        let KafkaRecord {
            key,
            value,
            headers,
            metadata,
        } = dead_letter.raw;
        let origin = previous.map_or(metadata, |previous| previous.origin);
        let mut headers: Vec<_> = headers
            .into_iter()
            .filter(|(name, _)| !name.starts_with(DEAD_LETTER_HEADER_PREFIX))
            .collect();
        let origin_fields = [
            (ORIGIN_TOPIC, Some(origin.topic)),
            (ORIGIN_PARTITION, Some(origin.partition.to_string())),
            (ORIGIN_OFFSET, Some(origin.offset.to_string())),
            (ORIGIN_TIMESTAMP, origin.timestamp.map(|t| t.to_string())),
        ];
        let fields = details
            .fields()
            .into_iter()
            .map(|(name, value)| (name, Some(value)))
            .chain(origin_fields);
        headers.extend(fields.filter_map(|(name, value)| {
            value.map(|value| (name.to_owned(), Some(value.into_bytes())))
        }));
        Self {
            key,
            value,
            headers,
        }
    }
}

/// The last UTF-8 value of a header.
fn header<'a>(
    headers: &'a [(String, Option<Vec<u8>>)],
    name: &str,
) -> anyhow::Result<Option<&'a str>> {
    let Some((_, value)) = headers.iter().rev().find(|(header, _)| header == name) else {
        return Ok(None);
    };
    let value = value
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("dead-letter header {name} has no value"))?;
    std::str::from_utf8(value)
        .map(Some)
        .map_err(|_| anyhow::anyhow!("dead-letter header {name} is not UTF-8"))
}
