//! Dead letters forwarded to a RabbitMQ exchange with their original message.
use super::record::{RabbitMqPublish, RabbitMqRecord, RabbitMqValue};
use crate::dead_letter::{DEAD_LETTER_HEADER_PREFIX, DeadLetter, DeadLetterDetails};

const ORIGIN_QUEUE: &str = "beavers-dlq-origin-queue";
const ORIGIN_EXCHANGE: &str = "beavers-dlq-origin-exchange";
const ORIGIN_ROUTING_KEY: &str = "beavers-dlq-origin-routing-key";

/// Where a dead-lettered payload was first received.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RabbitMqOrigin {
    pub queue: String,
    /// The exchange the payload was published to; empty for the default exchange.
    pub exchange: String,
    pub routing_key: String,
}

/// Failure details and origin that [`RabbitMqPublish::from_dead_letter`] writes as headers.
///
/// Read them from a record received from a dead-letter queue with
/// [`from_record`](Self::from_record), for example to select which dead letters a
/// subscription reprocesses or to stop after a number of dead-letterings.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RabbitMqDeadLetter {
    pub details: DeadLetterDetails,
    pub origin: RabbitMqOrigin,
}

impl RabbitMqDeadLetter {
    /// Reads dead-letter headers. Returns `None` for a record without them and an error when
    /// they are incomplete, malformed, or not strings.
    pub fn from_record<T>(record: &RabbitMqRecord<T>) -> anyhow::Result<Option<Self>> {
        let field = |name: &str| match record.headers.get(name) {
            None => Ok(None),
            Some(value) => value
                .as_str()
                .map(Some)
                .ok_or_else(|| anyhow::anyhow!("dead-letter header {name} is not a string")),
        };
        let Some(details) = DeadLetterDetails::parse(field)? else {
            return Ok(None);
        };
        let required = |name: &str| {
            field(name)?
                .map(str::to_owned)
                .ok_or_else(|| anyhow::anyhow!("dead-letter header {name} is missing"))
        };
        Ok(Some(Self {
            details,
            origin: RabbitMqOrigin {
                queue: required(ORIGIN_QUEUE)?,
                exchange: required(ORIGIN_EXCHANGE)?,
                routing_key: required(ORIGIN_ROUTING_KEY)?,
            },
        }))
    }
}

impl RabbitMqPublish<Vec<u8>> {
    /// Converts a dead letter into a RabbitMQ message that carries the original body,
    /// headers, and properties, plus failure-detail string headers named with
    /// [`DEAD_LETTER_HEADER_PREFIX`].
    ///
    /// Pass it to [`Subscription::dlq_with`](crate::Subscription::dlq_with) with a sink that
    /// publishes raw bytes; the sink's routing key applies. When the original message
    /// already carries dead-letter headers, because it was received from a dead-letter
    /// queue, its origin is kept and its count is incremented. Malformed dead-letter headers
    /// are replaced as if the message had never been dead-lettered.
    pub fn from_dead_letter<I>(dead_letter: DeadLetter<I, RabbitMqRecord<Vec<u8>>>) -> Self {
        let previous = RabbitMqDeadLetter::from_record(&dead_letter.raw)
            .ok()
            .flatten();
        let details = DeadLetterDetails::of(&dead_letter, previous.as_ref().map(|p| &p.details));
        let RabbitMqRecord {
            value,
            headers,
            properties,
            metadata,
        } = dead_letter.raw;
        let origin = previous.map_or(
            RabbitMqOrigin {
                queue: metadata.queue,
                exchange: metadata.exchange,
                routing_key: metadata.routing_key,
            },
            |previous| previous.origin,
        );
        let mut headers: super::RabbitMqHeaders = headers
            .into_iter()
            .filter(|(name, _)| !name.starts_with(DEAD_LETTER_HEADER_PREFIX))
            .collect();
        let origin_fields = [
            (ORIGIN_QUEUE, origin.queue),
            (ORIGIN_EXCHANGE, origin.exchange),
            (ORIGIN_ROUTING_KEY, origin.routing_key),
        ];
        headers.extend(
            details
                .fields()
                .into_iter()
                .chain(origin_fields)
                .map(|(name, value)| (name.to_owned(), RabbitMqValue::String(value))),
        );
        Self {
            value,
            routing_key: None,
            headers,
            properties,
        }
    }
}
