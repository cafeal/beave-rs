use super::record::{RabbitMqPublish, RabbitMqRecord};
use crate::{
    dead_letter::DEAD_LETTER_HEADER_PREFIX,
    forward::{SamePlatform, ValueRecord},
    handler::Result,
    middleware::Middleware,
};

/// RabbitMQ-to-RabbitMQ middleware that copies the received headers and
/// descriptive properties into each output message.
///
/// Explicit output fields take precedence: a received header is added only when
/// the output does not set that name, and a property only when the output
/// leaves it `None`. [Dead-letter headers](crate::DEAD_LETTER_HEADER_PREFIX) are
/// not inherited. The inherited properties describe the payload or its
/// conversation: content type, content encoding, type, correlation ID, app ID,
/// and priority. The message ID, timestamp, expiration, reply-to address, and
/// user ID identify or address one message and are never inherited. Neither is
/// the routing key, so an output on the source's exchange cannot route back
/// to the queue it came from unless the sink is configured to.
#[derive(Clone, Copy, Debug)]
pub struct RabbitMqInherit {
    headers: bool,
    properties: bool,
}

impl RabbitMqInherit {
    /// Inherits headers and descriptive properties.
    pub fn new() -> Self {
        Self {
            headers: true,
            properties: true,
        }
    }

    pub fn without_headers(mut self) -> Self {
        self.headers = false;
        self
    }

    pub fn without_properties(mut self) -> Self {
        self.properties = false;
        self
    }
}

impl Default for RabbitMqInherit {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, O> Middleware<RabbitMqRecord<I>, RabbitMqPublish<O>> for RabbitMqInherit
where
    I: Send + Sync + 'static,
    O: Send + 'static,
{
    async fn post_handler(
        &self,
        input: &RabbitMqRecord<I>,
        mut output: RabbitMqPublish<O>,
    ) -> Result<RabbitMqPublish<O>> {
        if self.headers {
            let inherited = input
                .headers
                .iter()
                .filter(|(name, _)| !name.starts_with(DEAD_LETTER_HEADER_PREFIX));
            for (name, value) in inherited {
                output
                    .headers
                    .entry(name.clone())
                    .or_insert_with(|| value.clone());
            }
        }
        if self.properties {
            let received = &input.properties;
            let properties = &mut output.properties;
            let fields = [
                (&mut properties.content_type, &received.content_type),
                (&mut properties.content_encoding, &received.content_encoding),
                (&mut properties.kind, &received.kind),
                (&mut properties.correlation_id, &received.correlation_id),
                (&mut properties.app_id, &received.app_id),
            ];
            for (output, received) in fields {
                if output.is_none() {
                    output.clone_from(received);
                }
            }
            if properties.priority.is_none() {
                properties.priority = received.priority;
            }
        }
        Ok(output)
    }
}

impl<T: Clone + Send + Sync + 'static> ValueRecord for RabbitMqRecord<T> {
    type Value = T;

    fn value(&self) -> Result<T> {
        Ok(self.value.clone())
    }
}

impl<T: Clone + Send + Sync + 'static, U: Send + Sync + 'static> SamePlatform<U>
    for RabbitMqRecord<T>
{
    type Publish = RabbitMqPublish<U>;
    type Inherit = RabbitMqInherit;

    fn publish(value: U) -> RabbitMqPublish<U> {
        RabbitMqPublish::new(value)
    }
}
