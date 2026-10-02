use super::record::{SqsPublish, SqsRecord};
use crate::{
    dead_letter::DEAD_LETTER_HEADER_PREFIX,
    forward::{SamePlatform, ValueRecord},
    handler::Result,
    middleware::Middleware,
};

/// SQS-to-SQS middleware that copies the received message attributes, and
/// optionally the FIFO message group, into each output message.
///
/// An attribute is added only when the output does not set that name.
/// [Dead-letter attributes](crate::DEAD_LETTER_HEADER_PREFIX) are not inherited.
/// The message group is inherited only when enabled with
/// [`with_message_group`](Self::with_message_group) and the output leaves it
/// `None`, for forwarding between FIFO queues. The message ID, deduplication
/// ID, and delay are never inherited.
#[derive(Clone, Copy, Debug)]
pub struct SqsInherit {
    attributes: bool,
    message_group: bool,
}

impl SqsInherit {
    /// Inherits message attributes.
    pub fn new() -> Self {
        Self {
            attributes: true,
            message_group: false,
        }
    }

    /// Does not inherit message attributes.
    pub fn without_attributes(mut self) -> Self {
        self.attributes = false;
        self
    }

    /// Also inherits the FIFO message group of the received message.
    pub fn with_message_group(mut self) -> Self {
        self.message_group = true;
        self
    }
}

impl Default for SqsInherit {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, O> Middleware<SqsRecord<I>, SqsPublish<O>> for SqsInherit
where
    I: Send + Sync + 'static,
    O: Send + 'static,
{
    async fn post_handler(
        &self,
        input: &SqsRecord<I>,
        mut output: SqsPublish<O>,
    ) -> Result<SqsPublish<O>> {
        if self.attributes {
            let inherited = input
                .attributes
                .iter()
                .filter(|(name, _)| !name.starts_with(DEAD_LETTER_HEADER_PREFIX));
            for (name, value) in inherited {
                output
                    .attributes
                    .entry(name.clone())
                    .or_insert_with(|| value.clone());
            }
        }
        if self.message_group && output.message_group_id.is_none() {
            output
                .message_group_id
                .clone_from(&input.metadata.message_group_id);
        }
        Ok(output)
    }
}

impl<T: Clone + Send + Sync + 'static> ValueRecord for SqsRecord<T> {
    type Value = T;

    fn value(&self) -> Result<T> {
        Ok(self.value.clone())
    }
}

impl<T: Clone + Send + Sync + 'static, U: Send + Sync + 'static> SamePlatform<U> for SqsRecord<T> {
    type Publish = SqsPublish<U>;
    type Inherit = SqsInherit;

    fn publish(value: U) -> SqsPublish<U> {
        SqsPublish::new(value)
    }
}
