use super::record::{PubSubPublish, PubSubRecord};
use crate::{
    dead_letter::DEAD_LETTER_HEADER_PREFIX,
    forward::{SamePlatform, ValueRecord},
    handler::Result,
    middleware::Middleware,
};

/// Pub/Sub-to-Pub/Sub middleware that copies the received message attributes,
/// and optionally the ordering key, into each output message.
///
/// An attribute is added only when the output does not set that name.
/// [Dead-letter attributes](crate::DEAD_LETTER_HEADER_PREFIX) are not inherited.
/// The ordering key is inherited only when enabled with
/// [`with_ordering_key`](Self::with_ordering_key) and the output leaves it
/// `None`. The message ID, publish time, and delivery attempt are never
/// inherited.
#[derive(Clone, Copy, Debug)]
pub struct PubSubInherit {
    attributes: bool,
    ordering_key: bool,
}

impl PubSubInherit {
    /// Inherits message attributes.
    pub fn new() -> Self {
        Self {
            attributes: true,
            ordering_key: false,
        }
    }

    /// Does not inherit message attributes.
    pub fn without_attributes(mut self) -> Self {
        self.attributes = false;
        self
    }

    /// Also inherits the ordering key of the received message.
    pub fn with_ordering_key(mut self) -> Self {
        self.ordering_key = true;
        self
    }
}

impl Default for PubSubInherit {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, O> Middleware<PubSubRecord<I>, PubSubPublish<O>> for PubSubInherit
where
    I: Send + Sync + 'static,
    O: Send + 'static,
{
    async fn post_handler(
        &self,
        input: &PubSubRecord<I>,
        mut output: PubSubPublish<O>,
    ) -> Result<PubSubPublish<O>> {
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
        if self.ordering_key && output.ordering_key.is_none() {
            output.ordering_key.clone_from(&input.ordering_key);
        }
        Ok(output)
    }
}

impl<T: Clone + Send + Sync + 'static> ValueRecord for PubSubRecord<T> {
    type Value = T;

    fn value(&self) -> Result<T> {
        Ok(self.value.clone())
    }
}

impl<T: Clone + Send + Sync + 'static, U: Send + Sync + 'static> SamePlatform<U>
    for PubSubRecord<T>
{
    type Publish = PubSubPublish<U>;
    type Inherit = PubSubInherit;

    fn publish(value: U) -> PubSubPublish<U> {
        PubSubPublish::new(value)
    }
}
