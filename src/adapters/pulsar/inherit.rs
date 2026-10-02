use super::record::{PulsarPublish, PulsarRecord};
use crate::{
    dead_letter::DEAD_LETTER_HEADER_PREFIX,
    forward::{SamePlatform, ValueRecord},
    handler::{HandlerError, Result},
    middleware::Middleware,
};

/// Pulsar-to-Pulsar middleware that copies the received key, properties, and
/// event time into each output message.
///
/// Explicit output fields take precedence. The key and event time are
/// inherited only when the output leaves them `None`, and a received property
/// is inserted only when the output does not already set that name.
/// [Dead-letter properties](crate::DEAD_LETTER_HEADER_PREFIX) are not inherited. Topic,
/// message ID, and publish time are never inherited, and no ordering key is
/// derived from the input.
#[derive(Clone, Copy, Debug)]
pub struct PulsarInherit {
    key: bool,
    properties: bool,
    event_time: bool,
}

impl PulsarInherit {
    /// Inherits the key, properties, and event time.
    pub fn new() -> Self {
        Self {
            key: true,
            properties: true,
            event_time: true,
        }
    }

    /// Stops inheriting the received key, so outputs keep their own key.
    pub fn without_key(mut self) -> Self {
        self.key = false;
        self
    }

    /// Stops inheriting received properties, so outputs keep only their own
    /// properties.
    pub fn without_properties(mut self) -> Self {
        self.properties = false;
        self
    }

    /// Stops inheriting the received event time, so outputs keep their own.
    pub fn without_event_time(mut self) -> Self {
        self.event_time = false;
        self
    }
}

impl Default for PulsarInherit {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, O> Middleware<PulsarRecord<I>, PulsarPublish<O>> for PulsarInherit
where
    I: Send + Sync + 'static,
    O: Send + 'static,
{
    async fn post_handler(
        &self,
        input: &PulsarRecord<I>,
        mut output: PulsarPublish<O>,
    ) -> Result<PulsarPublish<O>> {
        if self.key && output.key.is_none() {
            output.key.clone_from(&input.key);
        }
        if self.properties {
            let inherited = input
                .properties
                .iter()
                .filter(|(name, _)| !name.starts_with(DEAD_LETTER_HEADER_PREFIX));
            for (name, value) in inherited {
                output
                    .properties
                    .entry(name.clone())
                    .or_insert_with(|| value.clone());
            }
        }
        if self.event_time && output.event_time.is_none() {
            output.event_time = input.event_time;
        }
        Ok(output)
    }
}

impl<T: Clone + Send + Sync + 'static> ValueRecord for PulsarRecord<T> {
    type Value = T;

    /// A null value is rejected; register `Tombstones` to choose another
    /// policy before the handler runs.
    fn value(&self) -> Result<T> {
        self.value
            .clone()
            .ok_or_else(|| HandlerError::Reject(anyhow::anyhow!("Pulsar record has a null value")))
    }
}

impl<T: Clone + Send + Sync + 'static, U: Send + Sync + 'static> SamePlatform<U>
    for PulsarRecord<T>
{
    type Publish = PulsarPublish<U>;
    type Inherit = PulsarInherit;

    fn publish(value: U) -> PulsarPublish<U> {
        PulsarPublish::new(value)
    }
}
