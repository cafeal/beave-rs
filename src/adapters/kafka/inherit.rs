use super::record::{KafkaPublish, KafkaRecord};
use crate::{
    forward::{SamePlatform, ValueRecord},
    handler::{HandlerError, Result},
    middleware::Middleware,
};

/// Kafka-to-Kafka middleware that copies the received key and headers into
/// each output record.
///
/// Explicit output fields take precedence. A key is inherited only when the
/// output key is `None`. Received headers are placed before the output's own
/// headers, except those whose name the output already sets. Topic,
/// partition, offset, and timestamp are never inherited: the sink publishes to
/// its configured topic and Kafka chooses the partition and timestamp.
///
/// A `None` output key cannot be distinguished from an unset key. Use
/// [`KafkaInherit::without_key`] to publish keyless records.
#[derive(Clone, Copy, Debug)]
pub struct KafkaInherit {
    key: bool,
    headers: bool,
}

impl KafkaInherit {
    /// Inherits the key and headers.
    pub fn new() -> Self {
        Self {
            key: true,
            headers: true,
        }
    }

    pub fn without_key(mut self) -> Self {
        self.key = false;
        self
    }

    pub fn without_headers(mut self) -> Self {
        self.headers = false;
        self
    }
}

impl Default for KafkaInherit {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, O> Middleware<KafkaRecord<I>, KafkaPublish<O>> for KafkaInherit
where
    I: Send + Sync + 'static,
    O: Send + 'static,
{
    async fn post_handler(
        &self,
        input: &KafkaRecord<I>,
        mut output: KafkaPublish<O>,
    ) -> Result<KafkaPublish<O>> {
        if self.key && output.key.is_none() {
            output.key.clone_from(&input.key);
        }
        if self.headers {
            let mut headers: Vec<_> = input
                .headers
                .iter()
                .filter(|(name, _)| output.headers.iter().all(|(explicit, _)| explicit != name))
                .cloned()
                .collect();
            headers.append(&mut output.headers);
            output.headers = headers;
        }
        Ok(output)
    }
}

impl<T: Clone + Send + Sync + 'static> ValueRecord for KafkaRecord<T> {
    type Value = T;

    /// A Kafka null value is rejected; register `Tombstones` to choose
    /// another policy before the handler runs.
    fn value(&self) -> Result<T> {
        self.value
            .clone()
            .ok_or_else(|| HandlerError::Reject(anyhow::anyhow!("Kafka record has a null value")))
    }
}

impl<T: Clone + Send + Sync + 'static, U: Send + Sync + 'static> SamePlatform<U>
    for KafkaRecord<T>
{
    type Publish = KafkaPublish<U>;
    type Inherit = KafkaInherit;

    fn publish(value: U) -> KafkaPublish<U> {
        KafkaPublish::new(value)
    }
}
