use super::record::{KafkaPublish, KafkaRecord};
use crate::{
    handler::{HandlerError, Result},
    tombstone::{TombstonePublish, TombstoneRecord},
};

impl<T> TombstoneRecord for KafkaRecord<T> {
    fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }
}

impl<I, O> TombstonePublish<KafkaRecord<I>> for KafkaPublish<O> {
    fn tombstone(input: &KafkaRecord<I>) -> Result<Self> {
        let key = input.key.clone().ok_or_else(|| {
            HandlerError::Reject(anyhow::anyhow!("Kafka tombstone has no key to propagate"))
        })?;
        Ok(Self::tombstone(key))
    }
}
