use super::record::{PulsarPublish, PulsarRecord};
use crate::{
    handler::{HandlerError, Result},
    tombstone::{TombstonePublish, TombstoneRecord},
};

impl<T> TombstoneRecord for PulsarRecord<T> {
    fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }
}

impl<I, O> TombstonePublish<PulsarRecord<I>> for PulsarPublish<O> {
    fn tombstone(input: &PulsarRecord<I>) -> Result<Self> {
        let key = input.key.clone().ok_or_else(|| {
            HandlerError::Reject(anyhow::anyhow!("Pulsar tombstone has no key to propagate"))
        })?;
        Ok(Self::tombstone(key))
    }
}
