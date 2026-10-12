use super::record::{PulsarPublish, PulsarRecord};
use crate::{
    error::Error,
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
            HandlerError::Reject(
                Error::invalid_record("Pulsar tombstone has no key to propagate").into(),
            )
        })?;
        Ok(Self::tombstone(key))
    }
}
