use super::record::PulsarRecord;
use crate::tombstone::TombstoneRecord;

impl<T> TombstoneRecord for PulsarRecord<T> {
    fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }
}
