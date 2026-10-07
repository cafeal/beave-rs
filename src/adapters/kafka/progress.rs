use crate::error::{BoxError, Error, ensure};
use crate::shutdown::CancellationToken;
use rdkafka::{
    ClientContext,
    consumer::{BaseConsumer, ConsumerContext, Rebalance, StreamConsumer},
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

pub(super) type Partition = (String, i32);
pub(super) type KafkaConsumer = StreamConsumer<Context>;

#[derive(Default)]
pub(super) struct Progress {
    active: HashMap<Partition, PartitionProgress>,
    generations: HashMap<Partition, u64>,
}

struct PartitionProgress {
    generation: u64,
    revoked: CancellationToken,
    next: Option<i64>,
    /// Registered offsets at or after `next`, mapped to whether their delivery
    /// completed. Offsets absent between registered ones were never delivered.
    received: BTreeMap<i64, bool>,
}

impl Progress {
    fn activate(&mut self, key: Partition) {
        self.active.entry(key.clone()).or_insert_with(|| {
            let generation = self.generations.entry(key).or_default();
            *generation = generation.wrapping_add(1);
            PartitionProgress {
                generation: *generation,
                revoked: CancellationToken::new(),
                next: None,
                received: BTreeMap::new(),
            }
        });
    }

    fn revoke(&mut self, key: &Partition) {
        if let Some(partition) = self.active.remove(key) {
            partition.revoked.cancel();
        }
        let generation = self.generations.entry(key.clone()).or_default();
        *generation = generation.wrapping_add(1);
    }

    pub(super) fn revoke_all(&mut self) {
        let keys = self.active.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            self.revoke(&key);
        }
    }

    /// Registers a received offset for the current assignment. Records from a
    /// partition that is not assigned are stale and return `None`.
    pub(super) fn register(
        &mut self,
        key: &Partition,
        offset: i64,
    ) -> Option<(u64, CancellationToken)> {
        let partition = self.active.get_mut(key)?;
        match partition.next {
            Some(next) if offset < next => {}
            Some(_) => {
                partition.received.entry(offset).or_insert(false);
            }
            None => {
                partition.next = Some(offset);
                partition.received.entry(offset).or_insert(false);
            }
        }
        Some((partition.generation, partition.revoked.clone()))
    }

    pub(super) fn complete(
        &mut self,
        generation: u64,
        key: &Partition,
        offset: i64,
    ) -> Result<Option<i64>, BoxError> {
        let partition = self
            .active
            .get_mut(key)
            .ok_or_else(|| Error::msg("Kafka delivery belongs to a revoked assignment"))?;
        ensure!(
            partition.generation == generation,
            Error::msg,
            "Kafka delivery belongs to an expired assignment"
        );
        let Some(next) = partition.next else {
            return Err(Error::msg("Kafka partition has no registered delivery").into());
        };
        if offset < next {
            return Ok(None);
        }
        let done = partition
            .received
            .get_mut(&offset)
            .ok_or_else(|| Error::msg("Kafka offset was never registered"))?;
        *done = true;
        // Kafka delivers a partition's records in offset order, so an offset
        // between two registered offsets was never delivered (for example, it
        // was compacted away or is a transaction marker). The commit position
        // is the first unfinished registered offset, or the offset after the
        // last registered one when all are finished.
        let candidate = partition
            .received
            .iter()
            .find_map(|(offset, done)| (!done).then_some(*offset))
            .or_else(|| {
                partition
                    .received
                    .last_key_value()
                    .map(|(offset, _)| offset + 1)
            })
            .unwrap_or(next);
        Ok((candidate > next).then_some(candidate))
    }

    /// Whether `generation` is the current assignment of the partition.
    pub(super) fn is_current(&self, generation: u64, key: &Partition) -> bool {
        self.active
            .get(key)
            .is_some_and(|partition| partition.generation == generation)
    }

    pub(super) fn committed(&mut self, generation: u64, key: &Partition, next: i64) {
        let Some(partition) = self.active.get_mut(key) else {
            return;
        };
        if partition.generation != generation
            || partition.next.is_none_or(|current| next <= current)
        {
            return;
        }
        partition.received.retain(|offset, _| *offset >= next);
        partition.next = Some(next);
    }
}

/// Held while a producer transaction checks the assignment and commits
/// offsets, so a revoke waits for that commit and a commit never starts for a
/// revoked assignment.
pub(super) type TransactionGate = Arc<Mutex<()>>;

pub(super) struct Context {
    pub(super) progress: Arc<Mutex<Progress>>,
    pub(super) transactions: TransactionGate,
}

impl ClientContext for Context {}

impl ConsumerContext for Context {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Revoke(partitions) = rebalance {
            let _transactions = self.transactions.lock().unwrap();
            let mut progress = self.progress.lock().unwrap();
            for partition in partitions.elements() {
                progress.revoke(&(partition.topic().to_owned(), partition.partition()));
            }
        }
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Assign(partitions) = rebalance {
            let mut progress = self.progress.lock().unwrap();
            for partition in partitions.elements() {
                progress.activate((partition.topic().to_owned(), partition.partition()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> Partition {
        ("orders".to_owned(), 0)
    }

    fn assigned(keys: impl IntoIterator<Item = Partition>) -> Progress {
        let mut progress = Progress::default();
        for key in keys {
            progress.activate(key);
        }
        progress
    }

    fn register(progress: &mut Progress, key: &Partition, offset: i64) -> u64 {
        progress
            .register(key, offset)
            .expect("assigned partition")
            .0
    }

    #[test]
    fn completion_commits_up_to_the_first_unfinished_registered_offset() {
        let key = key();
        let mut progress = assigned([key.clone()]);
        let generation = register(&mut progress, &key, 7);
        register(&mut progress, &key, 8);
        register(&mut progress, &key, 9);

        assert_eq!(progress.complete(generation, &key, 9).unwrap(), None);
        assert_eq!(progress.complete(generation, &key, 7).unwrap(), Some(8));
        progress.committed(generation, &key, 8);
        assert_eq!(progress.complete(generation, &key, 8).unwrap(), Some(10));
    }

    #[test]
    fn offset_gaps_do_not_stall_commits() {
        // Compacted topics and transaction markers leave offsets that are
        // never delivered.
        let key = key();
        let mut progress = assigned([key.clone()]);
        let generation = register(&mut progress, &key, 7);
        register(&mut progress, &key, 9);
        register(&mut progress, &key, 12);

        assert_eq!(progress.complete(generation, &key, 7).unwrap(), Some(9));
        progress.committed(generation, &key, 9);
        assert_eq!(progress.complete(generation, &key, 12).unwrap(), None);
        assert_eq!(progress.complete(generation, &key, 9).unwrap(), Some(13));
        progress.committed(generation, &key, 13);

        register(&mut progress, &key, 20);
        assert_eq!(progress.complete(generation, &key, 20).unwrap(), Some(21));
    }

    #[test]
    fn failed_commit_keeps_completed_prefix_for_a_later_retry() {
        let key = key();
        let mut progress = assigned([key.clone()]);
        let generation = register(&mut progress, &key, 3);
        assert_eq!(progress.complete(generation, &key, 3).unwrap(), Some(4));
        assert_eq!(progress.complete(generation, &key, 3).unwrap(), Some(4));
        progress.committed(generation, &key, 4);
        assert_eq!(progress.complete(generation, &key, 3).unwrap(), None);
    }

    #[test]
    fn rebalance_generation_is_scoped_to_the_revoked_partition() {
        let first = key();
        let second = ("orders".to_owned(), 1);
        let mut progress = assigned([first.clone(), second.clone()]);
        let first_generation = register(&mut progress, &first, 0);
        let second_generation = register(&mut progress, &second, 0);

        progress.revoke(&first);
        progress.activate(first.clone());
        let reassigned_generation = register(&mut progress, &first, 0);
        assert_ne!(first_generation, reassigned_generation);
        assert!(progress.complete(first_generation, &first, 0).is_err());
        assert_eq!(
            progress.complete(second_generation, &second, 0).unwrap(),
            Some(1)
        );
    }

    #[test]
    fn revocation_cancels_only_the_revoked_assignment() {
        let first = key();
        let second = ("orders".to_owned(), 1);
        let mut progress = assigned([first.clone(), second.clone()]);
        let (_, first_token) = progress.register(&first, 0).unwrap();
        let (_, second_token) = progress.register(&second, 0).unwrap();

        progress.revoke(&first);
        assert!(first_token.is_cancelled());
        assert!(!second_token.is_cancelled());

        progress.activate(first.clone());
        let (_, reassigned_token) = progress.register(&first, 0).unwrap();
        assert!(!reassigned_token.is_cancelled());

        progress.revoke_all();
        assert!(second_token.is_cancelled());
        assert!(reassigned_token.is_cancelled());
    }

    #[test]
    fn only_the_current_generation_of_an_assigned_partition_is_current() {
        let key = key();
        let mut progress = assigned([key.clone()]);
        let generation = register(&mut progress, &key, 0);
        assert!(progress.is_current(generation, &key));
        assert!(!progress.is_current(generation, &("orders".to_owned(), 1)));

        progress.revoke(&key);
        assert!(!progress.is_current(generation, &key));
        progress.activate(key.clone());
        assert!(!progress.is_current(generation, &key));
        let reassigned = register(&mut progress, &key, 0);
        assert!(progress.is_current(reassigned, &key));
    }

    #[test]
    fn records_for_unassigned_partitions_are_not_registered() {
        let key = key();
        let mut progress = Progress::default();
        assert!(progress.register(&key, 0).is_none());

        progress.activate(key.clone());
        assert!(progress.register(&key, 0).is_some());
        progress.revoke(&key);
        assert!(progress.register(&key, 1).is_none());
    }

    #[test]
    fn duplicate_delivery_is_safe_after_commit_but_unknown_offsets_are_not() {
        let key = key();
        let mut progress = assigned([key.clone()]);
        let generation = register(&mut progress, &key, 5);
        assert_eq!(progress.complete(generation, &key, 5).unwrap(), Some(6));
        progress.committed(generation, &key, 6);
        assert_eq!(progress.complete(generation, &key, 5).unwrap(), None);
        assert!(progress.complete(generation, &key, 6).is_err());
    }
}
