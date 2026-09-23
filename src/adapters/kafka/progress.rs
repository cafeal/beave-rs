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
    next: Option<i64>,
    received: BTreeMap<i64, bool>,
}

impl Progress {
    fn activate(&mut self, key: Partition) {
        self.active.entry(key.clone()).or_insert_with(|| {
            let generation = self.generations.entry(key).or_default();
            *generation = generation.wrapping_add(1);
            PartitionProgress {
                generation: *generation,
                next: None,
                received: BTreeMap::new(),
            }
        });
    }

    fn revoke(&mut self, key: &Partition) {
        self.active.remove(key);
        let generation = self.generations.entry(key.clone()).or_default();
        *generation = generation.wrapping_add(1);
    }

    pub(super) fn revoke_all(&mut self) {
        let keys = self.active.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            self.revoke(&key);
        }
    }

    pub(super) fn register(&mut self, key: Partition, offset: i64) -> u64 {
        self.activate(key.clone());
        let partition = self.active.get_mut(&key).expect("activated partition");
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
        partition.generation
    }

    pub(super) fn complete(
        &mut self,
        generation: u64,
        key: &Partition,
        offset: i64,
    ) -> anyhow::Result<Option<i64>> {
        let partition = self
            .active
            .get_mut(key)
            .ok_or_else(|| anyhow::anyhow!("Kafka delivery belongs to a revoked assignment"))?;
        anyhow::ensure!(
            partition.generation == generation,
            "Kafka delivery belongs to an expired assignment"
        );
        let Some(next) = partition.next else {
            anyhow::bail!("Kafka partition has no registered delivery");
        };
        if offset < next {
            return Ok(None);
        }
        let done = partition
            .received
            .get_mut(&offset)
            .ok_or_else(|| anyhow::anyhow!("Kafka offset was never registered"))?;
        *done = true;
        let mut candidate = next;
        while let Some(true) = partition.received.get(&candidate) {
            candidate += 1;
        }
        Ok((candidate > next).then_some(candidate))
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

pub(super) struct Context(pub(super) Arc<Mutex<Progress>>);

impl ClientContext for Context {}

impl ConsumerContext for Context {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Revoke(partitions) = rebalance {
            let mut progress = self.0.lock().unwrap();
            for partition in partitions.elements() {
                progress.revoke(&(partition.topic().to_owned(), partition.partition()));
            }
        }
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Assign(partitions) = rebalance {
            let mut progress = self.0.lock().unwrap();
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

    #[test]
    fn completion_commits_only_the_contiguous_registered_prefix() {
        let mut progress = Progress::default();
        let key = key();
        let generation = progress.register(key.clone(), 7);
        progress.register(key.clone(), 9);

        assert_eq!(progress.complete(generation, &key, 9).unwrap(), None);
        assert_eq!(progress.complete(generation, &key, 7).unwrap(), Some(8));
        progress.committed(generation, &key, 8);
        assert_eq!(progress.complete(generation, &key, 9).unwrap(), None);

        progress.register(key.clone(), 8);
        assert_eq!(progress.complete(generation, &key, 8).unwrap(), Some(10));
    }

    #[test]
    fn failed_commit_keeps_completed_prefix_for_a_later_retry() {
        let mut progress = Progress::default();
        let key = key();
        let generation = progress.register(key.clone(), 3);
        assert_eq!(progress.complete(generation, &key, 3).unwrap(), Some(4));
        assert_eq!(progress.complete(generation, &key, 3).unwrap(), Some(4));
        progress.committed(generation, &key, 4);
        assert_eq!(progress.complete(generation, &key, 3).unwrap(), None);
    }

    #[test]
    fn rebalance_generation_is_scoped_to_the_revoked_partition() {
        let mut progress = Progress::default();
        let first = key();
        let second = ("orders".to_owned(), 1);
        let first_generation = progress.register(first.clone(), 0);
        let second_generation = progress.register(second.clone(), 0);

        progress.revoke(&first);
        let reassigned_generation = progress.register(first.clone(), 0);
        assert_ne!(first_generation, reassigned_generation);
        assert!(progress.complete(first_generation, &first, 0).is_err());
        assert_eq!(
            progress.complete(second_generation, &second, 0).unwrap(),
            Some(1)
        );
    }

    #[test]
    fn duplicate_delivery_is_safe_after_commit_but_unknown_offsets_are_not() {
        let mut progress = Progress::default();
        let key = key();
        let generation = progress.register(key.clone(), 5);
        assert_eq!(progress.complete(generation, &key, 5).unwrap(), Some(6));
        progress.committed(generation, &key, 6);
        assert_eq!(progress.complete(generation, &key, 5).unwrap(), None);
        assert!(progress.complete(generation, &key, 6).is_err());
    }
}
