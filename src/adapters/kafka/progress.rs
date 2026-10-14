use super::probe::KafkaProbe;
use crate::error::{BoxError, Error, ensure};
use crate::shutdown::CancellationToken;
use rdkafka::{
    ClientContext, Offset, TopicPartitionList,
    consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance},
    statistics::Statistics,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Condvar, Mutex, MutexGuard},
    time::{Duration, Instant},
};
use tracing::{debug, warn};

pub(super) type Partition = (String, i32);
pub(super) type KafkaConsumer = BaseConsumer<Context>;

/// Offset progress of every assigned partition, with a signal for a graceful
/// handoff waiting for deliveries to settle.
#[derive(Default)]
pub(super) struct SharedProgress {
    state: Mutex<Progress>,
    changed: Condvar,
}

impl SharedProgress {
    pub(super) fn lock(&self) -> MutexGuard<'_, Progress> {
        self.state.lock().unwrap()
    }

    /// Changes progress and wakes a handoff waiting for deliveries to settle.
    pub(super) fn update<T>(&self, change: impl FnOnce(&mut Progress) -> T) -> T {
        let result = change(&mut self.lock());
        self.changed.notify_all();
        result
    }

    /// Waits until no delivery of `partitions` is pending, or `timeout` passes.
    fn wait_settled(&self, partitions: &[Partition], timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut progress = self.lock();
        loop {
            if progress.settled(partitions) {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            progress = self
                .changed
                .wait_timeout(progress, deadline - now)
                .unwrap()
                .0;
        }
    }
}

#[derive(Default)]
pub(super) struct Progress {
    active: HashMap<Partition, PartitionProgress>,
    generations: HashMap<Partition, u64>,
    /// Partitions the runtime paused, kept across revocations so a partition
    /// assigned again stays paused until the runtime resumes it.
    paused: HashSet<Partition>,
}

struct PartitionProgress {
    generation: u64,
    revoked: CancellationToken,
    handoff: CancellationToken,
    next: Option<i64>,
    /// Registered offsets at or after `next`, mapped to their delivery's state.
    /// Offsets absent between registered ones were never delivered.
    received: BTreeMap<i64, Status>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Status {
    /// Delivered and neither acknowledged nor dropped.
    Pending,
    /// Acknowledged; the commit may advance past it.
    Done,
    /// Dropped without acknowledgement. The commit stops before it, but a
    /// handoff need not wait for it.
    Released,
}

/// What a delivery of an assigned partition carries from its registration.
pub(super) struct Registered {
    pub(super) generation: u64,
    pub(super) revoked: CancellationToken,
    pub(super) handoff: CancellationToken,
}

impl Progress {
    fn activate(&mut self, key: Partition) {
        self.active.entry(key.clone()).or_insert_with(|| {
            let generation = self.generations.entry(key).or_default();
            *generation = generation.wrapping_add(1);
            PartitionProgress {
                generation: *generation,
                revoked: CancellationToken::new(),
                handoff: CancellationToken::new(),
                next: None,
                received: BTreeMap::new(),
            }
        });
    }

    fn revoke(&mut self, key: &Partition) {
        if let Some(partition) = self.active.remove(key) {
            partition.handoff.cancel();
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

    /// The current assignment generation of a partition, if it is assigned.
    pub(super) fn generation(&self, key: &Partition) -> Option<u64> {
        self.active.get(key).map(|partition| partition.generation)
    }

    /// Registers an offset received in assignment `generation`. Records of a
    /// partition that is no longer in that assignment, or that is being
    /// handed off, are stale and return `None`.
    pub(super) fn register(
        &mut self,
        key: &Partition,
        generation: u64,
        offset: i64,
    ) -> Option<Registered> {
        let partition = self.active.get_mut(key)?;
        if partition.generation != generation || partition.handoff.is_cancelled() {
            return None;
        }
        match partition.next {
            Some(next) if offset < next => {}
            Some(_) => {
                partition.received.entry(offset).or_insert(Status::Pending);
            }
            None => {
                partition.next = Some(offset);
                partition.received.entry(offset).or_insert(Status::Pending);
            }
        }
        Some(Registered {
            generation,
            revoked: partition.revoked.clone(),
            handoff: partition.handoff.clone(),
        })
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
        let status = partition
            .received
            .get_mut(&offset)
            .ok_or_else(|| Error::msg("Kafka offset was never registered"))?;
        *status = Status::Done;
        Ok(partition.commit_position())
    }

    /// Records that the delivery of `offset` was dropped without
    /// acknowledgement. Its offset stays uncommitted.
    pub(super) fn release(&mut self, generation: u64, key: &Partition, offset: i64) {
        let Some(partition) = self.active.get_mut(key) else {
            return;
        };
        if partition.generation != generation {
            return;
        }
        if let Some(status @ Status::Pending) = partition.received.get_mut(&offset) {
            *status = Status::Released;
        }
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

    /// Starts handing off the assigned partitions among `keys`, so their
    /// unstarted deliveries are abandoned and new records are not registered.
    /// Returns those partitions.
    fn begin_handoff(&mut self, keys: &[Partition]) -> Vec<Partition> {
        keys.iter()
            .filter(|key| {
                self.active.get(*key).is_some_and(|partition| {
                    partition.handoff.cancel();
                    true
                })
            })
            .cloned()
            .collect()
    }

    /// Whether no delivery of `keys` is still pending.
    fn settled(&self, keys: &[Partition]) -> bool {
        keys.iter().all(|key| {
            self.active.get(key).is_none_or(|partition| {
                !partition
                    .received
                    .values()
                    .any(|status| *status == Status::Pending)
            })
        })
    }

    /// The assignment generation and the offset to commit for a partition
    /// whose acknowledged deliveries advanced past its committed position.
    fn commit_position(&self, key: &Partition) -> Option<(u64, i64)> {
        let partition = self.active.get(key)?;
        Some((partition.generation, partition.commit_position()?))
    }

    /// Marks a partition paused. Returns whether it is assigned, so the
    /// consumer should pause it now; otherwise it pauses when assigned.
    pub(super) fn pause(&mut self, key: &Partition) -> bool {
        self.paused.insert(key.clone());
        self.active.contains_key(key)
    }

    /// Clears a partition's pause. Returns whether it is assigned, so the
    /// consumer should resume it.
    pub(super) fn resume(&mut self, key: &Partition) -> bool {
        self.paused.remove(key) && self.active.contains_key(key)
    }
}

impl PartitionProgress {
    /// The commit position when it is past `next`: the first registered offset
    /// that is not acknowledged, or the offset after the last registered one
    /// when all are.
    fn commit_position(&self) -> Option<i64> {
        let next = self.next?;
        // Kafka delivers a partition's records in offset order, so an offset
        // between two registered offsets was never delivered (for example, it
        // was compacted away or is a transaction marker).
        let candidate = self
            .received
            .iter()
            .find_map(|(offset, status)| (*status != Status::Done).then_some(*offset))
            .or_else(|| self.received.last_key_value().map(|(offset, _)| offset + 1))
            .unwrap_or(next);
        (candidate > next).then_some(candidate)
    }
}

/// Held while a producer transaction checks the assignment and commits
/// offsets, so a revoke waits for that commit and a commit never starts for a
/// revoked assignment.
pub(super) type TransactionGate = Arc<Mutex<()>>;

/// Serializes acknowledgement commits, while the progress mutex remains
/// unlocked during librdkafka's blocking synchronous commit.
pub(super) type CommitGate = Arc<tokio::sync::Mutex<()>>;

pub(super) struct Context {
    pub(super) progress: Arc<SharedProgress>,
    pub(super) transactions: TransactionGate,
    pub(super) commits: CommitGate,
    /// How long a revoke waits for started deliveries; `None` revokes at once.
    pub(super) handoff: Option<Duration>,
    pub(super) probe: Arc<KafkaProbe>,
}

impl Context {
    /// Lets started deliveries of `partitions` finish for at most `timeout`,
    /// then commits their acknowledged offsets. Returns the commit gate, held
    /// until the revoke completes so no acknowledgement commits in between.
    fn hand_off(
        &self,
        consumer: &BaseConsumer<Self>,
        partitions: &[Partition],
        timeout: Duration,
    ) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        // A lost assignment cannot commit, and a revoke run while the consumer
        // is dropped on an async runtime thread must not block it.
        if consumer.assignment_lost() || tokio::runtime::Handle::try_current().is_ok() {
            return None;
        }
        let partitions = self
            .progress
            .update(|progress| progress.begin_handoff(partitions));
        if partitions.is_empty() {
            return None;
        }
        if !self.progress.wait_settled(&partitions, timeout) {
            warn!(
                ?timeout,
                "Kafka handoff timed out; unfinished deliveries are abandoned"
            );
        }
        let commits = self.commits.blocking_lock();
        let positions = {
            let progress = self.progress.lock();
            partitions
                .iter()
                .filter_map(|key| Some((key, progress.commit_position(key)?)))
                .collect::<Vec<_>>()
        };
        if positions.is_empty() {
            return Some(commits);
        }
        let mut offsets = TopicPartitionList::new();
        for ((topic, partition), (_, next)) in &positions {
            if let Err(error) =
                offsets.add_partition_offset(topic, *partition, Offset::Offset(*next))
            {
                warn!(%error, "Kafka handoff could not list an offset to commit");
                return Some(commits);
            }
        }
        match consumer.commit(&offsets, CommitMode::Sync) {
            Ok(()) => {
                debug!(partitions = positions.len(), "committed offsets on handoff");
                self.progress.update(|progress| {
                    for (key, (generation, next)) in &positions {
                        progress.committed(*generation, key, *next);
                    }
                });
            }
            Err(error) => warn!(%error, "Kafka handoff commit failed"),
        }
        Some(commits)
    }
}

fn partitions(list: &TopicPartitionList) -> Vec<Partition> {
    list.elements()
        .iter()
        .map(|partition| (partition.topic().to_owned(), partition.partition()))
        .collect()
}

impl ClientContext for Context {
    fn stats(&self, statistics: Statistics) {
        self.probe.update(&statistics);
    }
}

impl ConsumerContext for Context {
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Revoke(list) = rebalance {
            let revoked = partitions(list);
            let _commits = self
                .handoff
                .and_then(|timeout| self.hand_off(consumer, &revoked, timeout));
            let _transactions = self.transactions.lock().unwrap();
            self.progress.update(|progress| {
                for key in &revoked {
                    progress.revoke(key);
                }
            });
        }
    }

    fn post_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Assign(list) = rebalance {
            let mut paused = TopicPartitionList::new();
            {
                let mut progress = self.progress.lock();
                for key in partitions(list) {
                    if progress.paused.contains(&key) {
                        paused.add_partition(&key.0, key.1);
                    }
                    progress.activate(key);
                }
            }
            if paused.count() > 0
                && let Err(error) = consumer.pause(&paused)
            {
                warn!(%error, "Kafka could not pause reassigned partitions");
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

    /// Registers `offset` in the partition's current assignment.
    fn registered(progress: &mut Progress, key: &Partition, offset: i64) -> Option<Registered> {
        let generation = progress.generation(key)?;
        progress.register(key, generation, offset)
    }

    fn register(progress: &mut Progress, key: &Partition, offset: i64) -> u64 {
        registered(progress, key, offset)
            .expect("assigned partition")
            .generation
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
        let first_token = registered(&mut progress, &first, 0).unwrap().revoked;
        let second_token = registered(&mut progress, &second, 0).unwrap().revoked;

        progress.revoke(&first);
        assert!(first_token.is_cancelled());
        assert!(!second_token.is_cancelled());

        progress.activate(first.clone());
        let reassigned_token = registered(&mut progress, &first, 0).unwrap().revoked;
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
        assert!(registered(&mut progress, &key, 0).is_none());

        progress.activate(key.clone());
        let generation = register(&mut progress, &key, 0);
        progress.revoke(&key);
        assert!(progress.register(&key, generation, 1).is_none());
        progress.activate(key.clone());
        // A record polled in the earlier assignment is stale in the new one.
        assert!(progress.register(&key, generation, 1).is_none());
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

    #[test]
    fn released_deliveries_hold_back_the_commit_but_settle_a_handoff() {
        let key = key();
        let mut progress = assigned([key.clone()]);
        let generation = register(&mut progress, &key, 0);
        register(&mut progress, &key, 1);
        register(&mut progress, &key, 2);
        progress.release(generation, &key, 1);
        assert!(!progress.settled(std::slice::from_ref(&key)));

        assert_eq!(progress.complete(generation, &key, 0).unwrap(), Some(1));
        assert_eq!(progress.complete(generation, &key, 2).unwrap(), Some(1));
        assert!(progress.settled(std::slice::from_ref(&key)));
        assert_eq!(progress.commit_position(&key), Some((generation, 1)));

        // A release never undoes an acknowledgement.
        progress.release(generation, &key, 2);
        progress.committed(generation, &key, 1);
        assert_eq!(progress.commit_position(&key), None);
    }

    #[test]
    fn a_handoff_cancels_its_token_and_stops_registering_records() {
        let key = key();
        let other = ("orders".to_owned(), 1);
        let mut progress = assigned([key.clone(), other.clone()]);
        let delivery = registered(&mut progress, &key, 0).unwrap();
        let unassigned = ("orders".to_owned(), 2);

        let handing = progress.begin_handoff(&[key.clone(), unassigned]);
        assert_eq!(handing, std::slice::from_ref(&key));
        assert!(delivery.handoff.is_cancelled());
        assert!(!delivery.revoked.is_cancelled());
        assert!(registered(&mut progress, &key, 1).is_none());
        assert!(registered(&mut progress, &other, 0).is_some());

        // The delivery that started still acknowledges until the revoke.
        assert_eq!(
            progress.complete(delivery.generation, &key, 0).unwrap(),
            Some(1)
        );
        progress.revoke(&key);
        assert!(delivery.revoked.is_cancelled());
    }

    #[test]
    fn pause_survives_reassignment_until_resumed() {
        let key = key();
        let mut progress = Progress::default();
        assert!(!progress.pause(&key));
        progress.activate(key.clone());
        assert!(progress.paused.contains(&key));
        assert!(progress.resume(&key));
        assert!(!progress.resume(&key));
    }

    #[test]
    fn waiting_for_settlement_wakes_on_release() {
        let key = key();
        let shared = Arc::new(SharedProgress::default());
        let generation = {
            let mut progress = shared.lock();
            progress.activate(key.clone());
            register(&mut progress, &key, 0)
        };
        assert!(!shared.wait_settled(std::slice::from_ref(&key), Duration::from_millis(10)));
        let releasing = shared.clone();
        let release_key = key.clone();
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            releasing.update(|progress| progress.release(generation, &release_key, 0));
        });
        assert!(shared.wait_settled(std::slice::from_ref(&key), Duration::from_secs(5)));
        release.join().unwrap();
    }
}
