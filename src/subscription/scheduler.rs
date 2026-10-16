//! Distribution lanes and per-key flow control between decoding and bounded job execution.
use crate::{
    distribution::Distribution,
    message::{OrderingKey, SourceMessage},
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::{BuildHasher, Hash, RandomState},
};

/// Deliveries with equal lanes run one at a time. A lane is the hash of a
/// [`Distribution`] key: two keys that share a hash only share a lane, which
/// orders more than needed but never less.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct Lane(u64);

/// Assigns a decoded delivery its lane.
type AssignLane<M> = Box<dyn Fn(&M, &<M as SourceMessage>::Item) -> Option<Lane> + Send + Sync>;

/// A subscription's [`Distribution`], independent of its key type.
pub(super) struct Distributor<M: SourceMessage> {
    lane: AssignLane<M>,
    keeps_source_order: bool,
}

impl<M: SourceMessage> Distributor<M> {
    pub(super) fn new<D: Distribution<M>>(distribution: D) -> Self {
        let hasher = RandomState::new();
        Self {
            keeps_source_order: distribution.keeps_source_order(),
            lane: Box::new(move |message, value| {
                distribution
                    .key(message, value)
                    .map(|key| Lane(hasher.hash_one(&key)))
            }),
        }
    }

    /// Distributes by the key `key` returns, without keeping the source order.
    pub(super) fn by_key<K: Hash>(
        key: impl Fn(&M, &M::Item) -> Option<K> + Send + Sync + 'static,
    ) -> Self {
        let hasher = RandomState::new();
        Self {
            keeps_source_order: false,
            lane: Box::new(move |message, value| {
                key(message, value).map(|key| Lane(hasher.hash_one(&key)))
            }),
        }
    }

    pub(super) fn lane(&self, message: &M, value: &M::Item) -> Option<Lane> {
        (self.lane)(message, value)
    }

    pub(super) fn keeps_source_order(&self) -> bool {
        self.keeps_source_order
    }
}

/// A received delivery counted by the scheduler before it is distributed,
/// passed back to [`Scheduler::push`] or [`Scheduler::withdraw`].
pub(super) struct Admission {
    /// The key counted against the per-key limit; absent without a limit.
    flow: Option<OrderingKey>,
}

/// Identifies a delivery's place in the scheduler, passed back to
/// [`Scheduler::complete`] when the delivery finishes.
pub(super) struct Ticket {
    /// The lane later deliveries queue behind; absent when the delivery runs
    /// independently.
    lane: Option<Lane>,
    flow: Option<OrderingKey>,
}

/// A request for the source to pause or resume one ordering key.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum FlowChange {
    Pause(OrderingKey),
    Resume(OrderingKey),
}

/// Tracks every received but unfinished delivery, from admission through
/// distribution to completion.
///
/// A lane is present in `waiting` while one delivery in that lane is running
/// or ready; later deliveries in the same lane queue behind it.
pub(super) struct Scheduler<E> {
    ready: VecDeque<(Ticket, E)>,
    waiting: HashMap<Lane, VecDeque<(Ticket, E)>>,
    outstanding: usize,
    flow: Option<FlowControl>,
}

/// Unfinished deliveries per ordering key, and the keys paused because they
/// reached the limit. A paused key resumes at half the limit, so a key near the
/// limit does not pause and resume on every delivery.
struct FlowControl {
    limit: usize,
    unfinished: HashMap<OrderingKey, usize>,
    paused: HashSet<OrderingKey>,
    changes: Vec<FlowChange>,
}

impl FlowControl {
    fn add(&mut self, key: &OrderingKey) {
        let count = self.unfinished.entry(key.clone()).or_default();
        *count += 1;
        if *count >= self.limit && self.paused.insert(key.clone()) {
            self.changes.push(FlowChange::Pause(key.clone()));
        }
    }

    fn remove(&mut self, key: &OrderingKey) {
        let Some(count) = self.unfinished.get_mut(key) else {
            return;
        };
        *count -= 1;
        let count = *count;
        if count == 0 {
            self.unfinished.remove(key);
        }
        if count <= self.limit / 2 && self.paused.remove(key) {
            self.changes.push(FlowChange::Resume(key.clone()));
        }
    }
}

impl<E> Scheduler<E> {
    /// `per_key_limit` bounds unfinished deliveries per ordering key, through
    /// pause and resume requests to the source, whatever the distribution.
    pub(super) fn new(per_key_limit: Option<usize>) -> Self {
        Self {
            ready: VecDeque::new(),
            waiting: HashMap::new(),
            outstanding: 0,
            flow: per_key_limit.map(|limit| FlowControl {
                limit,
                unfinished: HashMap::new(),
                paused: HashSet::new(),
                changes: Vec::new(),
            }),
        }
    }

    /// Received deliveries that have not completed, including ones still
    /// decoding and running ones.
    pub(super) fn outstanding(&self) -> usize {
        self.outstanding
    }

    /// Counts a received delivery whose source ordering scope is `scope`.
    pub(super) fn admit(&mut self, scope: Option<OrderingKey>) -> Admission {
        self.outstanding += 1;
        let flow = match (&mut self.flow, scope) {
            (Some(flow), Some(key)) => {
                flow.add(&key);
                Some(key)
            }
            _ => None,
        };
        Admission { flow }
    }

    /// Queues an admitted delivery behind earlier deliveries in `lane`, or
    /// makes it ready at once without a lane.
    pub(super) fn push(&mut self, admission: Admission, lane: Option<Lane>, entry: E) {
        let ticket = Ticket {
            lane,
            flow: admission.flow,
        };
        match lane {
            Some(lane) => match self.waiting.get_mut(&lane) {
                Some(queue) => queue.push_back((ticket, entry)),
                None => {
                    self.waiting.insert(lane, VecDeque::new());
                    self.ready.push_back((ticket, entry));
                }
            },
            None => self.ready.push_back((ticket, entry)),
        }
    }

    /// Forgets an admitted delivery that was dropped before it was pushed.
    pub(super) fn withdraw(&mut self, admission: Admission) {
        self.outstanding -= 1;
        self.release(admission.flow.as_ref());
    }

    /// The next delivery allowed to start, with the ticket to pass to `complete`.
    pub(super) fn next_ready(&mut self) -> Option<(Ticket, E)> {
        self.ready.pop_front()
    }

    /// Records that a started delivery finished and releases its lane's successor.
    pub(super) fn complete(&mut self, ticket: Ticket) {
        self.outstanding -= 1;
        self.release(ticket.flow.as_ref());
        let Some(lane) = ticket.lane else {
            return;
        };
        let Some(queue) = self.waiting.get_mut(&lane) else {
            return;
        };
        match queue.pop_front() {
            Some(next) => self.ready.push_back(next),
            None => {
                self.waiting.remove(&lane);
            }
        }
    }

    /// Pause and resume requests since the last call, in the order they arose.
    pub(super) fn take_flow_changes(&mut self) -> Vec<FlowChange> {
        self.flow
            .as_mut()
            .map(|flow| std::mem::take(&mut flow.changes))
            .unwrap_or_default()
    }

    /// Stops counting deliveries per key, for a source that cannot pause keys.
    pub(super) fn disable_flow_control(&mut self) {
        self.flow = None;
    }

    /// Drops distributed deliveries that have not started. They remain
    /// unacknowledged.
    pub(super) fn discard_pending(&mut self) {
        let mut pending = std::mem::take(&mut self.ready);
        for (_, queue) in self.waiting.drain() {
            pending.extend(queue);
        }
        self.outstanding -= pending.len();
        for (ticket, _) in &pending {
            self.release(ticket.flow.as_ref());
        }
    }

    fn release(&mut self, key: Option<&OrderingKey>) {
        if let (Some(flow), Some(key)) = (&mut self.flow, key) {
            flow.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(partition: i64) -> OrderingKey {
        OrderingKey::new("orders", partition)
    }

    fn lane(id: u64) -> Option<Lane> {
        Some(Lane(id))
    }

    fn push(scheduler: &mut Scheduler<i32>, value: i32, lane: Option<Lane>) {
        let admission = scheduler.admit(None);
        scheduler.push(admission, lane, value);
    }

    /// Admits and pushes a delivery of partition `partition` into the lane of
    /// that partition.
    fn push_keyed(scheduler: &mut Scheduler<i32>, value: i32, partition: i64) {
        let admission = scheduler.admit(Some(key(partition)));
        scheduler.push(admission, lane(partition as u64), value);
    }

    fn drain_ready(scheduler: &mut Scheduler<i32>) -> Vec<(Ticket, i32)> {
        std::iter::from_fn(|| scheduler.next_ready()).collect()
    }

    fn lanes(ready: &[(Ticket, i32)]) -> Vec<(Option<Lane>, i32)> {
        ready
            .iter()
            .map(|(ticket, value)| (ticket.lane, *value))
            .collect()
    }

    #[test]
    fn same_lane_waits_for_completion_while_other_lanes_start() {
        let mut scheduler = Scheduler::new(None);
        push(&mut scheduler, 1, lane(0));
        push(&mut scheduler, 2, lane(0));
        push(&mut scheduler, 3, lane(1));
        push(&mut scheduler, 4, None);

        let mut ready = drain_ready(&mut scheduler);
        assert_eq!(lanes(&ready), vec![(lane(0), 1), (lane(1), 3), (None, 4)]);
        assert_eq!(scheduler.outstanding(), 4);

        let (independent, _) = ready.pop().unwrap();
        let (second, _) = ready.pop().unwrap();
        let (first, _) = ready.pop().unwrap();
        scheduler.complete(independent);
        scheduler.complete(second);
        assert!(scheduler.next_ready().is_none());
        scheduler.complete(first);
        let mut ready = drain_ready(&mut scheduler);
        assert_eq!(lanes(&ready), vec![(lane(0), 2)]);
        scheduler.complete(ready.pop().unwrap().0);
        assert_eq!(scheduler.outstanding(), 0);
        assert!(scheduler.waiting.is_empty());
    }

    #[test]
    fn admitted_deliveries_count_until_withdrawn() {
        let mut scheduler = Scheduler::<i32>::new(Some(2));
        let first = scheduler.admit(Some(key(0)));
        let second = scheduler.admit(Some(key(0)));
        assert_eq!(scheduler.outstanding(), 2);
        assert_eq!(scheduler.take_flow_changes(), [FlowChange::Pause(key(0))]);

        scheduler.withdraw(first);
        scheduler.withdraw(second);
        assert_eq!(scheduler.outstanding(), 0);
        assert_eq!(scheduler.take_flow_changes(), [FlowChange::Resume(key(0))]);
    }

    #[test]
    fn discarding_keeps_running_deliveries_outstanding() {
        let mut scheduler = Scheduler::new(None);
        push(&mut scheduler, 1, lane(0));
        push(&mut scheduler, 2, lane(0));
        push(&mut scheduler, 3, lane(1));
        let (running, _) = scheduler.next_ready().unwrap();

        scheduler.discard_pending();
        assert_eq!(scheduler.outstanding(), 1);
        scheduler.complete(running);
        assert_eq!(scheduler.outstanding(), 0);
        assert!(scheduler.next_ready().is_none());
    }

    #[test]
    fn a_key_pauses_at_the_limit_and_resumes_at_half_of_it() {
        let mut scheduler = Scheduler::new(Some(4));
        for value in 0..4 {
            push_keyed(&mut scheduler, value, 0);
        }
        push_keyed(&mut scheduler, 10, 1);
        assert_eq!(scheduler.take_flow_changes(), [FlowChange::Pause(key(0))]);

        // Buffered deliveries arriving after the pause do not pause again.
        push_keyed(&mut scheduler, 4, 0);
        assert!(scheduler.take_flow_changes().is_empty());

        let mut finished = 0;
        while finished < 3 {
            let (ticket, value) = scheduler.next_ready().unwrap();
            if value < 10 {
                finished += 1;
            }
            scheduler.complete(ticket);
        }
        assert_eq!(scheduler.take_flow_changes(), [FlowChange::Resume(key(0))]);
    }

    #[test]
    fn independent_deliveries_still_count_against_their_key() {
        let mut scheduler = Scheduler::new(Some(2));
        for value in [1, 2] {
            let admission = scheduler.admit(Some(key(0)));
            scheduler.push(admission, None, value);
        }
        assert_eq!(scheduler.take_flow_changes(), [FlowChange::Pause(key(0))]);
        for (ticket, _) in drain_ready(&mut scheduler) {
            scheduler.complete(ticket);
        }
        assert_eq!(scheduler.take_flow_changes(), [FlowChange::Resume(key(0))]);
    }

    #[test]
    fn discarded_deliveries_no_longer_count_against_the_limit() {
        let mut scheduler = Scheduler::new(Some(2));
        push_keyed(&mut scheduler, 1, 0);
        push_keyed(&mut scheduler, 2, 0);
        let (running, _) = scheduler.next_ready().unwrap();
        scheduler.discard_pending();
        scheduler.complete(running);
        assert!(scheduler.flow.as_ref().unwrap().unfinished.is_empty());
    }

    #[test]
    fn disabled_flow_control_requests_nothing() {
        let mut scheduler = Scheduler::new(Some(1));
        push_keyed(&mut scheduler, 1, 0);
        assert_eq!(scheduler.take_flow_changes(), [FlowChange::Pause(key(0))]);
        scheduler.disable_flow_control();
        let (ticket, _) = scheduler.next_ready().unwrap();
        scheduler.complete(ticket);
        assert!(scheduler.take_flow_changes().is_empty());
    }
}
