//! Ordering-key queues and per-key flow control between receive and bounded job execution.
use super::config::ProcessingOrder;
use crate::message::{OrderingKey, SourceMessage};
use std::collections::{HashMap, HashSet, VecDeque};

/// Identifies a delivery's place in the scheduler, passed back to
/// [`Scheduler::complete`] when the delivery finishes.
pub(super) struct Ticket {
    /// The key later deliveries queue behind; absent when unordered.
    order: Option<OrderingKey>,
    /// The key counted against the per-key limit; absent without a limit.
    flow: Option<OrderingKey>,
}

/// A request for the source to pause or resume one ordering key.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum FlowChange {
    Pause(OrderingKey),
    Resume(OrderingKey),
}

/// Tracks every received but unfinished delivery.
///
/// A key is present in `waiting` while one delivery with that key is running or
/// ready; later deliveries with the same key queue behind it.
pub(super) struct Scheduler<M> {
    order: ProcessingOrder,
    ready: VecDeque<(Ticket, M)>,
    waiting: HashMap<OrderingKey, VecDeque<(Ticket, M)>>,
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

impl<M: SourceMessage> Scheduler<M> {
    /// `per_key_limit` bounds unfinished deliveries per ordering key regardless
    /// of `order`, through pause and resume requests to the source.
    pub(super) fn new(order: ProcessingOrder, per_key_limit: Option<usize>) -> Self {
        Self {
            order,
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

    /// Received deliveries that have not completed, including running ones.
    pub(super) fn outstanding(&self) -> usize {
        self.outstanding
    }

    pub(super) fn push(&mut self, delivery: M) {
        self.outstanding += 1;
        let key = delivery.ordering_key();
        let flow = match (&mut self.flow, &key) {
            (Some(flow), Some(key)) => {
                flow.add(key);
                Some(key.clone())
            }
            _ => None,
        };
        let order = match self.order {
            ProcessingOrder::PerKey => key,
            ProcessingOrder::Unordered => None,
        };
        let ticket = Ticket {
            order: order.clone(),
            flow,
        };
        match order {
            Some(key) => match self.waiting.get_mut(&key) {
                Some(queue) => queue.push_back((ticket, delivery)),
                None => {
                    self.waiting.insert(key, VecDeque::new());
                    self.ready.push_back((ticket, delivery));
                }
            },
            None => self.ready.push_back((ticket, delivery)),
        }
    }

    /// The next delivery allowed to start, with the ticket to pass to `complete`.
    pub(super) fn next_ready(&mut self) -> Option<(Ticket, M)> {
        self.ready.pop_front()
    }

    /// Records that a started delivery finished and releases its key's successor.
    pub(super) fn complete(&mut self, ticket: Ticket) {
        self.outstanding -= 1;
        self.release(&ticket);
        let Some(key) = ticket.order else {
            return;
        };
        let Some(queue) = self.waiting.get_mut(&key) else {
            return;
        };
        match queue.pop_front() {
            Some(next) => self.ready.push_back(next),
            None => {
                self.waiting.remove(&key);
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

    /// Drops deliveries that have not started. They remain unacknowledged.
    pub(super) fn discard_pending(&mut self) {
        let mut pending = std::mem::take(&mut self.ready);
        for (_, queue) in self.waiting.drain() {
            pending.extend(queue);
        }
        self.outstanding -= pending.len();
        for (ticket, _) in &pending {
            self.release(ticket);
        }
    }

    fn release(&mut self, ticket: &Ticket) {
        if let (Some(flow), Some(key)) = (&mut self.flow, &ticket.flow) {
            flow.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Delivery;

    fn keyed(value: i32, partition: i64) -> Delivery<i32> {
        Delivery::untracked(value).with_ordering_key(key(partition))
    }

    fn key(partition: i64) -> OrderingKey {
        OrderingKey::new("orders", partition)
    }

    fn drain_ready(scheduler: &mut Scheduler<Delivery<i32>>) -> Vec<(Ticket, i32)> {
        std::iter::from_fn(|| scheduler.next_ready())
            .map(|(ticket, delivery)| (ticket, delivery.value))
            .collect()
    }

    fn orders(ready: &[(Ticket, i32)]) -> Vec<(Option<OrderingKey>, i32)> {
        ready
            .iter()
            .map(|(ticket, value)| (ticket.order.clone(), *value))
            .collect()
    }

    #[test]
    fn same_key_waits_for_completion_while_other_keys_start() {
        let mut scheduler = Scheduler::new(ProcessingOrder::PerKey, None);
        scheduler.push(keyed(1, 0));
        scheduler.push(keyed(2, 0));
        scheduler.push(keyed(3, 1));
        scheduler.push(Delivery::untracked(4));

        let mut ready = drain_ready(&mut scheduler);
        assert_eq!(
            orders(&ready),
            vec![(Some(key(0)), 1), (Some(key(1)), 3), (None, 4)]
        );
        assert_eq!(scheduler.outstanding(), 4);

        let (unkeyed, _) = ready.pop().unwrap();
        let (second, _) = ready.pop().unwrap();
        let (first, _) = ready.pop().unwrap();
        scheduler.complete(unkeyed);
        scheduler.complete(second);
        assert!(scheduler.next_ready().is_none());
        scheduler.complete(first);
        let mut ready = drain_ready(&mut scheduler);
        assert_eq!(orders(&ready), vec![(Some(key(0)), 2)]);
        scheduler.complete(ready.pop().unwrap().0);
        assert_eq!(scheduler.outstanding(), 0);
        assert!(scheduler.waiting.is_empty());
    }

    #[test]
    fn unordered_mode_ignores_ordering_keys() {
        let mut scheduler = Scheduler::new(ProcessingOrder::Unordered, None);
        scheduler.push(keyed(1, 0));
        scheduler.push(keyed(2, 0));
        assert_eq!(
            orders(&drain_ready(&mut scheduler)),
            vec![(None, 1), (None, 2)]
        );
    }

    #[test]
    fn discarding_keeps_running_deliveries_outstanding() {
        let mut scheduler = Scheduler::new(ProcessingOrder::PerKey, None);
        scheduler.push(keyed(1, 0));
        scheduler.push(keyed(2, 0));
        scheduler.push(keyed(3, 1));
        let (running, _) = scheduler.next_ready().unwrap();

        scheduler.discard_pending();
        assert_eq!(scheduler.outstanding(), 1);
        scheduler.complete(running);
        assert_eq!(scheduler.outstanding(), 0);
        assert!(scheduler.next_ready().is_none());
    }

    #[test]
    fn a_key_pauses_at_the_limit_and_resumes_at_half_of_it() {
        let mut scheduler = Scheduler::new(ProcessingOrder::PerKey, Some(4));
        for value in 0..4 {
            scheduler.push(keyed(value, 0));
        }
        scheduler.push(keyed(10, 1));
        assert_eq!(scheduler.take_flow_changes(), [FlowChange::Pause(key(0))]);

        // Buffered deliveries arriving after the pause do not pause again.
        scheduler.push(keyed(4, 0));
        assert!(scheduler.take_flow_changes().is_empty());

        let mut finished = 0;
        while finished < 3 {
            let (ticket, value) = scheduler.next_ready().unwrap();
            if value.value < 10 {
                finished += 1;
            }
            scheduler.complete(ticket);
        }
        assert_eq!(scheduler.take_flow_changes(), [FlowChange::Resume(key(0))]);
    }

    #[test]
    fn unordered_mode_still_limits_each_key() {
        let mut scheduler = Scheduler::new(ProcessingOrder::Unordered, Some(2));
        scheduler.push(keyed(1, 0));
        scheduler.push(keyed(2, 0));
        assert_eq!(scheduler.take_flow_changes(), [FlowChange::Pause(key(0))]);
        for (ticket, _) in drain_ready(&mut scheduler) {
            scheduler.complete(ticket);
        }
        assert_eq!(scheduler.take_flow_changes(), [FlowChange::Resume(key(0))]);
    }

    #[test]
    fn discarded_deliveries_no_longer_count_against_the_limit() {
        let mut scheduler = Scheduler::new(ProcessingOrder::PerKey, Some(2));
        scheduler.push(keyed(1, 0));
        scheduler.push(keyed(2, 0));
        let (running, _) = scheduler.next_ready().unwrap();
        scheduler.discard_pending();
        scheduler.complete(running);
        assert!(scheduler.flow.as_ref().unwrap().unfinished.is_empty());
    }

    #[test]
    fn disabled_flow_control_requests_nothing() {
        let mut scheduler = Scheduler::new(ProcessingOrder::PerKey, Some(1));
        scheduler.push(keyed(1, 0));
        assert_eq!(scheduler.take_flow_changes(), [FlowChange::Pause(key(0))]);
        scheduler.disable_flow_control();
        let (ticket, _) = scheduler.next_ready().unwrap();
        scheduler.complete(ticket);
        assert!(scheduler.take_flow_changes().is_empty());
    }
}
