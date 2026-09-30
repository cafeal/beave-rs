//! Ordering-key queues between receive and bounded job execution.
use super::config::ProcessingOrder;
use crate::message::{OrderingKey, SourceMessage};
use std::collections::{HashMap, VecDeque};

/// Tracks every received but unfinished delivery.
///
/// A key is present in `waiting` while one delivery with that key is running or
/// ready; later deliveries with the same key queue behind it.
pub(super) struct Scheduler<M> {
    order: ProcessingOrder,
    ready: VecDeque<(Option<OrderingKey>, M)>,
    waiting: HashMap<OrderingKey, VecDeque<M>>,
    outstanding: usize,
}

impl<M: SourceMessage> Scheduler<M> {
    pub(super) fn new(order: ProcessingOrder) -> Self {
        Self {
            order,
            ready: VecDeque::new(),
            waiting: HashMap::new(),
            outstanding: 0,
        }
    }

    /// Received deliveries that have not completed, including running ones.
    pub(super) fn outstanding(&self) -> usize {
        self.outstanding
    }

    pub(super) fn push(&mut self, delivery: M) {
        self.outstanding += 1;
        let key = match self.order {
            ProcessingOrder::PerKey => delivery.ordering_key(),
            ProcessingOrder::Unordered => None,
        };
        match key {
            Some(key) => match self.waiting.get_mut(&key) {
                Some(queue) => queue.push_back(delivery),
                None => {
                    self.waiting.insert(key.clone(), VecDeque::new());
                    self.ready.push_back((Some(key), delivery));
                }
            },
            None => self.ready.push_back((None, delivery)),
        }
    }

    /// The next delivery allowed to start, with the key to pass to `complete`.
    pub(super) fn next_ready(&mut self) -> Option<(Option<OrderingKey>, M)> {
        self.ready.pop_front()
    }

    /// Records that a started delivery finished and releases its key's successor.
    pub(super) fn complete(&mut self, key: Option<OrderingKey>) {
        self.outstanding -= 1;
        let Some(key) = key else {
            return;
        };
        let Some(queue) = self.waiting.get_mut(&key) else {
            return;
        };
        match queue.pop_front() {
            Some(next) => self.ready.push_back((Some(key), next)),
            None => {
                self.waiting.remove(&key);
            }
        }
    }

    /// Drops deliveries that have not started. They remain unacknowledged.
    pub(super) fn discard_pending(&mut self) {
        let pending = self.ready.len() + self.waiting.values().map(VecDeque::len).sum::<usize>();
        self.outstanding -= pending;
        self.ready.clear();
        self.waiting.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Delivery;

    fn keyed(value: i32, partition: i64) -> Delivery<i32> {
        Delivery::untracked(value).with_ordering_key(OrderingKey::new("orders", partition))
    }

    fn drain_ready(scheduler: &mut Scheduler<Delivery<i32>>) -> Vec<(Option<OrderingKey>, i32)> {
        std::iter::from_fn(|| scheduler.next_ready())
            .map(|(key, delivery)| (key, delivery.value))
            .collect()
    }

    #[test]
    fn same_key_waits_for_completion_while_other_keys_start() {
        let mut scheduler = Scheduler::new(ProcessingOrder::PerKey);
        scheduler.push(keyed(1, 0));
        scheduler.push(keyed(2, 0));
        scheduler.push(keyed(3, 1));
        scheduler.push(Delivery::untracked(4));

        let first = OrderingKey::new("orders", 0);
        let second = OrderingKey::new("orders", 1);
        assert_eq!(
            drain_ready(&mut scheduler),
            vec![
                (Some(first.clone()), 1),
                (Some(second.clone()), 3),
                (None, 4)
            ]
        );
        assert_eq!(scheduler.outstanding(), 4);

        scheduler.complete(None);
        scheduler.complete(Some(second));
        assert!(scheduler.next_ready().is_none());
        scheduler.complete(Some(first.clone()));
        assert_eq!(drain_ready(&mut scheduler), vec![(Some(first.clone()), 2)]);
        scheduler.complete(Some(first));
        assert_eq!(scheduler.outstanding(), 0);
        assert!(scheduler.waiting.is_empty());
    }

    #[test]
    fn unordered_mode_ignores_ordering_keys() {
        let mut scheduler = Scheduler::new(ProcessingOrder::Unordered);
        scheduler.push(keyed(1, 0));
        scheduler.push(keyed(2, 0));
        assert_eq!(drain_ready(&mut scheduler), vec![(None, 1), (None, 2)]);
    }

    #[test]
    fn discarding_keeps_running_deliveries_outstanding() {
        let mut scheduler = Scheduler::new(ProcessingOrder::PerKey);
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
}
