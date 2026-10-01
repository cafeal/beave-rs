use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// Where one record of a [`TestSource`](super::TestSource) stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryState {
    /// The subscription has not received the record.
    Pending,
    /// Received but not acknowledged: still in flight, abandoned after a
    /// revocation, or left unacknowledged when the subscription stopped.
    Received,
    /// Acknowledged: its outputs were published, or the error policy
    /// dead-lettered or discarded it.
    Acknowledged,
}

struct Ledger<R> {
    records: Vec<(R, DeliveryState)>,
    acknowledgements: Vec<usize>,
}

/// Shared view of what happened to each record of a
/// [`TestSource`](super::TestSource). Clones observe the same records.
pub struct Deliveries<R>(Arc<Mutex<Ledger<R>>>);

impl<R> Clone for Deliveries<R> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<R> Deliveries<R> {
    pub(super) fn new(records: Vec<R>) -> Self {
        Self(Arc::new(Mutex::new(Ledger {
            records: records
                .into_iter()
                .map(|record| (record, DeliveryState::Pending))
                .collect(),
            acknowledgements: Vec::new(),
        })))
    }

    fn ledger(&self) -> MutexGuard<'_, Ledger<R>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn receive(&self, index: usize) {
        self.ledger().records[index].1 = DeliveryState::Received;
    }

    pub(super) fn acknowledge(&self, index: usize) {
        let mut ledger = self.ledger();
        ledger.records[index].1 = DeliveryState::Acknowledged;
        ledger.acknowledgements.push(index);
    }

    /// The state of each record, in source order.
    pub fn states(&self) -> Vec<DeliveryState> {
        self.ledger()
            .records
            .iter()
            .map(|(_, state)| *state)
            .collect()
    }

    /// Whether every record was acknowledged.
    pub fn all_acknowledged(&self) -> bool {
        self.ledger()
            .records
            .iter()
            .all(|(_, state)| *state == DeliveryState::Acknowledged)
    }
}

impl<R: Clone> Deliveries<R> {
    /// Acknowledged records, in acknowledgement order.
    pub fn acknowledged(&self) -> Vec<R> {
        let ledger = self.ledger();
        ledger
            .acknowledgements
            .iter()
            .map(|&index| ledger.records[index].0.clone())
            .collect()
    }

    /// Records received but not acknowledged, in source order.
    pub fn unacknowledged(&self) -> Vec<R> {
        self.ledger()
            .records
            .iter()
            .filter(|(_, state)| *state == DeliveryState::Received)
            .map(|(record, _)| record.clone())
            .collect()
    }
}
