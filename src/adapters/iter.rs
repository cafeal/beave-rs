use crate::{
    message::Delivery,
    source::{Receive, ReceiveError, Source},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// A source that delivers the items of an iterator in order and ends when it
/// is exhausted.
///
/// Acknowledging a delivery increments a shared counter, which makes the
/// source useful for tests and examples.
pub struct IterSource<T> {
    items: Box<dyn Iterator<Item = T> + Send>,
    acknowledgements: Arc<AtomicUsize>,
}
impl<T> IterSource<T> {
    /// Creates a source that delivers each item of `items`.
    pub fn new<I: IntoIterator<Item = T>>(items: I) -> Self
    where
        I::IntoIter: Send + 'static,
    {
        Self {
            items: Box::new(items.into_iter()),
            acknowledgements: Arc::default(),
        }
    }
    /// Shared counter of acknowledged deliveries, incremented on each ACK.
    pub fn acknowledgements(&self) -> Arc<AtomicUsize> {
        self.acknowledgements.clone()
    }
}
impl<T: Clone + Send + Sync + 'static> Source for IterSource<T> {
    type Message = Delivery<T>;
    async fn receive(&mut self) -> Result<Receive<Delivery<T>>, ReceiveError> {
        Ok(match self.items.next() {
            Some(value) => {
                let counter = self.acknowledgements.clone();
                Receive::Message(Delivery::new(value, move || async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }))
            }
            None => Receive::End,
        })
    }
}
