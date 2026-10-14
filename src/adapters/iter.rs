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
    /// A handle that counts the acknowledged deliveries of this source. It
    /// stays usable after the source moves into an [`App`](crate::App).
    pub fn acknowledgements(&self) -> Acknowledgements {
        Acknowledgements(self.acknowledgements.clone())
    }
}

/// Number of deliveries an [`IterSource`] has acknowledged, returned by
/// [`IterSource::acknowledgements`].
#[derive(Clone, Debug, Default)]
pub struct Acknowledgements(Arc<AtomicUsize>);

impl Acknowledgements {
    /// The number of acknowledged deliveries so far.
    pub fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
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
