use crate::error::BoxError;
use crate::sink::Sink;
use std::sync::{Arc, Mutex};

/// A sink that collects published values in memory, in publication order.
///
/// Clones share the same storage, so a clone kept by a test can read what the
/// subscription published.
pub struct InMemorySink<T>(Arc<Mutex<Vec<T>>>);
impl<T> Clone for InMemorySink<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T> Default for InMemorySink<T> {
    fn default() -> Self {
        Self(Arc::default())
    }
}
impl<T: Clone> InMemorySink<T> {
    /// A copy of the values published so far.
    pub fn values(&self) -> Vec<T> {
        self.0.lock().unwrap().clone()
    }
}
impl<T: Clone + Send + Sync + 'static> Sink<T> for InMemorySink<T> {
    type Prepared = T;
    fn prepare(&self, value: T) -> Result<T, BoxError> {
        Ok(value)
    }
    async fn publish(&self, value: &T) -> Result<(), BoxError> {
        self.0.lock().unwrap().push(value.clone());
        Ok(())
    }
}
