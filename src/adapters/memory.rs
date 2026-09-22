use crate::sink::Sink;
use std::sync::{Arc, Mutex};

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
    pub fn values(&self) -> Vec<T> {
        self.0.lock().unwrap().clone()
    }
}
impl<T: Clone + Send + Sync + 'static> Sink<T> for InMemorySink<T> {
    type Prepared = T;
    fn prepare(&self, value: T) -> anyhow::Result<T> {
        Ok(value)
    }
    async fn publish(&self, value: &T) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(value.clone());
        Ok(())
    }
}
