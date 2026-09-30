//! Synchronous handlers executed on a dedicated, bounded worker pool.
//!
//! Wrap a synchronous function with [`blocking`] to register it wherever an async
//! [`Handler`] is accepted. The call runs on a pool thread, so it can block without
//! stalling the Tokio executor that drives receiving, publishing, and ACK.
//!
//! ```
//! use beavers::{App, InMemorySink, IterSource, Result, blocking};
//!
//! fn checksum(line: String) -> Result<u32> {
//!     Ok(line.bytes().map(u32::from).sum())
//! }
//!
//! let app = App::new().subscribe(
//!     "checksums",
//!     IterSource::new(["a".to_string()]),
//!     InMemorySink::default(),
//!     blocking(checksum),
//! );
//! # drop(app);
//! ```
use crate::handler::{Handler, HandlerError, Result};
use std::{
    any::Any,
    future::Future,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, PoisonError},
    thread,
};
use tokio::sync::{mpsc, oneshot};

type Job = Box<dyn FnOnce() + Send>;
type JobQueue = Arc<Mutex<mpsc::Receiver<Job>>>;

/// Wraps a synchronous handler so it runs on its own [`BlockingPool::default`] pool.
///
/// Use [`BlockingPool::blocking`] to share a pool between handlers or to choose its size.
pub fn blocking<F>(handler: F) -> Blocking<F> {
    BlockingPool::default().blocking(handler)
}

/// A synchronous handler bound to a [`BlockingPool`]; created by [`blocking`] or
/// [`BlockingPool::blocking`].
///
/// Each invocation waits for queue capacity, runs the function on a pool thread, and
/// resolves to its result. A panic becomes [`HandlerError::Fatal`], so the delivery is
/// not acknowledged and the subscription stops; the worker thread survives.
pub struct Blocking<F> {
    handler: Arc<F>,
    pool: BlockingPool,
}

impl<F> Clone for Blocking<F> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            pool: self.pool.clone(),
        }
    }
}

impl<I, O, F> Handler<I> for Blocking<F>
where
    F: Fn(I) -> Result<O> + Send + Sync + 'static,
    I: Send + 'static,
    O: Send + Sync + 'static,
{
    type Output = O;

    fn handle(&self, input: I) -> impl Future<Output = Result<O>> + Send {
        let handler = self.handler.clone();
        let pool = self.pool.clone();
        async move {
            let (result_tx, result_rx) = oneshot::channel();
            pool.submit(Box::new(move || {
                // The waiter was cancelled while the job was queued: skip it.
                if result_tx.is_closed() {
                    return;
                }
                let result = catch_unwind(AssertUnwindSafe(|| handler(input)))
                    .unwrap_or_else(|panic| Err(panicked(panic)));
                // A result produced after the waiter was cancelled is dropped.
                let _ = result_tx.send(result);
            }))
            .await?;
            result_rx.await.unwrap_or_else(|_| {
                Err(HandlerError::Fatal(anyhow::anyhow!(
                    "blocking worker dropped the job"
                )))
            })
        }
    }
}

/// A cloneable handle to a fixed set of worker threads with a bounded job queue.
///
/// Threads start on the first submitted job. They exit after their current job once
/// every clone of the pool and every handler using it has been dropped, which happens
/// when the subscriptions owning those handlers finish.
#[derive(Clone)]
pub struct BlockingPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    workers: usize,
    queue_capacity: usize,
    queue: Mutex<Option<mpsc::Sender<Job>>>,
}

impl Default for BlockingPool {
    /// One worker per available CPU, with a queue of the same size.
    fn default() -> Self {
        let workers = thread::available_parallelism().map_or(1, NonZeroUsize::get);
        Self::from_sizes(workers, workers)
    }
}

impl BlockingPool {
    /// A pool with `workers` threads and a queue of `workers` jobs.
    pub fn new(workers: usize) -> anyhow::Result<Self> {
        Self::with_queue_capacity(workers, workers)
    }

    /// A pool with `workers` threads and room for `queue_capacity` jobs waiting for a
    /// thread. Submitting to a full queue waits asynchronously for space.
    pub fn with_queue_capacity(workers: usize, queue_capacity: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(
            workers > 0,
            "blocking pool workers must be greater than zero"
        );
        anyhow::ensure!(
            queue_capacity > 0,
            "blocking pool queue capacity must be greater than zero"
        );
        Ok(Self::from_sizes(workers, queue_capacity))
    }

    fn from_sizes(workers: usize, queue_capacity: usize) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                workers,
                queue_capacity,
                queue: Mutex::new(None),
            }),
        }
    }

    /// Wraps a synchronous handler so it runs on this pool.
    pub fn blocking<F>(&self, handler: F) -> Blocking<F> {
        Blocking {
            handler: Arc::new(handler),
            pool: self.clone(),
        }
    }

    pub fn workers(&self) -> usize {
        self.inner.workers
    }

    pub fn queue_capacity(&self) -> usize {
        self.inner.queue_capacity
    }

    async fn submit(&self, job: Job) -> Result<()> {
        let queue = self.queue().map_err(HandlerError::Fatal)?;
        queue
            .send(job)
            .await
            .map_err(|_| HandlerError::Fatal(anyhow::anyhow!("blocking pool workers have stopped")))
    }

    /// Returns the job queue, starting the worker threads on first use.
    fn queue(&self) -> anyhow::Result<mpsc::Sender<Job>> {
        let mut queue = self
            .inner
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(sender) = queue.as_ref() {
            return Ok(sender.clone());
        }
        let (sender, receiver) = mpsc::channel(self.inner.queue_capacity);
        let receiver: JobQueue = Arc::new(Mutex::new(receiver));
        for index in 0..self.inner.workers {
            let receiver = receiver.clone();
            thread::Builder::new()
                .name(format!("beavers-blocking-{index}"))
                .spawn(move || work(&receiver))
                .map_err(|error| {
                    anyhow::Error::new(error).context("failed to start a blocking worker")
                })?;
        }
        *queue = Some(sender.clone());
        Ok(sender)
    }
}

fn work(queue: &Mutex<mpsc::Receiver<Job>>) {
    loop {
        // The lock is released before the job runs, so other workers can take jobs.
        let job = queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .blocking_recv();
        match job {
            Some(job) => job(),
            None => return,
        }
    }
}

fn panicked(panic: Box<dyn Any + Send>) -> HandlerError {
    let message = panic
        .downcast_ref::<&str>()
        .map(|message| message.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_string());
    HandlerError::Fatal(anyhow::anyhow!("blocking handler panicked: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc as std_mpsc, time::Duration};

    #[tokio::test]
    async fn cancelled_queued_jobs_never_run() {
        let pool = BlockingPool::new(1).unwrap();
        let (release_tx, release_rx) = std_mpsc::channel::<()>();
        let release_rx = Mutex::new(release_rx);
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let handler = pool.blocking(move |n: i32| {
            started_tx.send(n).unwrap();
            if n == 1 {
                release_rx.lock().unwrap().recv().unwrap();
            }
            Ok(n)
        });
        let first = tokio::spawn({
            let handler = handler.clone();
            async move { handler.handle(1).await }
        });
        assert_eq!(started_rx.recv().await, Some(1));
        // Queued behind the busy worker, then cancelled by the timeout.
        let queued = tokio::time::timeout(Duration::from_millis(20), handler.handle(2)).await;
        assert!(queued.is_err());
        release_tx.send(()).unwrap();
        assert_eq!(first.await.unwrap().unwrap(), 1);
        assert_eq!(handler.handle(3).await.unwrap(), 3);
        assert_eq!(started_rx.recv().await, Some(3));
    }

    #[tokio::test]
    async fn panics_become_fatal_and_keep_the_worker() {
        let pool = BlockingPool::new(1).unwrap();
        let handler = pool.blocking(|n: i32| {
            assert!(n >= 0, "negative input");
            Ok(n)
        });
        let error = handler.handle(-1).await.unwrap_err();
        let HandlerError::Fatal(error) = error else {
            panic!("expected a fatal error");
        };
        assert!(error.to_string().contains("negative input"));
        assert_eq!(handler.handle(4).await.unwrap(), 4);
    }

    #[test]
    fn zero_sizes_are_invalid() {
        assert!(BlockingPool::new(0).is_err());
        assert!(BlockingPool::with_queue_capacity(1, 0).is_err());
    }
}
