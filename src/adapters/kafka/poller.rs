use super::progress::{KafkaConsumer, SharedProgress};
use rdkafka::{
    error::KafkaError,
    message::{Message, OwnedMessage},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};
use tokio::sync::mpsc;

/// A record and the assignment generation of its partition when it was polled.
pub(super) type Polled = Result<(OwnedMessage, u64), KafkaError>;

/// How long one poll waits before checking whether the poller should stop.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Polls a consumer on a thread of its own and hands records to `receive`.
///
/// librdkafka runs rebalance callbacks inside `poll`, so a revoke that waits
/// for a graceful handoff blocks this thread instead of the subscription's
/// task, which keeps finishing and acknowledging deliveries meanwhile. The
/// channel holds one record, so the consumer is polled only as fast as the
/// subscription receives, as with a consumer polled by `receive` itself.
pub(super) struct Poller {
    records: mpsc::Receiver<Polled>,
    stop: Arc<AtomicBool>,
    thread: JoinHandle<()>,
}

impl Poller {
    pub(super) fn spawn(
        consumer: Arc<KafkaConsumer>,
        progress: Arc<SharedProgress>,
    ) -> std::io::Result<Self> {
        let (sender, records) = mpsc::channel(1);
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let thread = std::thread::Builder::new()
            .name("beavers-kafka-poll".into())
            .spawn(move || {
                // A dropped receiver also ends the thread, when the source is
                // dropped without closing.
                while !stopped.load(Ordering::Acquire) && !sender.is_closed() {
                    let polled = match consumer.poll(POLL_INTERVAL) {
                        None => continue,
                        Some(Err(error)) => Err(error),
                        Some(Ok(record)) => {
                            let record = record.detach();
                            let key = (record.topic().to_owned(), record.partition());
                            // A record of a partition that is not assigned is stale.
                            let Some(generation) = progress.lock().generation(&key) else {
                                continue;
                            };
                            Ok((record, generation))
                        }
                    };
                    if sender.blocking_send(polled).is_err() {
                        return;
                    }
                }
            })?;
        Ok(Self {
            records,
            stop,
            thread,
        })
    }

    /// The next polled record, or `None` once the polling thread has ended.
    /// Cancellation-safe: a dropped call loses no record.
    pub(super) async fn recv(&mut self) -> Option<Polled> {
        self.records.recv().await
    }

    /// Stops polling and waits for the thread, which may be finishing a
    /// rebalance callback.
    pub(super) async fn stop(self) {
        self.stop.store(true, Ordering::Release);
        // Dropping the receiver ends a send that waits for room.
        drop(self.records);
        let thread = self.thread;
        let _ = tokio::task::spawn_blocking(move || thread.join()).await;
    }
}
