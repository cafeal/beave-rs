//! Batches a transactional subscription's deliveries into sink transactions.
use super::{
    config::TransactionBatch,
    instruments::{Instruments, Stage},
};
use crate::{
    message::SourceMessage,
    retry::RetryPolicy,
    shutdown::CancellationToken,
    sink::{self, PublishRejected},
    transaction::{TransactionEntry, TransactionalSink},
};
use std::{marker::PhantomData, sync::Arc, time::Instant};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::sleep_until,
};
use tracing::{Instrument, Span, debug, info_span};

/// A delivery waiting in a batch, with its prepared outputs.
struct Entry<M, P> {
    delivery: M,
    outputs: Vec<P>,
    revocation: Option<CancellationToken>,
    enlisted: Instant,
    done: oneshot::Sender<anyhow::Result<()>>,
}

impl<M, P> Entry<M, P> {
    /// The runtime no longer waits for this delivery, because it was revoked
    /// or its job was aborted, so committing it is unnecessary.
    fn abandoned(&self) -> bool {
        self.done.is_closed()
            || self
                .revocation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
    }
}

/// Collects deliveries into batches and commits them one batch at a time in a
/// task of its own, so a batch's commit finishes even when every job waiting
/// for it is dropped.
pub(super) struct Batcher<M, P> {
    entries: Enlister<M, P>,
    task: JoinHandle<()>,
}

/// Adds deliveries to the batches of a [`Batcher`].
pub(super) struct Enlister<M, P>(mpsc::Sender<Entry<M, P>>);

impl<M, P> Clone for Enlister<M, P> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M, P> Batcher<M, P>
where
    M: SourceMessage + Sync,
    P: Send + Sync + 'static,
{
    pub(super) fn spawn<O, K>(
        sink: Arc<K>,
        batch: TransactionBatch,
        policy: RetryPolicy,
        instruments: Instruments,
    ) -> Self
    where
        O: Send + 'static,
        K: TransactionalSink<M, O, Prepared = P>,
    {
        // The channel holds at most one further batch while a batch commits.
        // Enlisting waits for capacity, which bounds the deliveries waiting for
        // a commit outside `max_in_flight`.
        let (entries, receiver) = mpsc::channel(batch.max_deliveries);
        let committer = Committer {
            sink,
            batch,
            policy,
            instruments,
            marker: PhantomData::<fn(O)>,
        };
        let task = tokio::spawn(committer.run(receiver).instrument(Span::current()));
        Self {
            entries: Enlister(entries),
            task,
        }
    }

    pub(super) fn enlister(&self) -> Enlister<M, P> {
        self.entries.clone()
    }

    /// Commits the deliveries already enlisted and stops once every enlister
    /// is dropped.
    pub(super) async fn close(self) -> anyhow::Result<()> {
        drop(self.entries);
        Ok(self.task.await?)
    }
}

impl<M: SourceMessage, P> Enlister<M, P> {
    /// Adds `delivery` to a batch and returns the completion that resolves
    /// when the batch commits, or fails when it cannot be committed.
    pub(super) async fn enlist(
        &self,
        delivery: M,
        outputs: Vec<P>,
    ) -> anyhow::Result<sink::Completion> {
        let (done, committed) = oneshot::channel();
        let entry = Entry {
            revocation: delivery.revocation(),
            delivery,
            outputs,
            enlisted: Instant::now(),
            done,
        };
        self.0
            .send(entry)
            .await
            .map_err(|_| anyhow::anyhow!("transaction batching has stopped"))?;
        Ok(sink::Completion::pending(async move {
            committed
                .await
                .unwrap_or_else(|_| Err(anyhow::anyhow!("transaction batch was abandoned")))
        }))
    }
}

struct Committer<K, O> {
    sink: Arc<K>,
    batch: TransactionBatch,
    policy: RetryPolicy,
    instruments: Instruments,
    marker: PhantomData<fn(O)>,
}

impl<K, O> Committer<K, O>
where
    O: Send + 'static,
{
    async fn run<M>(self, mut receiver: mpsc::Receiver<Entry<M, K::Prepared>>)
    where
        M: SourceMessage + Sync,
        K: TransactionalSink<M, O>,
    {
        // After a batch fails, later batches must not commit: a later
        // delivery's acknowledgement would cover the failed one of its scope.
        let mut failure: Option<String> = None;
        while let Some(first) = receiver.recv().await {
            let deadline = (first.enlisted + self.batch.max_linger).into();
            let mut members = vec![first];
            while members.len() < self.batch.max_deliveries {
                tokio::select! {
                    biased;
                    entry = receiver.recv() => match entry {
                        Some(entry) => members.push(entry),
                        None => break,
                    },
                    _ = sleep_until(deadline) => break,
                }
            }
            if failure.is_none()
                && let Err(error) = self.commit(&mut members).await
            {
                failure = Some(format!("{error:#}"));
            }
            for member in members {
                let result = match &failure {
                    Some(error) => Err(anyhow::anyhow!("{error}")),
                    None => Ok(()),
                };
                let _ = member.done.send(result);
            }
        }
    }

    /// Commits the batch, retrying under the publish retry policy. Abandoned
    /// deliveries leave the batch before each attempt; a failure caused by a
    /// revocation during the attempt is retried without the revoked deliveries
    /// and without counting against the policy. A rejection is not retried:
    /// it cannot be attributed to one delivery of the batch, so it fails the
    /// whole batch.
    async fn commit<M>(&self, members: &mut Vec<Entry<M, K::Prepared>>) -> anyhow::Result<()>
    where
        M: SourceMessage + Sync,
        K: TransactionalSink<M, O>,
    {
        let mut attempt = 1;
        let mut retrying = None;
        loop {
            members.retain(|member| !member.abandoned());
            if members.is_empty() {
                return Ok(());
            }
            let started = Instant::now();
            let result = {
                let batch: Vec<_> = members
                    .iter()
                    .map(|member| TransactionEntry {
                        delivery: &member.delivery,
                        outputs: &member.outputs,
                    })
                    .collect();
                let outputs: usize = members.iter().map(|member| member.outputs.len()).sum();
                self.sink
                    .commit(&batch)
                    .instrument(info_span!("commit", deliveries = batch.len(), outputs))
                    .await
            };
            let error = match result {
                Ok(()) => {
                    self.instruments.record(Stage::Commit, started);
                    self.instruments
                        .transaction_deliveries
                        .record(members.len() as f64);
                    return Ok(());
                }
                Err(error) => error,
            };
            self.instruments.publish_failures.increment(1);
            if PublishRejected::is(&error) {
                return Err(error.context("a transaction batch cannot route a rejected output"));
            }
            if members.iter().any(Entry::abandoned) {
                debug!(
                    error = format!("{error:#}"),
                    "retrying commit without revoked deliveries"
                );
                continue;
            }
            if attempt >= self.policy.max_attempts {
                return Err(error.context("publish retry exhausted"));
            }
            debug!(attempt, error = format!("{error:#}"), "retrying commit");
            if retrying.is_none() {
                retrying = Some(self.instruments.health.publish_retry());
            }
            tokio::time::sleep(self.policy.delay(attempt)).await;
            attempt += 1;
        }
    }
}
