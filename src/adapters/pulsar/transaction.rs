use super::{
    producer::Producers,
    record::PulsarPublish,
    sink::{Connection, PulsarPrepared, prepare},
    source::{Acknowledgement, PulsarMessage},
};
use crate::{
    codec::{Decoder, Encoder},
    sink::Sink,
    transaction::TransactionalSink,
};
use futures_util::future::try_join_all;
use magnetar::{Transaction, TxnState};
use std::{collections::HashSet, marker::PhantomData, sync::Arc};

/// Publishes Pulsar messages in transactions.
///
/// Created with [`PulsarSink::transactional`](super::PulsarSink::transactional).
/// With [`Subscription::transactional`](crate::Subscription::transactional)
/// and a [`PulsarSource`](super::PulsarSource), each transaction contains a
/// delivery's outputs and its acknowledgement. Used as a plain [`Sink`], each
/// publication is a transaction of its own and becomes visible to consumers
/// when it commits.
pub struct PulsarTransactionalSink<C, T> {
    connection: Arc<Connection>,
    codec: Arc<C>,
    marker: PhantomData<fn(T)>,
}

impl<C, T> Clone for PulsarTransactionalSink<C, T> {
    fn clone(&self) -> Self {
        Self {
            connection: self.connection.clone(),
            codec: self.codec.clone(),
            marker: PhantomData,
        }
    }
}

impl<C, T> PulsarTransactionalSink<C, T> {
    pub(super) fn new(connection: Connection, codec: Arc<C>) -> Self {
        Self {
            connection: Arc::new(connection),
            codec,
            marker: PhantomData,
        }
    }

    /// Runs one transaction in its own task, so dropping the caller neither
    /// leaves the transaction open nor lets `close` release the producers
    /// while it is in use.
    async fn transact(
        &self,
        outputs: Vec<PulsarPrepared>,
        acknowledgement: Option<SourceAcknowledgement>,
    ) -> anyhow::Result<()> {
        let connection = self.connection.clone();
        tokio::spawn(async move {
            let producers = connection.producers().await?;
            let client = &producers.client;
            let transaction = client
                .new_transaction(connection.config().transaction_timeout)
                .await?;
            if let Err(error) = run(&producers, transaction, &outputs, acknowledgement).await {
                // The coordinator also aborts the transaction when it times
                // out, so a failed abort only delays the cleanup.
                let _ = client.abort_transaction(transaction).await;
                return Err(error);
            }
            let state = client.commit_transaction(transaction).await?;
            anyhow::ensure!(
                state == TxnState::Committed,
                "Pulsar transaction ended as {state:?} instead of committing"
            );
            Ok(())
        })
        .await?
    }
}

impl<C, T> Sink<PulsarPublish<T>> for PulsarTransactionalSink<C, T>
where
    C: Encoder<T>,
    T: Send + Sync + 'static,
{
    type Prepared = PulsarPrepared;

    fn prepare(&self, output: PulsarPublish<T>) -> anyhow::Result<Self::Prepared> {
        prepare(&*self.codec, output)
    }

    async fn publish(&self, output: &Self::Prepared) -> anyhow::Result<()> {
        self.transact(vec![output.clone()], None).await
    }

    async fn close(&self) -> anyhow::Result<()> {
        self.connection.close().await
    }
}

impl<C, T, D, U> TransactionalSink<PulsarMessage<D, U>, PulsarPublish<T>>
    for PulsarTransactionalSink<C, T>
where
    C: Encoder<T>,
    T: Send + Sync + 'static,
    D: Decoder<U> + Send + Sync + 'static,
    U: Clone + Send + Sync + 'static,
{
    async fn commit(
        &self,
        delivery: &PulsarMessage<D, U>,
        outputs: &[Self::Prepared],
    ) -> anyhow::Result<()> {
        self.transact(outputs.to_vec(), Some(SourceAcknowledgement::new(delivery)))
            .await
    }
}

/// The individual acknowledgement a transaction commits for one delivery.
struct SourceAcknowledgement {
    acknowledgement: Acknowledgement,
    topic: String,
}

impl SourceAcknowledgement {
    fn new<D, U>(delivery: &PulsarMessage<D, U>) -> Self {
        Self {
            acknowledgement: delivery.acknowledgement.clone(),
            topic: delivery.topic().to_owned(),
        }
    }
}

/// Registers every partition the outputs are routed to and the delivery's
/// subscription with the transaction, then publishes the outputs and
/// acknowledges the delivery within it.
async fn run(
    producers: &Producers,
    transaction: Transaction,
    outputs: &[PulsarPrepared],
    acknowledgement: Option<SourceAcknowledgement>,
) -> anyhow::Result<()> {
    let client = &producers.client;
    let routes: Vec<_> = outputs
        .iter()
        .map(|output| (producers.route(output), output))
        .collect();
    let mut registered = HashSet::new();
    for (partition, _) in &routes {
        if registered.insert(partition.topic.as_str()) {
            client
                .register_partition_to_transaction(transaction, &partition.topic)
                .await?;
        }
    }
    try_join_all(
        routes
            .iter()
            .map(|(partition, output)| partition.send(output, Some(transaction.id()))),
    )
    .await?;
    if let Some(SourceAcknowledgement {
        acknowledgement,
        topic,
    }) = acknowledgement
    {
        acknowledgement.ensure_open()?;
        client
            .register_subscription_to_transaction(
                transaction,
                topic,
                &*acknowledgement.subscription,
            )
            .await?;
        acknowledgement
            .consumer
            .ack_with_txn(acknowledgement.id, transaction.id())
            .await?;
    }
    Ok(())
}
