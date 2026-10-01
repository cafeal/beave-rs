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
use magnetar::{Transaction, TxnState, proto::pb::ServerError, runtime_tokio::ClientError};
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
            let transaction = match producers
                .client
                .new_transaction(connection.config().transaction_timeout)
                .await
            {
                Ok(transaction) => transaction,
                Err(error) => {
                    // The client performs its transaction coordinator
                    // handshake only once, and a coordinator reloaded by a
                    // broker restart answers every new transaction with
                    // "transaction not found" until a client repeats it. A
                    // new client does, so the retry connects one.
                    connection.replace(producers).await;
                    return Err(error.into());
                }
            };
            let client = &producers.client;
            match run(&producers, transaction, &outputs, acknowledgement).await {
                Ok(Acknowledged::InTransaction) => {}
                Ok(Acknowledged::Before) => {
                    // An earlier transaction committed this delivery's
                    // outputs and acknowledgement, and the broker redelivered
                    // it before learning of the commit. Its outputs are
                    // dropped, and the delivery is done.
                    let _ = client.abort_transaction(transaction).await;
                    return Ok(());
                }
                Err(error) => {
                    // The coordinator also aborts the transaction when it
                    // times out, so a failed abort only delays the cleanup.
                    let _ = client.abort_transaction(transaction).await;
                    return Err(error);
                }
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
    async fn verify_source(&self, delivery: &PulsarMessage<D, U>) -> anyhow::Result<()> {
        let source = &*delivery.acknowledgement.service_url;
        let sink = &self.connection.config().service_url;
        anyhow::ensure!(
            same_service(source, sink),
            "Pulsar source service URL {source} differs from sink service URL {sink}; \
             a transactional sink must use the source's service URL"
        );
        Ok(())
    }

    async fn commit(
        &self,
        delivery: &PulsarMessage<D, U>,
        outputs: &[Self::Prepared],
    ) -> anyhow::Result<()> {
        self.transact(outputs.to_vec(), Some(SourceAcknowledgement::new(delivery)))
            .await
    }
}

/// Whether two service URLs name the same Pulsar service. The Pulsar protocol
/// does not report a cluster identity, and a transaction coordinator only
/// commits acknowledgements on its own cluster, so a transactional sink must use
/// its source's service URL. Case and a trailing slash are ignored.
fn same_service(source: &str, sink: &str) -> bool {
    source
        .trim_end_matches('/')
        .eq_ignore_ascii_case(sink.trim_end_matches('/'))
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

/// How a transaction's delivery was acknowledged.
enum Acknowledged {
    /// Within the transaction, or the transaction has no delivery.
    InTransaction,
    /// By a committed transaction before this one.
    Before,
}

/// Registers every partition the outputs are routed to and the delivery's
/// subscription with the transaction, then publishes the outputs and
/// acknowledges the delivery within it.
async fn run(
    producers: &Producers,
    transaction: Transaction,
    outputs: &[PulsarPrepared],
    acknowledgement: Option<SourceAcknowledgement>,
) -> anyhow::Result<Acknowledged> {
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
        if let Err(error) = acknowledgement
            .consumer
            .ack_with_txn(acknowledgement.id, transaction.id())
            .await
        {
            if acknowledged_before(&error) {
                return Ok(Acknowledged::Before);
            }
            return Err(error.into());
        }
    }
    Ok(Acknowledged::InTransaction)
}

/// Whether the broker rejected a transactional acknowledgement because a
/// committed acknowledgement already covers the message. The broker reports
/// this only through the message of a `TransactionConflict` error; a
/// conflict with a transaction that is still pending is retried instead.
fn acknowledged_before(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Broker { code, message }
            if *code == ServerError::TransactionConflict as i32
                && message.ends_with("already acked before.")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_conflict_with_a_committed_acknowledgement_counts_as_acknowledged() {
        let conflict = |message: &str| ClientError::Broker {
            code: ServerError::TransactionConflict as i32,
            message: message.to_owned(),
        };
        assert!(acknowledged_before(&conflict(
            "[persistent://public/default/in][s] Transaction:(0,264) try to ack \
             message:2033:9 (ackSet is null) already acked before."
        )));
        assert!(!acknowledged_before(&conflict(
            "[persistent://public/default/in][s] Transaction:(0,10) try to ack \
             message:39:0 (ackSet is null) in pending ack status."
        )));
        assert!(!acknowledged_before(&ClientError::Broker {
            code: ServerError::PersistenceError as i32,
            message: "already acked before.".to_owned(),
        }));
    }

    #[test]
    fn service_urls_match_regardless_of_case_and_trailing_slash() {
        assert!(same_service(
            "pulsar://localhost:6650",
            "PULSAR://LocalHost:6650/"
        ));
        assert!(!same_service(
            "pulsar://localhost:6650",
            "pulsar://localhost:6651"
        ));
        assert!(!same_service(
            "pulsar://localhost:6650",
            "pulsar+ssl://localhost:6650"
        ));
    }
}
