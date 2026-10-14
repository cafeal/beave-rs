use super::{
    client::Sqs,
    config::{SourceConfig, is_fifo},
    convert::attributes_from_sqs,
    lease::{Leases, keep, release},
    record::{SqsMetadata, SqsRecord},
};
use crate::{
    codec::Decoder,
    error::{BoxError, Error},
    message::{OrderingKey, SourceMessage},
    retry::RetryPolicy,
    shutdown::CancellationToken,
    source::{Receive, ReceiveError, Source},
};
use aws_sdk_sqs::{
    Client,
    error::{DisplayErrorContext, ProvideErrorMetadata},
    types::{Message, MessageSystemAttributeName},
};
use std::{collections::VecDeque, convert::Infallible, sync::Arc, time::Duration};
use tokio::{sync::mpsc, task::JoinHandle, time::Instant};

/// Error code of a receipt handle that no longer refers to a received message.
const INVALID_RECEIPT_HANDLE: &str = "ReceiptHandleIsInvalid";

/// Consumes one queue with long polling.
///
/// Built with [`Sqs::source`]. It starts polling on the first `receive`, which
/// also validates the settings.
pub struct SqsSource<C> {
    sqs: Sqs,
    config: SourceConfig,
    codec: Arc<C>,
    connection: Option<Connection>,
    closed: bool,
}

/// A received message waiting in the source's buffer with its lease.
struct Received {
    message: Message,
    lease: u64,
    revoked: CancellationToken,
}

struct Connection {
    client: Client,
    leases: Arc<Leases>,
    released: mpsc::UnboundedSender<Arc<str>>,
    keeper: Keeper,
    buffered: VecDeque<Received>,
    /// The `ReceiveMessage` call in progress. It runs as its own task, so a
    /// cancelled `receive` leaves it running and the next `receive` takes its
    /// messages.
    polling: Option<JoinHandle<Result<Vec<Received>, BoxError>>>,
}

/// The task that extends leases and releases abandoned messages. Dropping it
/// stops the task.
struct Keeper {
    stop: CancellationToken,
    task: JoinHandle<()>,
}

impl Keeper {
    /// Stops the task once it has released the messages abandoned so far.
    async fn stop(mut self) {
        self.stop.cancel();
        let _ = (&mut self.task).await;
    }
}

impl Drop for Keeper {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

impl<C> SqsSource<C> {
    pub(super) fn new(sqs: Sqs, config: SourceConfig, codec: C) -> Self {
        Self {
            sqs,
            config,
            codec: Arc::new(codec),
            connection: None,
            closed: false,
        }
    }

    /// Messages one `ReceiveMessage` call returns at most, from 1 to 10.
    /// Defaults to 10.
    pub fn max_messages(mut self, max_messages: i32) -> Self {
        self.config.max_messages = max_messages;
        self
    }

    /// How long `ReceiveMessage` waits for a message (long polling), up to 20
    /// seconds. Defaults to 20 seconds.
    pub fn wait_time(mut self, wait_time: Duration) -> Self {
        self.config.wait_time = wait_time;
        self
    }

    /// How long a received message stays invisible to other consumers. The
    /// source extends it while the delivery is buffered or in flight, up to
    /// SQS's limit of 12 hours from its receipt. Defaults to 30 seconds.
    pub fn visibility_timeout(mut self, visibility_timeout: Duration) -> Self {
        self.config.visibility_timeout = visibility_timeout;
        self
    }

    /// Attempts to delete a message when it is acknowledged. Defaults to 5
    /// attempts with delays from 100 ms up to 2 s.
    pub fn ack_retry(mut self, ack_retry: RetryPolicy) -> Self {
        self.config.ack_retry = ack_retry;
        self
    }

    /// Checks the handle and source settings without contacting SQS.
    ///
    /// Fails when the handle is invalid, the queue URL is not an http or https
    /// URL, `max_messages` is outside 1 to 10, `wait_time` exceeds 20 seconds,
    /// `visibility_timeout` is outside 1 second to 12 hours, or `ack_retry` is
    /// invalid.
    pub fn validate(&self) -> Result<(), Error> {
        self.sqs.validate()?;
        self.config.validate()
    }

    async fn connect(&self) -> Result<Connection, BoxError> {
        let client = self.sqs.client().await?.clone();
        let leases = Arc::new(Leases::new(self.config.visibility_timeout));
        let (released, releases) = mpsc::unbounded_channel();
        let stop = CancellationToken::new();
        let task = tokio::spawn(keep(
            client.clone(),
            self.config.queue_url.clone(),
            leases.clone(),
            releases,
            stop.clone(),
        ));
        Ok(Connection {
            client,
            leases,
            released,
            keeper: Keeper { stop, task },
            buffered: VecDeque::new(),
            polling: None,
        })
    }

    /// Starts a `ReceiveMessage` call whose messages get leases as they arrive.
    fn poll(&self, connection: &Connection) -> JoinHandle<Result<Vec<Received>, BoxError>> {
        let request = connection
            .client
            .receive_message()
            .queue_url(&self.config.queue_url)
            .max_number_of_messages(self.config.max_messages)
            .wait_time_seconds(self.config.wait_time.as_secs() as i32)
            .visibility_timeout(self.config.visibility_timeout.as_secs() as i32)
            .message_system_attribute_names(MessageSystemAttributeName::All)
            .message_attribute_names("All");
        let leases = connection.leases.clone();
        tokio::spawn(async move {
            let requested = Instant::now();
            let output = request.send().await.map_err(|error| {
                Error::msg(format!(
                    "receiving from SQS: {}",
                    DisplayErrorContext(&error)
                ))
            })?;
            Ok(output
                .messages
                .unwrap_or_default()
                .into_iter()
                .filter_map(|message| {
                    let (lease, revoked) = leases.add(message.receipt_handle()?, requested);
                    Some(Received {
                        message,
                        lease,
                        revoked,
                    })
                })
                .collect())
        })
    }
}

impl<C: Decoder<Value: Clone + Send + Sync + 'static>> Source for SqsSource<C> {
    type Message = SqsMessage<C>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        if self.closed {
            return Ok(Receive::End);
        }
        if self.connection.is_none() {
            self.validate()
                .map_err(|error| ReceiveError::Fatal(error.into()))?;
            self.connection = Some(self.connect().await.map_err(ReceiveError::Fatal)?);
        }
        loop {
            let connection = self.connection.as_mut().expect("connected source");
            if let Some(received) = connection.buffered.pop_front() {
                let message = SqsMessage::new(self, received);
                return Ok(Receive::Message(message));
            }
            if connection.polling.is_none() {
                let polling = self.poll(self.connection.as_ref().expect("connected source"));
                self.connection.as_mut().expect("connected source").polling = Some(polling);
            }
            let connection = self.connection.as_mut().expect("connected source");
            let polled = connection.polling.as_mut().expect("polling").await;
            connection.polling = None;
            match polled {
                Ok(Ok(messages)) => connection.buffered.extend(messages),
                Ok(Err(error)) => return Err(ReceiveError::Retry(error)),
                Err(error) => return Err(ReceiveError::Retry(error.into())),
            }
        }
    }

    /// Stops extending visibility and makes the messages that were received
    /// but not acknowledged visible again.
    async fn close(&mut self) -> Result<(), BoxError> {
        self.closed = true;
        let Some(connection) = self.connection.take() else {
            return Ok(());
        };
        if let Some(polling) = connection.polling {
            // A call that already returned keeps its messages' leases, so
            // they are released below with the others.
            polling.abort();
            let _ = polling.await;
        }
        connection.keeper.stop().await;
        let handles = connection.leases.drain();
        release(&connection.client, &self.config.queue_url, &handles).await;
        Ok(())
    }
}

/// One SQS message. Dropping it without acknowledging it ends its visibility
/// extension and makes the message visible again for another receive.
pub struct SqsMessage<C> {
    message: Message,
    queue_url: String,
    fifo: bool,
    client: Client,
    leases: Arc<Leases>,
    released: mpsc::UnboundedSender<Arc<str>>,
    lease: u64,
    revoked: CancellationToken,
    ack_retry: RetryPolicy,
    codec: Arc<C>,
    acknowledged: bool,
}

impl<C> SqsMessage<C> {
    fn new(source: &SqsSource<C>, received: Received) -> Self {
        let connection = source.connection.as_ref().expect("connected source");
        Self {
            message: received.message,
            queue_url: source.config.queue_url.clone(),
            fifo: is_fifo(&source.config.queue_url),
            client: connection.client.clone(),
            leases: connection.leases.clone(),
            released: connection.released.clone(),
            lease: received.lease,
            revoked: received.revoked,
            ack_retry: source.config.ack_retry.clone(),
            codec: source.codec.clone(),
            acknowledged: false,
        }
    }

    fn system_attribute(&self, name: MessageSystemAttributeName) -> Option<&str> {
        self.message.attributes()?.get(&name).map(String::as_str)
    }

    fn record<V, E>(&self, value: impl FnOnce(&[u8]) -> Result<V, E>) -> Result<SqsRecord<V>, E> {
        let number = |name| {
            self.system_attribute(name)
                .and_then(|value| value.parse::<i64>().ok())
        };
        let text = |name| self.system_attribute(name).map(str::to_owned);
        Ok(SqsRecord {
            value: value(self.message.body().unwrap_or_default().as_bytes())?,
            attributes: attributes_from_sqs(self.message.message_attributes()),
            metadata: SqsMetadata {
                queue_url: self.queue_url.clone(),
                message_id: self.message.message_id().unwrap_or_default().to_owned(),
                receive_count: number(MessageSystemAttributeName::ApproximateReceiveCount)
                    .and_then(|count| u32::try_from(count).ok())
                    .unwrap_or(1),
                sent_timestamp: number(MessageSystemAttributeName::SentTimestamp),
                first_receive_timestamp: number(
                    MessageSystemAttributeName::ApproximateFirstReceiveTimestamp,
                ),
                message_group_id: text(MessageSystemAttributeName::MessageGroupId),
                deduplication_id: text(MessageSystemAttributeName::MessageDeduplicationId),
                sequence_number: text(MessageSystemAttributeName::SequenceNumber),
            },
        })
    }
}

impl<C: Decoder<Value: Clone + Send + Sync + 'static>> SourceMessage for SqsMessage<C> {
    type Item = SqsRecord<C::Value>;
    type Raw = SqsRecord<Vec<u8>>;

    fn decode(&self) -> Result<Self::Item, BoxError> {
        self.record(|bytes| self.codec.decode(bytes))
    }

    fn raw(&self) -> Self::Raw {
        let Ok(record) = self.record(|bytes| Ok::<_, Infallible>(bytes.to_vec()));
        record
    }

    /// Deletes the message, retrying under `ack_retry`. A message whose lease
    /// ended, because its visibility could not be extended in time, is revoked
    /// instead: SQS may already have handed it to another consumer.
    async fn ack(mut self) -> Result<(), BoxError> {
        self.acknowledged = true;
        let Some(receipt_handle) = self.leases.remove(self.lease) else {
            self.revoked.cancel();
            return Err(Error::msg(
                "SQS visibility of the message expired before the acknowledgement",
            )
            .into());
        };
        let mut attempt = 0;
        loop {
            attempt += 1;
            let result = self
                .client
                .delete_message()
                .queue_url(&self.queue_url)
                .receipt_handle(receipt_handle.as_ref())
                .send()
                .await;
            let error = match result {
                Ok(_) => return Ok(()),
                Err(error) => error,
            };
            if error.code() == Some(INVALID_RECEIPT_HANDLE) {
                self.revoked.cancel();
            }
            if self.revoked.is_cancelled() || attempt >= self.ack_retry.max_attempts {
                return Err(Error::msg(format!(
                    "deleting an SQS message: {}",
                    DisplayErrorContext(&error)
                ))
                .into());
            }
            tokio::time::sleep(self.ack_retry.delay(attempt)).await;
        }
    }

    /// The message group of a FIFO queue message. Other queues deliver in no order.
    fn ordering_key(&self) -> Option<OrderingKey> {
        let group = self.system_attribute(MessageSystemAttributeName::MessageGroupId)?;
        self.fifo
            .then(|| OrderingKey::new(self.queue_url.as_str(), 0).with_key(group.as_bytes()))
    }

    fn revocation(&self) -> Option<CancellationToken> {
        Some(self.revoked.clone())
    }

    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        let Some(attributes) = self.message.message_attributes() else {
            return Vec::new();
        };
        attributes
            .iter()
            .filter(|(_, value)| value.data_type().starts_with("String"))
            .filter_map(|(name, value)| Some((name.as_str(), value.string_value()?)))
            .collect()
    }
}

impl<C> Drop for SqsMessage<C> {
    fn drop(&mut self) {
        if self.acknowledged {
            return;
        }
        if let Some(receipt_handle) = self.leases.remove(self.lease) {
            let _ = self.released.send(receipt_handle);
        }
    }
}
