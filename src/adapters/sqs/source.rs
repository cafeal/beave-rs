use super::{
    client::client,
    config::SqsSourceConfig,
    convert::attributes_from_sqs,
    lease::{Leases, keep, release},
    record::{SqsMetadata, SqsRecord},
};
use crate::{
    codec::Decoder,
    message::{OrderingKey, SourceMessage},
    shutdown::CancellationToken,
    source::{Receive, ReceiveError, Source},
};
use anyhow::Context as _;
use aws_sdk_sqs::{
    Client,
    error::{DisplayErrorContext, ProvideErrorMetadata},
    types::{Message, MessageSystemAttributeName},
};
use std::{collections::VecDeque, convert::Infallible, marker::PhantomData, sync::Arc};
use tokio::{sync::mpsc, task::JoinHandle, time::Instant};

/// Error code of a receipt handle that no longer refers to a received message.
const INVALID_RECEIPT_HANDLE: &str = "ReceiptHandleIsInvalid";

/// Consumes one queue with long polling. It creates its client on the first
/// `receive`.
pub struct SqsSource<C, T> {
    config: SqsSourceConfig,
    codec: Arc<C>,
    connection: Option<Connection>,
    closed: bool,
    marker: PhantomData<T>,
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
    keeper: CancellationToken,
    buffered: VecDeque<Received>,
    /// The `ReceiveMessage` call in progress. It runs as its own task, so a
    /// cancelled `receive` leaves it running and the next `receive` takes its
    /// messages.
    polling: Option<JoinHandle<anyhow::Result<Vec<Received>>>>,
}

impl<C: Default, T> SqsSource<C, T> {
    pub fn new(config: SqsSourceConfig) -> Self {
        Self::with_codec(config, C::default())
    }
}

impl<C, T> SqsSource<C, T> {
    pub fn with_codec(config: SqsSourceConfig, codec: C) -> Self {
        Self {
            config,
            codec: Arc::new(codec),
            connection: None,
            closed: false,
            marker: PhantomData,
        }
    }

    async fn connect(&self) -> Connection {
        let client = client(&self.config.endpoint()).await;
        let leases = Arc::new(Leases::new(self.config.visibility_timeout));
        let (released, releases) = mpsc::unbounded_channel();
        let keeper = CancellationToken::new();
        tokio::spawn(keep(
            client.clone(),
            self.config.queue_url.clone(),
            leases.clone(),
            releases,
            keeper.clone(),
        ));
        Connection {
            client,
            leases,
            released,
            keeper,
            buffered: VecDeque::new(),
            polling: None,
        }
    }

    /// Starts a `ReceiveMessage` call whose messages get leases as they arrive.
    fn poll(&self, connection: &Connection) -> JoinHandle<anyhow::Result<Vec<Received>>> {
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
            let output = request
                .send()
                .await
                .map_err(|error| anyhow::anyhow!("{}", DisplayErrorContext(&error)))
                .context("receiving from SQS")?;
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

impl<C: Decoder<T>, T: Clone + Send + Sync + 'static> Source for SqsSource<C, T> {
    type Message = SqsMessage<C, T>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        if self.closed {
            return Ok(Receive::End);
        }
        if self.connection.is_none() {
            self.config.validate().map_err(ReceiveError::Fatal)?;
            self.connection = Some(self.connect().await);
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
    async fn close(&mut self) -> anyhow::Result<()> {
        self.closed = true;
        let Some(connection) = self.connection.take() else {
            return Ok(());
        };
        connection.keeper.cancel();
        if let Some(polling) = connection.polling {
            polling.abort();
            if let Ok(Ok(messages)) = polling.await {
                for received in messages {
                    connection.leases.remove(received.lease);
                }
            }
        }
        let handles = connection.leases.drain();
        release(&connection.client, &self.config.queue_url, &handles).await;
        Ok(())
    }
}

/// One SQS message. Dropping it without acknowledging it ends its visibility
/// extension and makes the message visible again for another receive.
pub struct SqsMessage<C, T> {
    message: Message,
    queue_url: String,
    fifo: bool,
    client: Client,
    leases: Arc<Leases>,
    released: mpsc::UnboundedSender<Arc<str>>,
    lease: u64,
    revoked: CancellationToken,
    ack_retry: crate::retry::RetryPolicy,
    codec: Arc<C>,
    acknowledged: bool,
    marker: PhantomData<T>,
}

impl<C, T> SqsMessage<C, T> {
    fn new(source: &SqsSource<C, T>, received: Received) -> Self {
        let connection = source.connection.as_ref().expect("connected source");
        Self {
            message: received.message,
            queue_url: source.config.queue_url.clone(),
            fifo: source.config.endpoint().is_fifo(),
            client: connection.client.clone(),
            leases: connection.leases.clone(),
            released: connection.released.clone(),
            lease: received.lease,
            revoked: received.revoked,
            ack_retry: source.config.ack_retry.clone(),
            codec: source.codec.clone(),
            acknowledged: false,
            marker: PhantomData,
        }
    }

    fn system_attribute(&self, name: MessageSystemAttributeName) -> Option<&str> {
        self.message.attributes()?.get(&name).map(String::as_str)
    }

    fn record<V, E>(&self, value: impl FnOnce(&[u8]) -> Result<V, E>) -> Result<SqsRecord<V>, E> {
        let number = |name| {
            self.system_attribute(name)
                .and_then(|value| value.parse::<u64>().ok())
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

impl<C: Decoder<T>, T: Clone + Send + Sync + 'static> SourceMessage for SqsMessage<C, T> {
    type Item = SqsRecord<T>;
    type Raw = SqsRecord<Vec<u8>>;

    fn decode(&self) -> anyhow::Result<Self::Item> {
        self.record(|bytes| self.codec.decode(bytes))
    }

    fn raw(&self) -> Self::Raw {
        let Ok(record) = self.record(|bytes| Ok::<_, Infallible>(bytes.to_vec()));
        record
    }

    /// Deletes the message, retrying under `ack_retry`. A message whose lease
    /// ended, because its visibility could not be extended in time, is revoked
    /// instead: SQS may already have handed it to another consumer.
    async fn ack(mut self) -> anyhow::Result<()> {
        self.acknowledged = true;
        let Some(receipt_handle) = self.leases.remove(self.lease) else {
            self.revoked.cancel();
            anyhow::bail!("SQS visibility of the message expired before the acknowledgement");
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
                return Err(anyhow::anyhow!("{}", DisplayErrorContext(&error)))
                    .context("deleting an SQS message");
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

impl<C, T> Drop for SqsMessage<C, T> {
    fn drop(&mut self) {
        if self.acknowledged {
            return;
        }
        if let Some(receipt_handle) = self.leases.remove(self.lease) {
            let _ = self.released.send(receipt_handle);
        }
    }
}
