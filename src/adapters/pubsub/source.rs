use super::{
    client::PubSub,
    config::SourceConfig,
    record::{PubSubMetadata, PubSubRecord},
};
use crate::{
    codec::Decoder,
    error::{BoxError, Error},
    message::{OrderingKey, SourceMessage},
    shutdown::CancellationToken,
    source::{Receive, ReceiveError, Source},
};
use google_cloud_pubsub::{
    client::Subscriber,
    error::AckError,
    model::Message,
    subscriber::{MessageStream, ShutdownToken, handler::Handler},
};
use std::{convert::Infallible, marker::PhantomData, sync::Arc, time::Duration};

/// Consumes one subscription with a streaming pull.
///
/// Built with [`PubSub::source`]. It opens its stream on the handle's
/// subscriber client on the first `receive`, which also validates the
/// settings.
pub struct PubSubSource<C, T> {
    pubsub: PubSub,
    config: SourceConfig,
    codec: Arc<C>,
    connection: Option<Connection>,
    closed: bool,
    marker: PhantomData<T>,
}

struct Connection {
    stream: Option<MessageStream>,
    /// Streams replaced after an error. Their lease management keeps running
    /// for the deliveries they handed out; `close` waits for it.
    retired: Vec<ShutdownToken>,
}

impl<C, T> PubSubSource<C, T> {
    pub(super) fn new(pubsub: PubSub, config: SourceConfig, codec: C) -> Self {
        Self {
            pubsub,
            config,
            codec: Arc::new(codec),
            connection: None,
            closed: false,
            marker: PhantomData,
        }
    }

    /// Messages Pub/Sub hands to the source before any of them is
    /// acknowledged. Zero or less sets no limit. Defaults to 1000.
    pub fn max_outstanding_messages(mut self, max: i64) -> Self {
        self.config.max_outstanding_messages = max;
        self
    }

    /// Bytes of message data Pub/Sub hands to the source before any of it is
    /// acknowledged. Zero or less sets no limit. Defaults to 100 MiB.
    pub fn max_outstanding_bytes(mut self, max: i64) -> Self {
        self.config.max_outstanding_bytes = max;
        self
    }

    /// How far each lease extension moves a message's acknowledgement
    /// deadline, from 10 seconds to 10 minutes. Pub/Sub redelivers a message
    /// this long after the source stops extending it, for example because the
    /// process crashed. Defaults to 60 seconds.
    pub fn ack_deadline(mut self, ack_deadline: Duration) -> Self {
        self.config.ack_deadline = ack_deadline;
        self
    }

    /// How long the source extends a message's lease at most. A delivery held
    /// longer is revoked, because Pub/Sub redelivers it. Defaults to one hour.
    pub fn max_lease(mut self, max_lease: Duration) -> Self {
        self.config.max_lease = max_lease;
        self
    }

    /// Checks the handle and source settings without contacting Pub/Sub.
    ///
    /// Fails when the handle is invalid, the subscription is not named
    /// `projects/{project}/subscriptions/{id}`, `ack_deadline` is outside 10
    /// seconds to 10 minutes, or `max_lease` is shorter than `ack_deadline`.
    pub fn validate(&self) -> Result<(), Error> {
        self.pubsub.validate()?;
        self.config.validate()
    }

    fn subscribe(&self, client: &Subscriber) -> MessageStream {
        client
            .subscribe(&self.config.subscription)
            .set_max_outstanding_messages(self.config.max_outstanding_messages)
            .set_max_outstanding_bytes(self.config.max_outstanding_bytes)
            .set_max_lease_extension(self.config.ack_deadline)
            .set_max_lease(self.config.max_lease)
            .build()
    }
}

impl<C: Decoder<T>, T: Clone + Send + Sync + 'static> Source for PubSubSource<C, T> {
    type Message = PubSubMessage<C, T>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        if self.closed {
            return Ok(Receive::End);
        }
        if self.connection.is_none() {
            self.validate()
                .map_err(|error| ReceiveError::Fatal(error.into()))?;
            self.connection = Some(Connection {
                stream: None,
                retired: Vec::new(),
            });
        }
        let connection = self.connection.as_ref().expect("connected source");
        let stream = match connection.stream {
            Some(_) => None,
            None => {
                let client = self
                    .pubsub
                    .subscriber()
                    .await
                    .map_err(ReceiveError::Fatal)?;
                Some(self.subscribe(client))
            }
        };
        let connection = self.connection.as_mut().expect("connected source");
        if let Some(stream) = stream {
            connection.stream = Some(stream);
        }
        let stream = connection.stream.as_mut().expect("open stream");
        let error = match stream.next().await {
            Some(Ok((message, handler))) => {
                return Ok(Receive::Message(PubSubMessage::new(self, message, handler)));
            }
            Some(Err(error)) => Error::wrap("receiving from Pub/Sub", error),
            None => Error::msg("the Pub/Sub stream ended"),
        };
        // The client retries transient errors itself, so this stream is done.
        // The next receive opens another one.
        let stream = connection.stream.take().expect("open stream");
        connection.retired.push(stream.shutdown_token());
        Err(ReceiveError::Retry(error.into()))
    }

    /// Stops the stream, sends the acknowledgements still pending, and makes
    /// the messages that were received but not handed out available again.
    async fn close(&mut self) -> Result<(), BoxError> {
        self.closed = true;
        let Some(connection) = self.connection.take() else {
            return Ok(());
        };
        let mut tokens = connection.retired;
        tokens.extend(connection.stream.map(|stream| stream.shutdown_token()));
        for token in tokens {
            token.shutdown().await;
        }
        Ok(())
    }
}

/// One Pub/Sub message. The source extends its lease until it is
/// acknowledged or dropped. Dropping it without acknowledging it nacks it,
/// so Pub/Sub redelivers it at once.
pub struct PubSubMessage<C, T> {
    message: Message,
    handler: Option<Handler>,
    subscription: String,
    revoked: CancellationToken,
    /// Cancelled when the message is dropped, ending its lease watch.
    done: CancellationToken,
    codec: Arc<C>,
    marker: PhantomData<T>,
}

impl<C, T> PubSubMessage<C, T> {
    fn new(source: &PubSubSource<C, T>, message: Message, handler: Handler) -> Self {
        let revoked = CancellationToken::new();
        let done = CancellationToken::new();
        tokio::spawn(watch_lease(
            source.config.max_lease,
            revoked.clone(),
            done.clone(),
        ));
        Self {
            message,
            handler: Some(handler),
            subscription: source.config.subscription.clone(),
            revoked,
            done,
            codec: source.codec.clone(),
            marker: PhantomData,
        }
    }

    fn record<V, E>(
        &self,
        value: impl FnOnce(&[u8]) -> Result<V, E>,
    ) -> Result<PubSubRecord<V>, E> {
        let message = &self.message;
        let publish_time = message.publish_time.as_ref().map(|time| {
            time.seconds()
                .saturating_mul(1000)
                .saturating_add(i64::from(time.nanos() / 1_000_000))
        });
        let delivery_attempt = self
            .handler
            .as_ref()
            .and_then(Handler::delivery_attempt)
            .and_then(|attempt| u32::try_from(attempt).ok());
        Ok(PubSubRecord {
            value: value(&message.data)?,
            attributes: message
                .attributes
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            ordering_key: (!message.ordering_key.is_empty()).then(|| message.ordering_key.clone()),
            metadata: PubSubMetadata {
                subscription: self.subscription.clone(),
                message_id: message.message_id.clone(),
                publish_time,
                delivery_attempt,
            },
        })
    }
}

/// Revokes a delivery once its lease reached `max_lease`, after which the
/// client stops extending it and Pub/Sub redelivers the message.
async fn watch_lease(max_lease: Duration, revoked: CancellationToken, done: CancellationToken) {
    tokio::select! {
        _ = tokio::time::sleep(max_lease) => revoked.cancel(),
        _ = done.cancelled() => {}
    }
}

impl<C: Decoder<T>, T: Clone + Send + Sync + 'static> SourceMessage for PubSubMessage<C, T> {
    type Item = PubSubRecord<T>;
    type Raw = PubSubRecord<Vec<u8>>;

    fn decode(&self) -> Result<Self::Item, BoxError> {
        self.record(|bytes| self.codec.decode(bytes))
    }

    fn raw(&self) -> Self::Raw {
        let Ok(record) = self.record(|bytes| Ok::<_, Infallible>(bytes.to_vec()));
        record
    }

    /// Acknowledges the message. On a subscription with exactly-once delivery,
    /// this waits for Pub/Sub to confirm the acknowledgement and revokes the
    /// delivery when its lease expired first. Otherwise the client sends
    /// acknowledgements in batches without confirmation, so the message can
    /// still be redelivered.
    async fn ack(mut self) -> Result<(), BoxError> {
        let handler = self.handler.take().expect("unacknowledged message");
        match handler {
            Handler::ExactlyOnce(handler) => match handler.confirmed_ack().await {
                Ok(()) => Ok(()),
                Err(error) => {
                    if matches!(error, AckError::LeaseExpired) {
                        self.revoked.cancel();
                    }
                    Err(Error::wrap("acknowledging a Pub/Sub message", error).into())
                }
            },
            handler => {
                handler.ack();
                Ok(())
            }
        }
    }

    /// The ordering key of a message published with one. Pub/Sub delivers the
    /// messages of a key in order only on a subscription with message ordering
    /// enabled.
    fn ordering_key(&self) -> Option<OrderingKey> {
        let key = &self.message.ordering_key;
        (!key.is_empty())
            .then(|| OrderingKey::new(self.subscription.as_str(), 0).with_key(key.as_bytes()))
    }

    fn revocation(&self) -> Option<CancellationToken> {
        Some(self.revoked.clone())
    }

    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        self.message
            .attributes
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect()
    }
}

impl<C, T> Drop for PubSubMessage<C, T> {
    fn drop(&mut self) {
        self.done.cancel();
    }
}
