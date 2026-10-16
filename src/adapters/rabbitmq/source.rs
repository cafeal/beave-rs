use super::{
    config::SourceConfig,
    connection::{RabbitMq, Role, close},
    convert::{headers_from_amqp, is_persistent, properties_from_amqp, short_string},
    record::{RabbitMqMetadata, RabbitMqRecord},
};
use crate::{
    codec::Decoder,
    error::{BoxError, Context, Error},
    message::{MessagingAttributes, OrderingKey, SourceMessage},
    shutdown::CancellationToken,
    source::{Receive, ReceiveError, Source},
};
use futures_util::StreamExt;
use lapin::{
    Channel, Consumer,
    message::Delivery,
    options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions},
    types::{AMQPValue, FieldTable},
};
use std::{convert::Infallible, sync::Arc};

/// Consumes one queue on a channel of its own.
///
/// Built with [`RabbitMq::source`]. It opens its channel and starts its
/// consumer on the first `receive`, which also validates the settings, and
/// again after the channel or connection is lost.
pub struct RabbitMqSource<C> {
    rabbit: RabbitMq,
    config: SourceConfig,
    codec: Arc<C>,
    session: Option<Session>,
    closed: bool,
}

/// One consuming channel. Its deliveries are revoked when the session ends,
/// because the broker then requeues them and their delivery tags cannot be
/// acknowledged on another channel.
struct Session {
    channel: Channel,
    consumer: Consumer,
    revoked: CancellationToken,
}

impl Session {
    /// Revokes the session's deliveries and closes its channel in the
    /// background, so the broker requeues what they leave unacknowledged.
    fn end(self) {
        self.revoked.cancel();
        tokio::spawn(async move {
            let _ = close(&self.channel).await;
        });
    }
}

impl<C> RabbitMqSource<C> {
    pub(super) fn new(rabbit: RabbitMq, config: SourceConfig, codec: C) -> Self {
        Self {
            rabbit,
            config,
            codec: Arc::new(codec),
            session: None,
            closed: false,
        }
    }

    /// Unacknowledged messages the broker sends ahead of acknowledgements
    /// (`basic.qos` prefetch count). It bounds the source's buffered and
    /// in-flight deliveries together. Defaults to 100.
    pub fn prefetch(mut self, prefetch: u16) -> Self {
        self.config.prefetch = prefetch;
        self
    }

    /// Gives every delivery the queue as its ordering key, so the subscription
    /// processes them one at a time in delivery order under the default
    /// [`SourceOrder`](crate::SourceOrder) distribution. Use it with
    /// a queue that has a single active consumer. Disabled by default.
    pub fn ordered(mut self, ordered: bool) -> Self {
        self.config.ordered = ordered;
        self
    }

    /// Checks the handle and source settings without connecting.
    ///
    /// Fails when the handle is invalid, the queue name is empty or longer than
    /// 255 bytes, or `prefetch` is zero.
    pub fn validate(&self) -> Result<(), Error> {
        self.rabbit.validate()?;
        self.config.validate()
    }

    async fn connect(&self) -> Result<Session, BoxError> {
        let channel = self.rabbit.channel(Role::Consume).await?;
        let consumer = async {
            channel
                .basic_qos(self.config.prefetch, BasicQosOptions::default())
                .await?;
            let consumer = channel
                .basic_consume(
                    short_string("queue name", &self.config.queue)?,
                    "".into(),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
                .with_context(|| format!("consuming RabbitMQ queue {:?}", self.config.queue))?;
            Ok::<_, BoxError>(consumer)
        }
        .await;
        match consumer {
            Ok(consumer) => Ok(Session {
                channel,
                consumer,
                revoked: CancellationToken::new(),
            }),
            Err(error) => {
                let _ = close(&channel).await;
                Err(error)
            }
        }
    }
}

impl<C: Decoder<Value: Clone + Send + Sync + 'static>> Source for RabbitMqSource<C> {
    type Message = RabbitMqMessage<C>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        if self.closed {
            return Ok(Receive::End);
        }
        if self.session.is_none() {
            self.validate()
                .map_err(|error| ReceiveError::Fatal(error.into()))?;
            self.session = Some(self.connect().await.map_err(ReceiveError::Retry)?);
        }
        let session = self.session.as_mut().expect("connected source");
        // The consumer is a channel-backed stream: a delivery leaves it only
        // when `next` completes, so dropping a pending receive loses nothing.
        let error = match session.consumer.next().await {
            Some(Ok(delivery)) => {
                let ordering_key = self
                    .config
                    .ordered
                    .then(|| OrderingKey::new(self.config.queue.as_str(), 0));
                return Ok(Receive::Message(RabbitMqMessage {
                    delivery,
                    queue: self.config.queue.clone(),
                    codec: self.codec.clone(),
                    revoked: session.revoked.clone(),
                    ordering_key,
                }));
            }
            Some(Err(error)) => Error::wrap("RabbitMQ consumer failed", error),
            None => Error::msg("RabbitMQ consumer was cancelled"),
        };
        self.session.take().expect("connected source").end();
        Err(ReceiveError::Retry(error.into()))
    }

    async fn close(&mut self) -> Result<(), BoxError> {
        self.closed = true;
        match self.session.take() {
            Some(session) => {
                session.revoked.cancel();
                Ok(close(&session.channel).await?)
            }
            None => Ok(()),
        }
    }
}

/// One RabbitMQ delivery. Dropping it leaves the message unacknowledged until
/// its channel closes, when the broker requeues it.
pub struct RabbitMqMessage<C> {
    delivery: Delivery,
    queue: String,
    codec: Arc<C>,
    revoked: CancellationToken,
    ordering_key: Option<OrderingKey>,
}

impl<C> RabbitMqMessage<C> {
    fn record<V, E>(
        &self,
        value: impl FnOnce(&[u8]) -> Result<V, E>,
    ) -> Result<RabbitMqRecord<V>, E> {
        let delivery = &self.delivery;
        Ok(RabbitMqRecord {
            value: value(&delivery.data)?,
            headers: headers_from_amqp(delivery.properties.headers().as_ref()),
            properties: properties_from_amqp(&delivery.properties),
            metadata: RabbitMqMetadata {
                queue: self.queue.clone(),
                exchange: delivery.exchange.as_str().to_owned(),
                routing_key: delivery.routing_key.as_str().to_owned(),
                redelivered: delivery.redelivered,
                persistent: is_persistent(&delivery.properties),
                delivery_tag: delivery.delivery_tag,
            },
        })
    }
}

impl<C: Decoder<Value: Clone + Send + Sync + 'static>> SourceMessage for RabbitMqMessage<C> {
    type Item = RabbitMqRecord<C::Value>;
    type Raw = RabbitMqRecord<Vec<u8>>;

    fn decode(&self) -> Result<Self::Item, BoxError> {
        self.record(|bytes| self.codec.decode(bytes))
    }

    fn raw(&self) -> Self::Raw {
        let Ok(record) = self.record(|bytes| Ok::<_, Infallible>(bytes.to_vec()));
        record
    }

    /// Sends `basic.ack` for the delivery tag. AMQP does not confirm an
    /// acknowledgement, so a connection lost right after it can still requeue
    /// the message.
    async fn ack(self) -> Result<(), BoxError> {
        match self.delivery.acker.ack(BasicAckOptions::default()).await {
            Ok(true) => Ok(()),
            Ok(false) => {
                self.revoked.cancel();
                Err(Error::msg("RabbitMQ channel closed before the acknowledgement").into())
            }
            Err(error) => {
                self.revoked.cancel();
                Err(Error::wrap("acknowledging a RabbitMQ delivery", error).into())
            }
        }
    }

    fn ordering_key(&self) -> Option<OrderingKey> {
        self.ordering_key.clone()
    }

    fn revocation(&self) -> Option<CancellationToken> {
        Some(self.revoked.clone())
    }

    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        let Some(table) = self.delivery.properties.headers() else {
            return Vec::new();
        };
        table
            .inner()
            .iter()
            .filter_map(|(name, value)| {
                let text = match value {
                    AMQPValue::LongString(value) => std::str::from_utf8(value.as_bytes()).ok()?,
                    AMQPValue::ShortString(value) => value.as_str(),
                    _ => return None,
                };
                Some((name.as_str(), text))
            })
            .collect()
    }

    fn messaging_attributes(&self) -> Option<MessagingAttributes<'_>> {
        let mut attributes = MessagingAttributes::new("rabbitmq").destination(&self.queue);
        if let Some(id) = self.delivery.properties.message_id() {
            attributes = attributes.message_id(id.as_str());
        }
        Some(attributes)
    }
}
