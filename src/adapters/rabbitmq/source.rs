use super::{
    config::RabbitMqSourceConfig,
    connection::{close, connect},
    convert::{headers_from_amqp, is_persistent, properties_from_amqp, short_string},
    record::{RabbitMqMetadata, RabbitMqRecord},
};
use crate::{
    codec::Decoder,
    message::{OrderingKey, SourceMessage},
    shutdown::CancellationToken,
    source::{Receive, ReceiveError, Source},
};
use anyhow::Context as _;
use futures_util::StreamExt;
use lapin::{
    Connection, Consumer,
    message::Delivery,
    options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions},
    types::{AMQPValue, FieldTable},
};
use std::{convert::Infallible, marker::PhantomData, sync::Arc};

/// Consumes one queue. It connects and starts its consumer on the first
/// `receive`, and again after the connection or channel is lost.
pub struct RabbitMqSource<C, T> {
    config: RabbitMqSourceConfig,
    codec: Arc<C>,
    session: Option<Session>,
    closed: bool,
    marker: PhantomData<T>,
}

/// One connection with its consuming channel. Its deliveries are revoked when
/// the session ends, because the broker then requeues them and their delivery
/// tags cannot be acknowledged on another channel.
struct Session {
    connection: Connection,
    consumer: Consumer,
    revoked: CancellationToken,
}

impl Session {
    /// Revokes the session's deliveries and closes its connection in the
    /// background, so the broker requeues what they leave unacknowledged.
    fn end(self) {
        self.revoked.cancel();
        tokio::spawn(async move {
            let _ = close(&self.connection).await;
        });
    }
}

impl<C: Default, T> RabbitMqSource<C, T> {
    /// Creates a source that decodes with the codec's default value.
    ///
    /// The connection is opened on the first receive, which also validates the
    /// configuration.
    pub fn new(config: RabbitMqSourceConfig) -> Self {
        Self::with_codec(config, C::default())
    }
}

impl<C, T> RabbitMqSource<C, T> {
    /// Creates a source that decodes with `codec`.
    ///
    /// The connection is opened on the first receive, which also validates the
    /// configuration.
    pub fn with_codec(config: RabbitMqSourceConfig, codec: C) -> Self {
        Self {
            config,
            codec: Arc::new(codec),
            session: None,
            closed: false,
            marker: PhantomData,
        }
    }

    async fn connect(&self) -> anyhow::Result<Session> {
        let connection = connect(
            &self.config.uri,
            format!("beavers source {}", self.config.queue),
        )
        .await?;
        let consumer = async {
            let channel = connection.create_channel().await?;
            channel
                .basic_qos(self.config.prefetch, BasicQosOptions::default())
                .await?;
            channel
                .basic_consume(
                    short_string("queue name", &self.config.queue)?,
                    "".into(),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
                .with_context(|| format!("consuming RabbitMQ queue {:?}", self.config.queue))
        }
        .await;
        match consumer {
            Ok(consumer) => Ok(Session {
                connection,
                consumer,
                revoked: CancellationToken::new(),
            }),
            Err(error) => {
                let _ = close(&connection).await;
                Err(error)
            }
        }
    }
}

impl<C: Decoder<T>, T: Clone + Send + Sync + 'static> Source for RabbitMqSource<C, T> {
    type Message = RabbitMqMessage<C, T>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        if self.closed {
            return Ok(Receive::End);
        }
        if self.session.is_none() {
            self.config.validate().map_err(ReceiveError::Fatal)?;
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
                    marker: PhantomData,
                }));
            }
            Some(Err(error)) => anyhow::Error::new(error).context("RabbitMQ consumer failed"),
            None => anyhow::anyhow!("RabbitMQ consumer was cancelled"),
        };
        self.session.take().expect("connected source").end();
        Err(ReceiveError::Retry(error))
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.closed = true;
        match self.session.take() {
            Some(session) => {
                session.revoked.cancel();
                close(&session.connection).await
            }
            None => Ok(()),
        }
    }
}

/// One RabbitMQ delivery. Dropping it leaves the message unacknowledged until
/// its channel closes, when the broker requeues it.
pub struct RabbitMqMessage<C, T> {
    delivery: Delivery,
    queue: String,
    codec: Arc<C>,
    revoked: CancellationToken,
    ordering_key: Option<OrderingKey>,
    marker: PhantomData<T>,
}

impl<C, T> RabbitMqMessage<C, T> {
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

impl<C: Decoder<T>, T: Clone + Send + Sync + 'static> SourceMessage for RabbitMqMessage<C, T> {
    type Item = RabbitMqRecord<T>;
    type Raw = RabbitMqRecord<Vec<u8>>;

    fn decode(&self) -> anyhow::Result<Self::Item> {
        self.record(|bytes| self.codec.decode(bytes))
    }

    fn raw(&self) -> Self::Raw {
        let Ok(record) = self.record(|bytes| Ok::<_, Infallible>(bytes.to_vec()));
        record
    }

    /// Sends `basic.ack` for the delivery tag. AMQP does not confirm an
    /// acknowledgement, so a connection lost right after it can still requeue
    /// the message.
    async fn ack(self) -> anyhow::Result<()> {
        match self.delivery.acker.ack(BasicAckOptions::default()).await {
            Ok(true) => Ok(()),
            Ok(false) => {
                self.revoked.cancel();
                anyhow::bail!("RabbitMQ channel closed before the acknowledgement")
            }
            Err(error) => {
                self.revoked.cancel();
                Err(anyhow::Error::new(error).context("acknowledging a RabbitMQ delivery"))
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
}
