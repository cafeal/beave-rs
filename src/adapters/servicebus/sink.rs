use super::{
    client::ServiceBus,
    config::SinkConfig,
    connection::{Connection, unique_name},
    convert::message_to_amqp,
    record::ServiceBusPublish,
};
use crate::error::{BoxError, Context as _, Error, ensure};
use crate::{
    adapters::pending::PendingLimit,
    codec::Encoder,
    sink::{Completion, PublishRejected, Sink},
};
use fe2o3_amqp::{
    Sendable, Sender,
    link::SendError,
    types::messaging::{Data, Outcome},
};
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::Mutex;

/// An encoded Service Bus message with its properties and annotations.
/// Preparing a value performs all codec work and validation once.
pub struct ServiceBusPrepared {
    sendable: Sendable<Data>,
}

impl ServiceBusPrepared {
    /// The encoded message body.
    pub fn body(&self) -> &[u8] {
        &self.sendable.message.body.0
    }
}

impl fmt::Debug for ServiceBusPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = &self.sendable.message;
        f.debug_struct("ServiceBusPrepared")
            .field("body_len", &message.body.0.len())
            .field("properties", &message.properties)
            .field(
                "application_properties",
                &message
                    .application_properties
                    .as_ref()
                    .map(|properties| properties.0.keys().collect::<Vec<_>>()),
            )
            .finish()
    }
}

/// The send link on the handle's connection.
struct Publisher {
    _connection: Arc<Connection>,
    sender: Sender,
}

impl Publisher {
    async fn close(self) {
        let _ = self.sender.close().await;
    }
}

struct State {
    publisher: Mutex<Option<Publisher>>,
    closed: AtomicBool,
    pending: PendingLimit,
}

/// Sends every output to one queue or topic and completes it when Service Bus
/// accepts the message.
///
/// Built with [`ServiceBus::sink`]. It attaches its send link to the handle's
/// connection on the first publication, which also validates the settings.
/// Clones share the link and closure state.
pub struct ServiceBusSink<C> {
    bus: ServiceBus,
    config: SinkConfig,
    codec: Arc<C>,
    state: Arc<State>,
}

impl<C> Clone for ServiceBusSink<C> {
    fn clone(&self) -> Self {
        Self {
            bus: self.bus.clone(),
            config: self.config.clone(),
            codec: self.codec.clone(),
            state: self.state.clone(),
        }
    }
}

impl<C> ServiceBusSink<C> {
    pub(super) fn new(bus: ServiceBus, config: SinkConfig, codec: C) -> Self {
        Self {
            bus,
            codec: Arc::new(codec),
            state: Arc::new(State {
                publisher: Mutex::new(None),
                closed: AtomicBool::new(false),
                pending: PendingLimit::new(config.max_pending, "Service Bus"),
            }),
            config,
        }
    }

    /// Messages sent and not yet accepted by Service Bus. `submit` waits while
    /// this many are outstanding. Defaults to 1000.
    pub fn max_pending(mut self, max_pending: usize) -> Self {
        self.config.max_pending = max_pending;
        self.state = Arc::new(State {
            publisher: Mutex::new(None),
            closed: AtomicBool::new(false),
            pending: PendingLimit::new(max_pending, "Service Bus"),
        });
        self
    }

    /// Checks the handle and sink settings without contacting Service Bus.
    ///
    /// Fails when the handle is invalid, the entity path is empty or has
    /// surrounding slashes or whitespace, or `max_pending` is zero.
    pub fn validate(&self) -> Result<(), Error> {
        self.bus.validate()?;
        self.config.validate()
    }

    async fn connect(&self) -> Result<Publisher, BoxError> {
        self.validate()?;
        let entity = &self.config.entity_path;
        let connection = self.bus.connection(std::slice::from_ref(entity)).await?;
        let name = unique_name("sender");
        let sender = Sender::builder()
            .name(name.as_str())
            .source(name.as_str())
            .target(entity.as_str())
            .attach(&mut *connection.session().await)
            .await
            .with_context(|| format!("attaching a Service Bus sender to {entity}"))?;
        Ok(Publisher {
            _connection: connection,
            sender,
        })
    }
}

impl<C> Sink<ServiceBusPublish<C::Value>> for ServiceBusSink<C>
where
    C: Encoder<Value: Send + Sync + 'static>,
{
    type Prepared = ServiceBusPrepared;

    fn prepare(&self, output: ServiceBusPublish<C::Value>) -> Result<Self::Prepared, BoxError> {
        let message = message_to_amqp(
            self.codec.encode(&output.value)?,
            &output.application_properties,
            &output.properties,
            output.scheduled_enqueue_time,
        )?;
        Ok(ServiceBusPrepared {
            sendable: Sendable::from(message),
        })
    }

    async fn publish(&self, output: &Self::Prepared) -> Result<(), BoxError> {
        self.submit(output).await?.wait().await
    }

    /// Returns once the message is transferred on the send link. The
    /// completion resolves when Service Bus accepts it. A message larger than
    /// the link's maximum message size fails as
    /// [`PublishRejected`](crate::PublishRejected).
    async fn submit(&self, output: &Self::Prepared) -> Result<Completion, BoxError> {
        let permit = self.state.pending.acquire().await?;
        let mut publisher = self.state.publisher.lock().await;
        ensure!(
            !self.state.closed.load(Ordering::Acquire),
            Error::closed,
            "Service Bus sink is closed"
        );
        if publisher.is_none() {
            *publisher = Some(self.connect().await?);
        }
        let sent = publisher
            .as_mut()
            .expect("connected sink")
            .sender
            .send_batchable_ref(&output.sendable)
            .await;
        let delivery = match sent {
            Ok(delivery) => delivery,
            Err(SendError::MessageSizeExceeded(error)) => {
                return Err(PublishRejected::wrap(Error::wrap(
                    "sending to Service Bus",
                    error,
                )));
            }
            Err(error) => {
                if let Some(lost) = publisher.take() {
                    tokio::spawn(lost.close());
                }
                return Err(Error::wrap("sending to Service Bus", error).into());
            }
        };
        drop(publisher);
        Ok(Completion::pending(async move {
            let _permit = permit;
            match delivery
                .await
                .context("waiting for Service Bus to accept a message")?
            {
                Outcome::Accepted(_) => Ok(()),
                Outcome::Rejected(rejected) => Err(Error::msg(format!(
                    "Service Bus rejected a message: {}",
                    rejected.error.map_or_else(
                        || "no error given".to_owned(),
                        |error| format!(
                            "{:?} {}",
                            error.condition,
                            error.description.unwrap_or_default()
                        )
                    )
                ))
                .into()),
                outcome => Err(Error::msg(format!(
                    "Service Bus did not accept a message: {outcome:?}"
                ))
                .into()),
            }
        }))
    }

    /// Stops new submissions and detaches the send link. The handle's
    /// connection stays open.
    async fn close(&self) -> Result<(), BoxError> {
        self.state.closed.store(true, Ordering::Release);
        self.state.pending.close();
        let publisher = self.state.publisher.lock().await.take();
        if let Some(publisher) = publisher {
            publisher.close().await;
        }
        Ok(())
    }
}
