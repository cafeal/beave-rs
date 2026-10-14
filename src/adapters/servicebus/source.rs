use super::{
    client::ServiceBus,
    config::SourceConfig,
    connection::{Connection, unique_name},
    convert::{
        DEAD_LETTER_SOURCE, ENQUEUED_TIME, LOCK_TOKEN, LOCKED_UNTIL, SCHEDULED_ENQUEUE_TIME,
        SEQUENCE_NUMBER, annotation, application_properties_from_amqp, body_bytes, find,
        properties_from_amqp, timestamp, uuid_text,
    },
    lock::{Locks, keep},
    management::Management,
    record::{ServiceBusMetadata, ServiceBusRecord},
};
use crate::error::{BoxError, Context as _, Error};
use crate::{
    codec::Decoder,
    message::SourceMessage,
    shutdown::CancellationToken,
    source::{Receive, ReceiveError, Source},
};
use fe2o3_amqp::{
    Delivery, Receiver,
    link::{
        RecvError,
        delivery::DeliveryInfo,
        receiver::{CreditMode, ReceiverDisposer},
    },
    types::{
        definitions::{ReceiverSettleMode, SenderSettleMode},
        messaging::{Body, Message, Modified},
        primitives::{SimpleValue, Uuid, Value},
    },
};
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, timeout},
};
use tracing::{debug, warn};

/// Receives from one queue or subscription in peek-lock mode.
///
/// Built with [`ServiceBus::source`]. It attaches its links to the handle's
/// connection on the first `receive`, which also validates the settings, and
/// again after the connection or a link is lost.
pub struct ServiceBusSource<C> {
    bus: ServiceBus,
    config: SourceConfig,
    codec: Arc<C>,
    link: Option<Link>,
    closed: bool,
}

type Received = Result<Delivery<Body<Value>>, RecvError>;

/// The receive link, which a task owns, and the task that renews the locks of
/// its messages, on the handle's connection.
struct Link {
    _connection: Arc<Connection>,
    requests: mpsc::Sender<oneshot::Sender<Received>>,
    abandoned: mpsc::UnboundedSender<DeliveryInfo>,
    disposer: ReceiverDisposer,
    locks: Arc<Locks>,
    tasks: Tasks,
}

/// The receiving and lock-renewal tasks. Dropping them stops both.
struct Tasks {
    stop: CancellationToken,
    receiving: JoinHandle<()>,
    renewing: JoinHandle<()>,
}

impl Tasks {
    /// Stops both tasks once the receiving task has returned the messages it
    /// still holds.
    async fn stop(mut self) {
        self.stop.cancel();
        let _ = (&mut self.receiving).await;
        let _ = (&mut self.renewing).await;
    }
}

impl Drop for Tasks {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

impl Link {
    /// Revokes the link's deliveries, whose settlements can no longer reach
    /// Service Bus, and detaches the links in the background.
    fn end(self) {
        self.locks.revoke_all();
        tokio::spawn(self.close());
    }

    async fn close(self) {
        self.tasks.stop().await;
        self.locks.revoke_all();
    }

    /// The next delivery from the receiving task.
    async fn next(&self) -> Option<Received> {
        let (reply, received) = oneshot::channel();
        self.requests.send(reply).await.ok()?;
        received.await.ok()
    }
}

/// Longest time closing waits for messages Service Bus already sent ahead.
const CLOSE_DRAIN: Duration = Duration::from_secs(5);
/// How long closing waits for one more message sent ahead.
const CLOSE_DRAIN_IDLE: Duration = Duration::from_millis(250);

/// Owns the receive link: answers each request with the next delivery and
/// abandons the messages sent to `abandoned`, until `stop` is cancelled or the
/// link fails.
///
/// A delivery whose request was dropped, because the `receive` waiting for it
/// was cancelled, is kept for the next request. When stopping, the task
/// returns every message it still holds, including those Service Bus sent
/// ahead, so they are delivered again at once instead of when their locks end.
async fn receive(
    mut receiver: Receiver,
    mut requests: mpsc::Receiver<oneshot::Sender<Received>>,
    mut abandoned: mpsc::UnboundedReceiver<DeliveryInfo>,
    stop: CancellationToken,
) {
    let mut kept: Option<Received> = None;
    'serve: loop {
        let reply = tokio::select! {
            _ = stop.cancelled() => break,
            Some(info) = abandoned.recv() => {
                abandon(&receiver, info).await;
                continue;
            }
            reply = requests.recv() => match reply {
                Some(reply) => reply,
                None => break,
            },
        };
        let received = match kept.take() {
            Some(received) => received,
            None => loop {
                tokio::select! {
                    _ = stop.cancelled() => break 'serve,
                    Some(info) = abandoned.recv() => abandon(&receiver, info).await,
                    // `recv` is cancellation-safe: a delivery leaves the
                    // link's buffer only when it completes.
                    received = receiver.recv::<Body<Value>>() => break received,
                }
            },
        };
        let failed = matches!(
            received,
            Err(ref error) if !matches!(error, RecvError::MessageSizeExceeded(_))
        );
        if let Err(received) = reply.send(received) {
            kept = Some(received);
        } else if failed {
            return;
        }
    }
    // Stop refilling the credit, then hand back what Service Bus already sent
    // ahead. Those messages never reached the application, so releasing them
    // does not count a delivery; a released message that comes back to this
    // link uses up its remaining credit.
    receiver.set_credit_mode(CreditMode::Manual);
    if let Some(Ok(delivery)) = kept {
        release(&receiver, delivery.into_parts().0).await;
    }
    let drained = async {
        loop {
            while let Ok(info) = abandoned.try_recv() {
                abandon(&receiver, info).await;
            }
            match timeout(CLOSE_DRAIN_IDLE, receiver.recv::<Body<Value>>()).await {
                Ok(Ok(delivery)) => release(&receiver, delivery.into_parts().0).await,
                Ok(Err(RecvError::MessageSizeExceeded(_))) => {}
                Ok(Err(_)) | Err(_) => return,
            }
        }
    };
    let _ = timeout(CLOSE_DRAIN, drained).await;
    while let Ok(info) = abandoned.try_recv() {
        abandon(&receiver, info).await;
    }
    let _ = receiver.close().await;
}

/// Returns a message the application never received with a `released`
/// disposition, which does not count a delivery.
async fn release(receiver: &Receiver, info: DeliveryInfo) {
    if let Err(error) = receiver.release(info).await {
        warn!(%error, "releasing a Service Bus message failed");
    }
}

/// Abandons a message with a `modified` disposition that counts a failed
/// delivery, so Service Bus delivers it again with an incremented delivery
/// count.
async fn abandon(receiver: &Receiver, info: DeliveryInfo) {
    let modified = Modified {
        delivery_failed: Some(true),
        undeliverable_here: None,
        message_annotations: None,
    };
    if let Err(error) = receiver.modify(info, modified).await {
        warn!(%error, "abandoning a Service Bus message failed");
    }
}

impl<C> ServiceBusSource<C> {
    pub(super) fn new(bus: ServiceBus, config: SourceConfig, codec: C) -> Self {
        Self {
            bus,
            config,
            codec: Arc::new(codec),
            link: None,
            closed: false,
        }
    }

    /// Messages Service Bus may send ahead of `receive` (the link credit).
    /// Each is locked from the moment Service Bus sends it, so keep it small
    /// enough that prefetched messages are received well within the lock
    /// duration. Defaults to 10.
    pub fn prefetch(mut self, prefetch: u32) -> Self {
        self.config.prefetch = prefetch;
        self
    }

    /// Checks the handle and source settings without contacting Service Bus.
    ///
    /// Fails when the handle is invalid, the entity path is empty or has
    /// surrounding slashes or whitespace, or `prefetch` is zero.
    pub fn validate(&self) -> Result<(), Error> {
        self.bus.validate()?;
        self.config.validate()
    }

    async fn connect(&self) -> Result<Link, Error> {
        let entity = &self.config.entity_path;
        let connection = self
            .bus
            .connection(&[entity.clone(), format!("{entity}/$management")])
            .await?;
        let (receiver, management) = {
            let mut session = connection.session().await;
            let name = unique_name("receiver");
            let receiver = Receiver::builder()
                .name(name.as_str())
                .source(entity.as_str())
                .target(name.as_str())
                .sender_settle_mode(SenderSettleMode::Unsettled)
                .receiver_settle_mode(ReceiverSettleMode::First)
                .credit_mode(CreditMode::Auto(self.config.prefetch))
                .auto_accept(false)
                .attach(&mut *session)
                .await
                .with_context(|| format!("attaching a Service Bus receiver to {entity}"))?;
            match Management::attach(&mut session, entity, name).await {
                Ok(management) => (receiver, management),
                Err(error) => {
                    let _ = receiver.close().await;
                    return Err(error);
                }
            }
        };
        let disposer = receiver.disposer();
        let locks = Arc::new(Locks::default());
        let (abandoned, abandons) = mpsc::unbounded_channel();
        let (requests, served) = mpsc::channel(1);
        let stop = CancellationToken::new();
        let tasks = Tasks {
            receiving: tokio::spawn(receive(receiver, served, abandons, stop.clone())),
            renewing: tokio::spawn(keep(management, locks.clone(), stop.clone())),
            stop,
        };
        Ok(Link {
            _connection: connection,
            requests,
            abandoned,
            disposer,
            locks,
            tasks,
        })
    }
}

/// The lock token of a delivery: its `x-opt-lock-token` annotation, or else
/// its delivery tag, which Service Bus fills with the token's bytes in .NET
/// `Guid` order.
fn lock_token(delivery: &Delivery<Body<Value>>) -> Option<Uuid> {
    let annotated = delivery
        .message()
        .delivery_annotations
        .as_ref()
        .and_then(|annotations| find(&annotations.0, LOCK_TOKEN));
    if let Some(Value::Uuid(token)) = annotated {
        return Some(token.clone());
    }
    let tag: [u8; 16] = delivery.delivery_tag().as_ref().try_into().ok()?;
    let mut bytes = tag;
    bytes[..4].reverse();
    bytes[4..6].reverse();
    bytes[6..8].reverse();
    Some(Uuid::from(bytes))
}

impl<C: Decoder<Value: Clone + Send + Sync + 'static>> Source for ServiceBusSource<C> {
    type Message = ServiceBusMessage<C>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        if self.closed {
            return Ok(Receive::End);
        }
        if self.link.is_none() {
            self.validate()
                .map_err(|error| ReceiveError::Fatal(error.into()))?;
            self.link = Some(
                self.connect()
                    .await
                    .map_err(|error| ReceiveError::Retry(error.into()))?,
            );
        }
        loop {
            let link = self.link.as_mut().expect("connected source");
            // A delivery whose request is dropped stays with the receiving task.
            let error = match link.next().await {
                Some(Ok(delivery)) => {
                    let token = lock_token(&delivery).ok_or_else(|| {
                        ReceiveError::Fatal(
                            Error::msg("Service Bus delivered a message without a lock token")
                                .into(),
                        )
                    })?;
                    let (info, message) = delivery.into_parts();
                    let locked_until = timestamp(annotation(&message, LOCKED_UNTIL));
                    let now = (Instant::now(), SystemTime::now());
                    let Some((lock, revoked)) = link.locks.add(token.clone(), locked_until, now)
                    else {
                        // Waiting in the link's buffer outlasted the lock, so
                        // Service Bus delivers the message again.
                        debug!("skipping a Service Bus message whose lock ended before receive");
                        continue;
                    };
                    return Ok(Receive::Message(ServiceBusMessage {
                        message,
                        info: Some(info),
                        entity_path: self.config.entity_path.clone(),
                        lock_token: uuid_text(token.as_inner()),
                        lock,
                        locks: link.locks.clone(),
                        revoked,
                        disposer: link.disposer.clone(),
                        abandoned: link.abandoned.clone(),
                        codec: self.codec.clone(),
                    }));
                }
                Some(Err(RecvError::MessageSizeExceeded(error))) => {
                    debug!(%error, "Service Bus message exceeds the link's maximum size");
                    continue;
                }
                Some(Err(error)) => Error::wrap("receiving from Service Bus", error),
                None => Error::msg("Service Bus receive link stopped"),
            };
            self.link.take().expect("connected source").end();
            return Err(ReceiveError::Retry(error.into()));
        }
    }

    /// Abandons the messages that were dropped without an acknowledgement and
    /// detaches the links. The handle's connection stays open.
    async fn close(&mut self) -> Result<(), BoxError> {
        self.closed = true;
        if let Some(link) = self.link.take() {
            link.close().await;
        }
        Ok(())
    }
}

/// One Service Bus message, locked to this receiver. Dropping it without
/// acknowledging it ends its lock renewal and abandons it, so Service Bus
/// delivers it again with an incremented delivery count.
pub struct ServiceBusMessage<C> {
    message: Message<Body<Value>>,
    info: Option<DeliveryInfo>,
    entity_path: String,
    lock_token: String,
    lock: u64,
    locks: Arc<Locks>,
    revoked: CancellationToken,
    disposer: ReceiverDisposer,
    abandoned: mpsc::UnboundedSender<DeliveryInfo>,
    codec: Arc<C>,
}

impl<C> ServiceBusMessage<C> {
    fn record<V>(&self, value: V) -> ServiceBusRecord<V> {
        let message = &self.message;
        let text = |name| match annotation(message, name) {
            Some(Value::String(text)) => Some(text.clone()),
            _ => None,
        };
        ServiceBusRecord {
            value,
            application_properties: application_properties_from_amqp(
                message.application_properties.as_ref(),
            ),
            properties: properties_from_amqp(message),
            metadata: ServiceBusMetadata {
                entity_path: self.entity_path.clone(),
                sequence_number: match annotation(message, SEQUENCE_NUMBER) {
                    Some(Value::Long(number)) => *number,
                    _ => 0,
                },
                enqueued_time: timestamp(annotation(message, ENQUEUED_TIME)),
                scheduled_enqueue_time: timestamp(annotation(message, SCHEDULED_ENQUEUE_TIME)),
                delivery_count: message
                    .header
                    .as_ref()
                    .map_or(0, |header| header.delivery_count)
                    .saturating_add(1),
                lock_token: self.lock_token.clone(),
                dead_letter_source: text(DEAD_LETTER_SOURCE),
            },
        }
    }
}

impl<C: Decoder<Value: Clone + Send + Sync + 'static>> SourceMessage for ServiceBusMessage<C> {
    type Item = ServiceBusRecord<C::Value>;
    type Raw = ServiceBusRecord<Vec<u8>>;

    /// Decodes the binary data of the body. A body that is an AMQP value or
    /// sequence fails to decode.
    fn decode(&self) -> Result<Self::Item, BoxError> {
        match body_bytes(&self.message.body) {
            Ok(bytes) => Ok(self.record(self.codec.decode(&bytes)?)),
            Err((kind, _)) => Err(Error::invalid_record(format!(
                "Service Bus message body is {kind}, not binary data"
            ))
            .into()),
        }
    }

    /// The body's binary data, or the AMQP encoding of a body that is an AMQP
    /// value or sequence.
    fn raw(&self) -> Self::Raw {
        self.record(body_bytes(&self.message.body).unwrap_or_else(|(_, encoded)| encoded))
    }

    /// Completes the message with an `accepted` disposition. Service Bus does
    /// not confirm it, so a message whose lock is lost before the disposition
    /// arrives is delivered again. A message whose lock already ended, or
    /// whose link was lost, is revoked instead.
    async fn ack(mut self) -> Result<(), BoxError> {
        let info = self.info.take().expect("unacknowledged message");
        if !self.locks.remove(self.lock) {
            self.revoked.cancel();
            return Err(
                Error::msg("Service Bus message lock ended before the acknowledgement").into(),
            );
        }
        if let Err(error) = self.disposer.accept(info).await {
            self.revoked.cancel();
            return Err(Error::wrap("completing a Service Bus message", error).into());
        }
        Ok(())
    }

    fn revocation(&self) -> Option<CancellationToken> {
        Some(self.revoked.clone())
    }

    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        let Some(properties) = self.message.application_properties.as_ref() else {
            return Vec::new();
        };
        properties
            .0
            .iter()
            .filter_map(|(name, value)| match value {
                SimpleValue::String(value) => Some((name.as_str(), value.as_str())),
                _ => None,
            })
            .collect()
    }
}

impl<C> Drop for ServiceBusMessage<C> {
    fn drop(&mut self) {
        if let Some(info) = self.info.take()
            && self.locks.remove(self.lock)
        {
            let _ = self.abandoned.send(info);
        }
    }
}
