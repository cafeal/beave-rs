//! Request-response link to an entity's `$management` node.
use crate::error::{Context as _, Error};
use fe2o3_amqp::{
    Receiver, Sender,
    session::SessionHandle,
    types::{
        messaging::{
            AmqpValue, ApplicationProperties, Body, Message, MessageId, Outcome, Properties,
        },
        primitives::{Array, OrderedMap, SimpleValue, Timestamp, Uuid, Value},
    },
};
use std::{fmt, time::Duration};
use tokio::time::timeout;

const RENEW_LOCK: &str = "com.microsoft:renew-lock";
const OPERATION: &str = "operation";
const ASSOCIATED_LINK_NAME: &str = "associated-link-name";
const STATUS_CODE: &str = "statusCode";
const STATUS_DESCRIPTION: &str = "statusDescription";
const LOCK_TOKENS: &str = "lock-tokens";
const EXPIRATIONS: &str = "expirations";

/// Status code of a lock that expired or was released.
pub(super) const LOCK_LOST: i64 = 410;

/// Longest time a request waits for its response.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// A failed management operation.
#[derive(Debug)]
pub(super) enum ManagementError {
    /// Service Bus answered with a status other than 200.
    Status { code: i64, description: String },
    /// The request was not answered, or the link failed.
    Link(Error),
}

impl fmt::Display for ManagementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status { code, description } => {
                write!(f, "Service Bus answered {code}: {description}")
            }
            Self::Link(error) => write!(f, "{error:#}"),
        }
    }
}

/// A sender and a receiver attached to `<entity>/$management`. Requests are
/// matched to responses by message ID, so a response that arrives after its
/// request timed out is skipped.
pub(super) struct Management {
    sender: Sender,
    receiver: Receiver,
    reply_to: String,
    associated_link: String,
    next_id: u64,
}

impl Management {
    /// Attaches to the entity's management node. `associated_link` is the
    /// name of the receive link whose locks the requests concern.
    pub(super) async fn attach(
        session: &mut SessionHandle<()>,
        entity_path: &str,
        associated_link: String,
    ) -> Result<Self, Error> {
        let node = format!("{entity_path}/$management");
        let reply_to = format!("{associated_link}-management");
        let sender = Sender::builder()
            .name(format!("{reply_to}-sender"))
            .target(node.as_str())
            .attach(session)
            .await
            .context("attaching a Service Bus management sender")?;
        let receiver = Receiver::builder()
            .name(format!("{reply_to}-receiver"))
            .source(node.as_str())
            .target(reply_to.as_str())
            .attach(session)
            .await
            .context("attaching a Service Bus management receiver")?;
        Ok(Self {
            sender,
            receiver,
            reply_to,
            associated_link,
            next_id: 0,
        })
    }

    /// Renews the locks of `tokens`, returning their new expiry times in the
    /// same order.
    pub(super) async fn renew_locks(
        &mut self,
        tokens: &[Uuid],
    ) -> Result<Vec<Timestamp>, ManagementError> {
        let body: OrderedMap<String, Array<Uuid>> =
            [(LOCK_TOKENS.to_owned(), Array(tokens.to_vec()))]
                .into_iter()
                .collect();
        let response = self.request(RENEW_LOCK, body).await?;
        let expirations = map_entry(&response, EXPIRATIONS)
            .and_then(|value| match value {
                Value::Array(values) => values
                    .0
                    .iter()
                    .map(|value| match value {
                        Value::Timestamp(timestamp) => Some(timestamp.clone()),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>(),
                _ => None,
            })
            .filter(|expirations| expirations.len() == tokens.len())
            .ok_or_else(|| {
                ManagementError::Link(Error::msg(
                    "Service Bus renew-lock response has no expiration for each lock",
                ))
            })?;
        Ok(expirations)
    }

    async fn request(
        &mut self,
        operation: &str,
        body: OrderedMap<String, Array<Uuid>>,
    ) -> Result<Value, ManagementError> {
        self.next_id += 1;
        let id = self.next_id;
        let message = Message {
            header: None,
            delivery_annotations: None,
            message_annotations: None,
            properties: Some(Properties {
                message_id: Some(MessageId::Ulong(id)),
                reply_to: Some(self.reply_to.clone()),
                ..Properties::default()
            }),
            application_properties: Some(ApplicationProperties(
                [
                    (
                        OPERATION.to_owned(),
                        SimpleValue::String(operation.to_owned()),
                    ),
                    (
                        ASSOCIATED_LINK_NAME.to_owned(),
                        SimpleValue::String(self.associated_link.clone()),
                    ),
                ]
                .into_iter()
                .collect(),
            )),
            body: AmqpValue(body),
            footer: None,
        };
        let exchange = async {
            match self
                .sender
                .send(message)
                .await
                .context("sending a Service Bus management request")?
            {
                Outcome::Accepted(_) => {}
                outcome => {
                    return Err(Error::msg(format!(
                        "Service Bus refused a management request: {outcome:?}"
                    )));
                }
            }
            loop {
                let delivery = self
                    .receiver
                    .recv::<Body<Value>>()
                    .await
                    .context("receiving a Service Bus management response")?;
                self.receiver
                    .accept(&delivery)
                    .await
                    .context("settling a Service Bus management response")?;
                let message = delivery.into_message();
                let answers = message
                    .properties
                    .as_ref()
                    .and_then(|properties| properties.correlation_id.as_ref())
                    == Some(&MessageId::Ulong(id));
                if answers {
                    return Ok(message);
                }
            }
        };
        let message = timeout(REQUEST_TIMEOUT, exchange)
            .await
            .map_err(|_| {
                ManagementError::Link(Error::msg("Service Bus management request timed out"))
            })?
            .map_err(ManagementError::Link)?;
        let property = |name: &str| {
            message
                .application_properties
                .as_ref()
                .and_then(|properties| properties.0.get(name))
        };
        let code = property(STATUS_CODE).and_then(integer).ok_or_else(|| {
            ManagementError::Link(Error::msg(
                "Service Bus management response has no status code",
            ))
        })?;
        if code != 200 {
            let description = match property(STATUS_DESCRIPTION) {
                Some(SimpleValue::String(description)) => description.clone(),
                _ => String::new(),
            };
            return Err(ManagementError::Status { code, description });
        }
        match message.body {
            Body::Value(AmqpValue(value)) => Ok(value),
            _ => Ok(Value::Null),
        }
    }

    /// Detaches both links.
    pub(super) async fn close(self) {
        let _ = self.sender.close().await;
        let _ = self.receiver.close().await;
    }
}

fn integer(value: &SimpleValue) -> Option<i64> {
    match value {
        SimpleValue::Int(value) => Some((*value).into()),
        SimpleValue::Long(value) => Some(*value),
        SimpleValue::Uint(value) => Some((*value).into()),
        SimpleValue::Short(value) => Some((*value).into()),
        SimpleValue::Ushort(value) => Some((*value).into()),
        _ => None,
    }
}

fn map_entry<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    match value {
        Value::Map(map) => map.iter().find_map(|(key, value)| match key {
            Value::String(key) | Value::Symbol(fe2o3_amqp::types::primitives::Symbol(key))
                if key == name =>
            {
                Some(value)
            }
            _ => None,
        }),
        _ => None,
    }
}
