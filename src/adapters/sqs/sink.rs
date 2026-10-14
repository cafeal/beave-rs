use super::{
    client::Sqs,
    config::{is_fifo, validate_queue_url},
    convert::attributes_to_sqs,
    record::SqsPublish,
};
use crate::{
    codec::Encoder,
    error::{BoxError, Context, Error, ensure},
    sink::{PublishRejected, Sink},
};
use aws_sdk_sqs::{
    error::{DisplayErrorContext, ProvideErrorMetadata},
    types::MessageAttributeValue,
};
use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

/// Longest per-message delay SQS accepts.
const MAX_DELAY: Duration = Duration::from_secs(15 * 60);

/// Error codes of a message SQS refuses whatever the attempt.
const REJECTED: [&str; 5] = [
    "InvalidMessageContents",
    "InvalidParameterValue",
    "InvalidAttributeName",
    "InvalidAttributeValue",
    "MissingParameter",
];

/// An SQS message ready to send: the encoded body, attributes, and FIFO fields.
#[derive(Clone)]
pub struct SqsPrepared {
    body: String,
    attributes: HashMap<String, MessageAttributeValue>,
    message_group_id: Option<String>,
    deduplication_id: Option<String>,
    delay_seconds: Option<i32>,
}

impl SqsPrepared {
    /// The encoded message body.
    pub fn body(&self) -> &str {
        &self.body
    }
}

impl fmt::Debug for SqsPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqsPrepared")
            .field("body_len", &self.body.len())
            .field("attributes", &self.attributes.keys().collect::<Vec<_>>())
            .field("message_group_id", &self.message_group_id)
            .field("deduplication_id", &self.deduplication_id)
            .field("delay_seconds", &self.delay_seconds)
            .finish()
    }
}

/// Sends each output to one queue with `SendMessage`. A publication succeeds
/// once SQS has stored the message.
///
/// Built with [`Sqs::sink`]. The handle's client is created on the first
/// publication, which also validates the settings. Clones share their closure
/// state.
pub struct SqsSink<C> {
    sqs: Sqs,
    queue_url: String,
    codec: Arc<C>,
    closed: Arc<AtomicBool>,
}

impl<C> Clone for SqsSink<C> {
    fn clone(&self) -> Self {
        Self {
            sqs: self.sqs.clone(),
            queue_url: self.queue_url.clone(),
            codec: self.codec.clone(),
            closed: self.closed.clone(),
        }
    }
}

impl<C> SqsSink<C> {
    pub(super) fn new(sqs: Sqs, queue_url: String, codec: C) -> Self {
        Self {
            sqs,
            queue_url,
            codec: Arc::new(codec),
            closed: Arc::default(),
        }
    }

    /// Checks the handle and sink settings without contacting SQS.
    ///
    /// Fails when the handle is invalid or the queue URL is not an http or
    /// https URL.
    pub fn validate(&self) -> Result<(), Error> {
        self.sqs.validate()?;
        validate_queue_url(&self.queue_url)
    }
}

impl<C> Sink<SqsPublish<C::Value>> for SqsSink<C>
where
    C: Encoder<Value: Send + Sync + 'static>,
{
    type Prepared = SqsPrepared;

    /// Encodes the value as the message body, which SQS requires to be text.
    /// A codec whose output is not UTF-8, such as Protobuf, fails here.
    fn prepare(&self, output: SqsPublish<C::Value>) -> Result<Self::Prepared, BoxError> {
        let body = String::from_utf8(self.codec.encode(&output.value)?)
            .context("SQS message bodies must be UTF-8 text")?;
        let fifo = is_fifo(&self.queue_url);
        if fifo {
            ensure!(
                output.message_group_id.is_some(),
                Error::invalid_record,
                "messages sent to an SQS FIFO queue need a message group ID"
            );
            ensure!(
                output.delay.is_none(),
                Error::invalid_record,
                "SQS FIFO queues accept no per-message delay"
            );
        }
        if let Some(delay) = output.delay {
            ensure!(
                delay <= MAX_DELAY,
                Error::invalid_record,
                "SQS message delay must be at most 15 minutes"
            );
        }
        Ok(SqsPrepared {
            body,
            attributes: attributes_to_sqs(&output.attributes)?,
            message_group_id: output.message_group_id,
            deduplication_id: output.deduplication_id,
            delay_seconds: output.delay.map(|delay| delay.as_secs() as i32),
        })
    }

    async fn publish(&self, output: &Self::Prepared) -> Result<(), BoxError> {
        ensure!(
            !self.closed.load(Ordering::Acquire),
            Error::closed,
            "SQS sink is closed"
        );
        self.validate()?;
        let result = self
            .sqs
            .client()
            .await?
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(&output.body)
            .set_message_attributes(
                (!output.attributes.is_empty()).then(|| output.attributes.clone()),
            )
            .set_message_group_id(output.message_group_id.clone())
            .set_message_deduplication_id(output.deduplication_id.clone())
            .set_delay_seconds(output.delay_seconds)
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(error) => {
                let rejected = error.code().is_some_and(|code| REJECTED.contains(&code));
                let error = Error::msg(format!("sending to SQS: {}", DisplayErrorContext(&error)));
                Err(if rejected {
                    PublishRejected::wrap(error)
                } else {
                    error.into()
                })
            }
        }
    }

    async fn close(&self) -> Result<(), BoxError> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }
}
