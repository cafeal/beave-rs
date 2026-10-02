use super::{
    client::client, config::SqsSinkConfig, convert::attributes_to_sqs, record::SqsPublish,
};
use crate::{
    codec::Encoder,
    sink::{PublishRejected, Sink},
};
use anyhow::Context as _;
use aws_sdk_sqs::{
    Client,
    error::{DisplayErrorContext, ProvideErrorMetadata},
    types::MessageAttributeValue,
};
use std::{
    collections::HashMap,
    fmt,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::OnceCell;

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
pub struct SqsSink<C, T> {
    config: SqsSinkConfig,
    codec: Arc<C>,
    client: Arc<OnceCell<Client>>,
    closed: Arc<AtomicBool>,
    marker: PhantomData<fn(T)>,
}

impl<C, T> Clone for SqsSink<C, T> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            codec: self.codec.clone(),
            client: self.client.clone(),
            closed: self.closed.clone(),
            marker: PhantomData,
        }
    }
}

impl<C: Default, T> SqsSink<C, T> {
    /// A sink with a default-constructed codec.
    pub fn new(config: SqsSinkConfig) -> Self {
        Self::with_codec(config, C::default())
    }
}

impl<C, T> SqsSink<C, T> {
    /// A sink that uses an existing codec instance, such as a configured `Avro` codec.
    pub fn with_codec(config: SqsSinkConfig, codec: C) -> Self {
        Self {
            config,
            codec: Arc::new(codec),
            client: Arc::default(),
            closed: Arc::default(),
            marker: PhantomData,
        }
    }

    async fn client(&self) -> anyhow::Result<&Client> {
        anyhow::ensure!(!self.closed.load(Ordering::Acquire), "SQS sink is closed");
        self.client
            .get_or_try_init(|| async {
                self.config.validate()?;
                anyhow::Ok(client(&self.config.endpoint()).await)
            })
            .await
    }
}

impl<C, T> Sink<SqsPublish<T>> for SqsSink<C, T>
where
    C: Encoder<T>,
    T: Send + Sync + 'static,
{
    type Prepared = SqsPrepared;

    /// Encodes the value as the message body, which SQS requires to be text.
    /// A codec whose output is not UTF-8, such as Protobuf, fails here.
    fn prepare(&self, output: SqsPublish<T>) -> anyhow::Result<Self::Prepared> {
        let body = String::from_utf8(self.codec.encode(&output.value)?)
            .context("SQS message bodies must be UTF-8 text")?;
        let fifo = self.config.endpoint().is_fifo();
        if fifo {
            anyhow::ensure!(
                output.message_group_id.is_some(),
                "messages sent to an SQS FIFO queue need a message group ID"
            );
            anyhow::ensure!(
                output.delay.is_none(),
                "SQS FIFO queues accept no per-message delay"
            );
        }
        if let Some(delay) = output.delay {
            anyhow::ensure!(
                delay <= MAX_DELAY,
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

    async fn publish(&self, output: &Self::Prepared) -> anyhow::Result<()> {
        let client = self.client().await?;
        let result = client
            .send_message()
            .queue_url(&self.config.queue_url)
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
                let error =
                    anyhow::anyhow!("{}", DisplayErrorContext(&error)).context("sending to SQS");
                Err(if rejected {
                    PublishRejected::wrap(error)
                } else {
                    error
                })
            }
        }
    }

    async fn close(&self) -> anyhow::Result<()> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }
}
