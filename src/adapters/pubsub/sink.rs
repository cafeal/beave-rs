use super::{client::publisher, config::PubSubSinkConfig, record::PubSubPublish};
use crate::{
    codec::Encoder,
    error::{BoxError, Error, ensure},
    sink::{PublishRejected, Sink},
};
use google_cloud_gax::error::rpc::Code;
use google_cloud_pubsub::{client::Publisher, error::PublishError, model::Message};
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::OnceCell;

/// Most attributes Pub/Sub accepts per message.
const MAX_ATTRIBUTES: usize = 100;
/// Longest attribute name Pub/Sub accepts, in bytes.
const MAX_ATTRIBUTE_NAME: usize = 256;
/// Longest attribute value Pub/Sub accepts, in bytes.
pub(super) const MAX_ATTRIBUTE_VALUE: usize = 1024;
/// Longest ordering key Pub/Sub accepts, in bytes.
const MAX_ORDERING_KEY: usize = 1024;

/// A Pub/Sub message ready to publish: the encoded data, attributes, and
/// ordering key.
#[derive(Clone)]
pub struct PubSubPrepared {
    message: Message,
}

impl PubSubPrepared {
    /// The encoded message data.
    pub fn data(&self) -> &[u8] {
        &self.message.data
    }
}

impl fmt::Debug for PubSubPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PubSubPrepared")
            .field("data_len", &self.message.data.len())
            .field(
                "attributes",
                &self.message.attributes.keys().collect::<Vec<_>>(),
            )
            .field("ordering_key", &self.message.ordering_key)
            .finish()
    }
}

/// Publishes each output to one topic. A publication succeeds once Pub/Sub
/// has stored the message and returned its ID.
pub struct PubSubSink<C> {
    config: PubSubSinkConfig,
    codec: Arc<C>,
    publisher: Arc<OnceCell<Publisher>>,
    closed: Arc<AtomicBool>,
}

impl<C> Clone for PubSubSink<C> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            codec: self.codec.clone(),
            publisher: self.publisher.clone(),
            closed: self.closed.clone(),
        }
    }
}

impl<C: Default> PubSubSink<C> {
    /// A sink with a default-constructed codec.
    pub fn new(config: PubSubSinkConfig) -> Self {
        Self::with_codec(config, C::default())
    }
}

impl<C> PubSubSink<C> {
    /// A sink that uses an existing codec instance, such as a configured `Avro` codec.
    pub fn with_codec(config: PubSubSinkConfig, codec: C) -> Self {
        Self {
            config,
            codec: Arc::new(codec),
            publisher: Arc::default(),
            closed: Arc::default(),
        }
    }

    async fn publisher(&self) -> Result<&Publisher, BoxError> {
        ensure!(
            !self.closed.load(Ordering::Acquire),
            Error::closed,
            "Pub/Sub sink is closed"
        );
        self.publisher
            .get_or_try_init(|| async {
                self.config.validate()?;
                publisher(&self.config).await
            })
            .await
    }
}

impl<C> Sink<PubSubPublish<C::Value>> for PubSubSink<C>
where
    C: Encoder<Value: Send + Sync + 'static>,
{
    type Prepared = PubSubPrepared;

    /// Encodes the value as the message data and checks the attribute and
    /// ordering key limits.
    fn prepare(&self, output: PubSubPublish<C::Value>) -> Result<Self::Prepared, BoxError> {
        let data = self.codec.encode(&output.value)?;
        ensure!(
            output.attributes.len() <= MAX_ATTRIBUTES,
            Error::invalid_record,
            "Pub/Sub messages accept at most {MAX_ATTRIBUTES} attributes"
        );
        for (name, value) in &output.attributes {
            ensure!(
                !name.is_empty() && name.len() <= MAX_ATTRIBUTE_NAME,
                Error::invalid_record,
                "Pub/Sub attribute names must be 1 to {MAX_ATTRIBUTE_NAME} bytes: {name:?}"
            );
            ensure!(
                !name.starts_with("goog"),
                Error::invalid_record,
                "Pub/Sub reserves attribute names starting with goog: {name:?}"
            );
            ensure!(
                value.len() <= MAX_ATTRIBUTE_VALUE,
                Error::invalid_record,
                "Pub/Sub attribute {name} is longer than {MAX_ATTRIBUTE_VALUE} bytes"
            );
        }
        let ordering_key = output.ordering_key.unwrap_or_default();
        ensure!(
            ordering_key.len() <= MAX_ORDERING_KEY,
            Error::invalid_record,
            "Pub/Sub ordering keys must be at most {MAX_ORDERING_KEY} bytes"
        );
        ensure!(
            !data.is_empty() || !output.attributes.is_empty(),
            Error::invalid_record,
            "Pub/Sub messages need data or at least one attribute"
        );
        Ok(PubSubPrepared {
            message: Message::new()
                .set_data(data)
                .set_attributes(output.attributes)
                .set_ordering_key(ordering_key),
        })
    }

    async fn publish(&self, output: &Self::Prepared) -> Result<(), BoxError> {
        let publisher = self.publisher().await?;
        let error = match publisher.publish(output.message.clone()).await {
            Ok(_) => return Ok(()),
            Err(error) => error,
        };
        let ordering_key = &output.message.ordering_key;
        if !ordering_key.is_empty() {
            // The client pauses an ordering key after a failure. The runtime
            // retries or dead-letters this output itself, so later outputs of
            // the key may be published.
            publisher.resume_publish(ordering_key.as_str());
        }
        let rejected = match &error {
            PublishError::Rpc(error) => error
                .status()
                .is_some_and(|status| status.code == Code::InvalidArgument),
            _ => false,
        };
        let error = Error::wrap("publishing to Pub/Sub", error);
        Err(if rejected {
            PublishRejected::wrap(error)
        } else {
            error.into()
        })
    }

    /// Sends the messages still batched and stops new publications.
    async fn close(&self) -> Result<(), BoxError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        if let Some(publisher) = self.publisher.get() {
            publisher.flush().await;
        }
        Ok(())
    }
}
