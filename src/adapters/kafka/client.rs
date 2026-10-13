use super::{
    config::{SinkConfig, SourceConfig, TRANSACTIONAL_ID, validate_properties},
    sink::KafkaSink,
    source::KafkaSource,
};
use crate::error::{BoxError, Error, ensure};
use rdkafka::{ClientConfig, producer::FutureProducer};
use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

/// A Kafka cluster connection that the sources and sinks built from it share.
///
/// Every sink built from one handle publishes through one producer, created on
/// the first publication of any of them. Each source has a consumer of its
/// own, because a consumer belongs to one consumer group, and each
/// [`transactional`](KafkaSink::transactional) sink has a producer of its own,
/// because a transactional producer has one open transaction at a time. All of
/// them use the handle's bootstrap servers and properties.
///
/// Each builder method returns a handle with a producer of its own, so
/// configure the handle before building sources and sinks from it.
///
/// ```no_run
/// use beavers::{Json, adapters::kafka::Kafka};
///
/// let kafka = Kafka::new("localhost:9092").sasl_plain("user", "password");
/// let orders = kafka.source::<Json, String>("billing", ["orders"]);
/// let invoices = kafka.sink::<Json, String>("invoices").max_pending(500);
/// ```
#[derive(Clone)]
pub struct Kafka {
    shared: Arc<Shared>,
}

#[derive(Clone)]
struct Settings {
    brokers: String,
    properties: HashMap<String, String>,
    producer_properties: HashMap<String, String>,
}

struct Shared {
    settings: Settings,
    producer: Mutex<Option<FutureProducer>>,
}

impl Kafka {
    /// A handle for the cluster at `brokers`, set as librdkafka
    /// `bootstrap.servers`.
    pub fn new(brokers: impl Into<String>) -> Self {
        Self::from_settings(Settings {
            brokers: brokers.into(),
            properties: HashMap::new(),
            producer_properties: HashMap::new(),
        })
    }

    /// Sets a librdkafka property on every consumer and producer of the
    /// handle, such as `client.id` or `ssl.ca.location`.
    pub fn property(self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.configure(|settings| {
            settings.properties.insert(name.into(), value.into());
        })
    }

    /// Sets a librdkafka property on the producers of the handle only, such as
    /// `linger.ms` or `compression.type`. Set a transactional ID with
    /// [`KafkaSink::transactional`] instead of `transactional.id`.
    pub fn producer_property(self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.configure(|settings| {
            settings
                .producer_properties
                .insert(name.into(), value.into());
        })
    }

    /// Authenticates with SASL/PLAIN, over TLS unless `security.protocol` is
    /// set otherwise.
    ///
    /// TLS needs librdkafka built with SSL support, which the application
    /// enables with the `ssl` or `ssl-vendored` feature of its own `rdkafka`
    /// dependency.
    pub fn sasl_plain(self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.configure(|settings| {
            let properties = &mut settings.properties;
            properties
                .entry("security.protocol".into())
                .or_insert_with(|| "SASL_SSL".into());
            properties.insert("sasl.mechanism".into(), "PLAIN".into());
            properties.insert("sasl.username".into(), username.into());
            properties.insert("sasl.password".into(), password.into());
        })
    }

    /// A source that consumes `topics` as consumer group `group_id` and decodes
    /// with the codec's default value.
    ///
    /// The consumer is created on the first receive, which also validates the
    /// settings.
    pub fn source<C: Default, T>(
        &self,
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> KafkaSource<C, T> {
        self.source_with_codec(C::default(), group_id, topics)
    }

    /// A source like [`source`](Self::source) that decodes with `codec`.
    pub fn source_with_codec<C, T>(
        &self,
        codec: C,
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> KafkaSource<C, T> {
        let topics = topics.into_iter().map(Into::into).collect();
        KafkaSource::new(
            self.clone(),
            SourceConfig::new(group_id.into(), topics),
            codec,
        )
    }

    /// A sink that publishes to `topic` and encodes with the codec's default
    /// value.
    ///
    /// Defaults: a `max_pending` of 1000 and a 30 second `close_timeout`.
    pub fn sink<C: Default, T>(&self, topic: impl Into<String>) -> KafkaSink<C, T> {
        self.sink_with_codec(C::default(), topic)
    }

    /// A sink like [`sink`](Self::sink) that encodes with `codec`.
    pub fn sink_with_codec<C, T>(&self, codec: C, topic: impl Into<String>) -> KafkaSink<C, T> {
        KafkaSink::new(self.clone(), SinkConfig::new(topic.into()), codec)
    }

    /// Checks the settings without network access.
    ///
    /// Fails when the brokers are empty, a property has an empty name or a
    /// value containing a NUL byte, or `transactional.id` is set as a producer
    /// property.
    pub fn validate(&self) -> Result<(), Error> {
        let settings = &self.shared.settings;
        ensure!(
            !settings.brokers.trim().is_empty(),
            Error::config,
            "Kafka brokers are required"
        );
        ensure!(
            !settings.properties.contains_key(TRANSACTIONAL_ID)
                && !settings.producer_properties.contains_key(TRANSACTIONAL_ID),
            Error::config,
            "set the Kafka transactional ID with KafkaSink::transactional"
        );
        validate_properties(&settings.properties)?;
        validate_properties(&settings.producer_properties)
    }

    /// A client configuration with the bootstrap servers and the properties
    /// every client of the handle shares.
    pub(super) fn client_config(&self) -> ClientConfig {
        let settings = &self.shared.settings;
        let mut config = ClientConfig::new();
        for (key, value) in &settings.properties {
            config.set(key, value);
        }
        config.set("bootstrap.servers", &settings.brokers);
        config
    }

    /// A client configuration for a producer of the handle.
    pub(super) fn producer_config(&self) -> ClientConfig {
        let mut config = self.client_config();
        for (key, value) in &self.shared.settings.producer_properties {
            config.set(key, value);
        }
        config
    }

    /// The producer the handle's non-transactional sinks share, created on
    /// first use.
    pub(super) fn producer(&self) -> Result<FutureProducer, BoxError> {
        let mut slot = self.shared.producer.lock().unwrap();
        if let Some(producer) = &*slot {
            return Ok(producer.clone());
        }
        self.validate()?;
        let producer: FutureProducer = self.producer_config().create()?;
        *slot = Some(producer.clone());
        Ok(producer)
    }

    /// The shared producer if it has been created.
    pub(super) fn created_producer(&self) -> Option<FutureProducer> {
        self.shared.producer.lock().unwrap().clone()
    }

    fn configure(self, change: impl FnOnce(&mut Settings)) -> Self {
        let mut settings = self.shared.settings.clone();
        change(&mut settings);
        Self::from_settings(settings)
    }

    fn from_settings(settings: Settings) -> Self {
        Self {
            shared: Arc::new(Shared {
                settings,
                producer: Mutex::new(None),
            }),
        }
    }
}

/// Shows property names only, because their values can hold credentials.
impl fmt::Debug for Kafka {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let settings = &self.shared.settings;
        let mut properties: Vec<_> = settings.properties.keys().collect();
        properties.sort();
        let mut producer_properties: Vec<_> = settings.producer_properties.keys().collect();
        producer_properties.sort();
        f.debug_struct("Kafka")
            .field("brokers", &settings.brokers)
            .field("properties", &properties)
            .field("producer_properties", &producer_properties)
            .finish_non_exhaustive()
    }
}
