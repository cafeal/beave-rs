//! The namespace connection that the sources and sinks of a handle share.
use super::{
    config::{Credential, Settings, SinkConfig, SourceConfig},
    connection::Connection,
    sink::ServiceBusSink,
    source::ServiceBusSource,
};
use crate::error::Error;
use std::{fmt, sync::Arc};
use tokio::sync::Mutex;

/// A Service Bus namespace connection that the sources and sinks built from it
/// share.
///
/// The handle opens one AMQP connection with one session, and each source and
/// sink attaches its links to it. Each entity is authorized once on the
/// connection through claims-based security; tokens signed with a shared
/// access key are renewed before they expire. The connection opens on the
/// first use that needs it, which also validates the settings, and is opened
/// again after it is lost. It closes once this handle and every source and
/// sink built from it have been dropped.
///
/// Each builder method returns a handle with a connection of its own, so
/// configure the handle before building sources and sinks from it. `Debug`
/// output hides the key and the signature.
///
/// ```no_run
/// use beavers::{Json, adapters::servicebus::ServiceBus};
///
/// let bus = ServiceBus::new("contoso.servicebus.windows.net")
///     .shared_access_key("RootManageSharedAccessKey", "key");
/// let orders = bus.source::<Json<String>>("orders").prefetch(20);
/// let invoices = bus.sink::<Json<String>>("invoices");
/// ```
#[derive(Clone)]
pub struct ServiceBus {
    shared: Arc<Shared>,
}

struct Shared {
    settings: Settings,
    connection: Mutex<Option<Arc<Connection>>>,
}

impl ServiceBus {
    /// A handle for the fully qualified `namespace`, such as
    /// `contoso.servicebus.windows.net`, optionally followed by `:port`. Set a
    /// credential with [`shared_access_key`](Self::shared_access_key) or
    /// [`shared_access_signature`](Self::shared_access_signature).
    pub fn new(namespace: impl Into<String>) -> Self {
        Self::from_settings(Settings::new(namespace.into()))
    }

    /// Reads a connection string as the Azure portal and the emulator show it,
    /// such as `Endpoint=sb://contoso.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...`.
    ///
    /// It accepts `Endpoint`, `SharedAccessKeyName` with `SharedAccessKey` or
    /// `SharedAccessSignature`, and `UseDevelopmentEmulator=true`. A string with
    /// `EntityPath` is refused: pass the entity to the source or sink instead,
    /// where the key of an entity-level policy still works for that entity.
    pub fn from_connection_string(connection_string: &str) -> Result<Self, Error> {
        Settings::from_connection_string(connection_string).map(Self::from_settings)
    }

    /// Authenticates with a shared access policy `name`, such as
    /// `RootManageSharedAccessKey`, and its primary or secondary `key`. The
    /// adapter signs a token for each entity and renews it before it expires.
    pub fn shared_access_key(self, name: impl Into<String>, key: impl Into<String>) -> Self {
        self.configure(|settings| {
            settings.credential = Some(Credential::SharedAccessKey {
                name: name.into(),
                key: key.into(),
            })
        })
    }

    /// Authenticates with a signed token,
    /// `SharedAccessSignature sr=...&sig=...&se=...&skn=...`, whose resource
    /// covers the entities. It cannot be renewed, so the connection fails once
    /// it expires.
    pub fn shared_access_signature(self, signature: impl Into<String>) -> Self {
        self.configure(|settings| {
            settings.credential = Some(Credential::SharedAccessSignature(signature.into()))
        })
    }

    /// Connects with plain AMQP to port 5672 instead of AMQP over TLS to port
    /// 5671, as the Service Bus emulator requires. A port in the namespace
    /// overrides either default.
    pub fn emulator(self) -> Self {
        self.configure(|settings| settings.emulator = true)
    }

    /// A source that receives from `entity_path` in peek-lock mode and decodes
    /// with the codec's default value. The entity is a queue such as `orders`,
    /// a topic subscription as `events/Subscriptions/billing`, or a dead-letter
    /// subqueue such as `orders/$DeadLetterQueue`. Session-enabled entities are
    /// not supported.
    ///
    /// Defaults: a `prefetch` of 10.
    pub fn source<C: Default>(&self, entity_path: impl Into<String>) -> ServiceBusSource<C> {
        self.source_with_codec(C::default(), entity_path)
    }

    /// A source like [`source`](Self::source) that decodes with `codec`.
    pub fn source_with_codec<C>(
        &self,
        codec: C,
        entity_path: impl Into<String>,
    ) -> ServiceBusSource<C> {
        ServiceBusSource::new(self.clone(), SourceConfig::new(entity_path.into()), codec)
    }

    /// A sink that sends every output to the queue or topic `entity_path` and
    /// encodes with the codec's default value.
    ///
    /// Defaults: a `max_pending` of 1000.
    pub fn sink<C: Default>(&self, entity_path: impl Into<String>) -> ServiceBusSink<C> {
        self.sink_with_codec(C::default(), entity_path)
    }

    /// A sink like [`sink`](Self::sink) that encodes with `codec`.
    pub fn sink_with_codec<C>(
        &self,
        codec: C,
        entity_path: impl Into<String>,
    ) -> ServiceBusSink<C> {
        ServiceBusSink::new(self.clone(), SinkConfig::new(entity_path.into()), codec)
    }

    /// Checks the settings without contacting Service Bus.
    ///
    /// Fails when the namespace is not a host name with an optional port, no
    /// credential is set, a shared access key lacks its name or key, or a
    /// signature does not start with `SharedAccessSignature `.
    pub fn validate(&self) -> Result<(), Error> {
        self.shared.settings.validate()
    }

    /// The shared connection with `entities` authorized, connecting on first
    /// use and again after the connection was lost.
    pub(super) async fn connection(&self, entities: &[String]) -> Result<Arc<Connection>, Error> {
        let connection = {
            let mut slot = self.shared.connection.lock().await;
            match slot.as_ref() {
                Some(connection) if !connection.is_lost().await => connection.clone(),
                _ => {
                    self.validate()?;
                    let connection = Arc::new(Connection::open(&self.shared.settings).await?);
                    *slot = Some(connection.clone());
                    connection
                }
            }
        };
        connection.authorize(entities).await?;
        Ok(connection)
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
                connection: Mutex::new(None),
            }),
        }
    }
}

impl fmt::Debug for ServiceBus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let settings = &self.shared.settings;
        f.debug_struct("ServiceBus")
            .field("namespace", &settings.namespace)
            .field("credential", &settings.credential)
            .field("emulator", &settings.emulator)
            .finish_non_exhaustive()
    }
}
