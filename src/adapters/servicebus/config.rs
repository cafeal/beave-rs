use crate::adapters::pending::PendingLimit;
use crate::error::{Error, ensure};
use std::fmt;

/// Port of AMQP over TLS, which Azure Service Bus requires.
const TLS_PORT: u16 = 5671;
/// Port of plain AMQP, which the Service Bus emulator serves.
const PLAIN_PORT: u16 = 5672;

/// How a source or sink authenticates to a Service Bus namespace.
///
/// The `Debug` output hides the key and the signature.
#[derive(Clone)]
pub enum ServiceBusCredential {
    /// A shared access policy name and key. The adapter signs a token for each
    /// entity it uses and renews it before it expires.
    SharedAccessKey {
        /// The policy name, such as `RootManageSharedAccessKey`.
        name: String,
        /// The policy's primary or secondary key.
        key: String,
    },
    /// A signed token, `SharedAccessSignature sr=...&sig=...&se=...&skn=...`,
    /// whose resource covers the entity. It cannot be renewed, so the
    /// connection fails once it expires.
    SharedAccessSignature(String),
}

impl fmt::Debug for ServiceBusCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SharedAccessKey { name, .. } => f
                .debug_struct("SharedAccessKey")
                .field("name", name)
                .finish_non_exhaustive(),
            Self::SharedAccessSignature(_) => f.write_str("SharedAccessSignature(..)"),
        }
    }
}

/// A Service Bus namespace and the credential to reach it with.
#[derive(Clone, Debug)]
pub struct ServiceBusConnection {
    /// The fully qualified namespace, such as `contoso.servicebus.windows.net`,
    /// optionally followed by `:port`.
    pub namespace: String,
    /// The credential the source or sink authenticates with.
    pub credential: ServiceBusCredential,
    /// Connects with plain AMQP to port 5672 instead of AMQP over TLS to port
    /// 5671, as the Service Bus emulator requires. A port in `namespace`
    /// overrides either default.
    pub emulator: bool,
}

impl ServiceBusConnection {
    /// A TLS connection to `namespace` with `credential`.
    pub fn new(namespace: impl Into<String>, credential: ServiceBusCredential) -> Self {
        Self {
            namespace: namespace.into(),
            credential,
            emulator: false,
        }
    }

    /// Reads a connection string as the Azure portal and the emulator show it,
    /// such as `Endpoint=sb://contoso.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...`.
    ///
    /// It accepts `Endpoint`, `SharedAccessKeyName` with `SharedAccessKey` or
    /// `SharedAccessSignature`, and `UseDevelopmentEmulator=true`. A string with
    /// `EntityPath` is refused: set the entity in the source or sink
    /// configuration instead, where the key of an entity-level policy still
    /// works for that entity.
    pub fn from_connection_string(connection_string: &str) -> Result<Self, Error> {
        let mut endpoint = None;
        let mut key_name = None;
        let mut key = None;
        let mut signature = None;
        let mut emulator = false;
        for part in connection_string.split(';').map(str::trim) {
            if part.is_empty() {
                continue;
            }
            let Some((name, value)) = part.split_once('=') else {
                return Err(Error::config(
                    "Service Bus connection string part without a value",
                ));
            };
            let value = value.trim();
            match name.trim().to_ascii_lowercase().as_str() {
                "endpoint" => endpoint = Some(value),
                "sharedaccesskeyname" => key_name = Some(value),
                "sharedaccesskey" => key = Some(value),
                "sharedaccesssignature" => signature = Some(value),
                "usedevelopmentemulator" => emulator = value.eq_ignore_ascii_case("true"),
                "entitypath" => {
                    return Err(Error::config(
                        "Service Bus connection string names an entity; remove EntityPath and \
                     set the entity in the source or sink configuration",
                    ));
                }
                other => {
                    return Err(Error::config(format!(
                        "unknown Service Bus connection string key {other:?}"
                    )));
                }
            }
        }
        let endpoint = endpoint
            .ok_or_else(|| Error::config("Service Bus connection string needs Endpoint"))?;
        let namespace = endpoint
            .strip_prefix("sb://")
            .ok_or_else(|| Error::config("Service Bus Endpoint must start with sb://"))?
            .trim_end_matches('/');
        let credential = match (key_name, key, signature) {
            (Some(name), Some(key), None) => ServiceBusCredential::SharedAccessKey {
                name: name.to_owned(),
                key: key.to_owned(),
            },
            (None, None, Some(signature)) => {
                ServiceBusCredential::SharedAccessSignature(signature.to_owned())
            }
            _ => {
                return Err(Error::config(
                    "Service Bus connection string needs SharedAccessKeyName and SharedAccessKey, \
                 or SharedAccessSignature",
                ));
            }
        };
        let connection = Self {
            namespace: namespace.to_owned(),
            credential,
            emulator,
        };
        connection.validate()?;
        Ok(connection)
    }

    fn validate(&self) -> Result<(), Error> {
        ensure!(
            !self.namespace.is_empty()
                && !self
                    .namespace
                    .chars()
                    .any(|c| c.is_whitespace() || c == '/'),
            Error::config,
            "Service Bus namespace must be a host name such as contoso.servicebus.windows.net"
        );
        if let Some((_, port)) = self.namespace.rsplit_once(':') {
            port.parse::<u16>()
                .map_err(|_| Error::config("Service Bus namespace has an invalid port"))?;
        }
        match &self.credential {
            ServiceBusCredential::SharedAccessKey { name, key } => ensure!(
                !name.is_empty() && !key.is_empty(),
                Error::config,
                "Service Bus shared access key needs a name and a key"
            ),
            ServiceBusCredential::SharedAccessSignature(signature) => ensure!(
                signature.starts_with("SharedAccessSignature "),
                Error::config,
                "Service Bus shared access signature must start with \"SharedAccessSignature \""
            ),
        }
        Ok(())
    }

    /// The namespace's host name, without a port.
    pub(super) fn host(&self) -> &str {
        match self.namespace.rsplit_once(':') {
            Some((host, _)) => host,
            None => &self.namespace,
        }
    }

    /// The port to connect to.
    pub(super) fn port(&self) -> u16 {
        match self.namespace.rsplit_once(':') {
            Some((_, port)) => port.parse().unwrap_or(TLS_PORT),
            None if self.emulator => PLAIN_PORT,
            None => TLS_PORT,
        }
    }
}

/// Checks an entity path such as `orders` or `events/Subscriptions/billing`.
fn validate_entity_path(path: &str) -> Result<(), Error> {
    ensure!(
        !path.is_empty()
            && !path.starts_with('/')
            && !path.ends_with('/')
            && !path.chars().any(char::is_whitespace),
        Error::config,
        "Service Bus entity path must be a queue, topic, or subscription path without \
         surrounding slashes or whitespace"
    );
    Ok(())
}

/// Configuration of a [`ServiceBusSource`](super::ServiceBusSource).
#[derive(Clone, Debug)]
pub struct ServiceBusSourceConfig {
    /// The namespace and credential.
    pub connection: ServiceBusConnection,
    /// The entity to receive from: a queue such as `orders`, a topic
    /// subscription as `events/Subscriptions/billing`, or a dead-letter
    /// subqueue such as `orders/$DeadLetterQueue`. Session-enabled entities are
    /// not supported.
    pub entity_path: String,
    /// Messages Service Bus may send ahead of `receive` (the link credit). Each
    /// is locked from the moment Service Bus sends it, so keep it small enough
    /// that prefetched messages are received well within the lock duration.
    pub prefetch: u32,
}

impl ServiceBusSourceConfig {
    /// A configuration with a `prefetch` of 10.
    pub fn new(connection: ServiceBusConnection, entity_path: impl Into<String>) -> Self {
        Self {
            connection,
            entity_path: entity_path.into(),
            prefetch: 10,
        }
    }

    /// Checks the settings without contacting Service Bus.
    pub fn validate(&self) -> Result<(), Error> {
        self.connection.validate()?;
        validate_entity_path(&self.entity_path)?;
        ensure!(
            self.prefetch > 0,
            Error::config,
            "Service Bus prefetch must be greater than zero"
        );
        Ok(())
    }
}

/// Configuration of a [`ServiceBusSink`](super::ServiceBusSink).
#[derive(Clone, Debug)]
pub struct ServiceBusSinkConfig {
    /// The namespace and credential.
    pub connection: ServiceBusConnection,
    /// The queue or topic every output is sent to.
    pub entity_path: String,
    /// Messages sent and not yet accepted by Service Bus. `submit` waits while
    /// this many are outstanding.
    pub max_pending: usize,
}

impl ServiceBusSinkConfig {
    /// A configuration with a `max_pending` of 1000.
    pub fn new(connection: ServiceBusConnection, entity_path: impl Into<String>) -> Self {
        Self {
            connection,
            entity_path: entity_path.into(),
            max_pending: 1000,
        }
    }

    /// Checks the settings without contacting Service Bus.
    pub fn validate(&self) -> Result<(), Error> {
        self.connection.validate()?;
        validate_entity_path(&self.entity_path)?;
        PendingLimit::validate(self.max_pending, "Service Bus")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EMULATOR: &str = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

    #[test]
    fn connection_strings_are_read() {
        let emulator = ServiceBusConnection::from_connection_string(EMULATOR).unwrap();
        assert_eq!((emulator.host(), emulator.port()), ("localhost", 5672));
        assert!(matches!(
            emulator.credential,
            ServiceBusCredential::SharedAccessKey { ref name, ref key }
                if name == "RootManageSharedAccessKey" && key == "SAS_KEY_VALUE"
        ));

        let azure = ServiceBusConnection::from_connection_string(
            "Endpoint=sb://contoso.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=a2V5=",
        )
        .unwrap();
        assert_eq!(
            (azure.host(), azure.port(), azure.emulator),
            ("contoso.servicebus.windows.net", 5671, false)
        );

        let custom_port = ServiceBusConnection::from_connection_string(
            "Endpoint=sb://localhost:5673;SharedAccessSignature=SharedAccessSignature sr=x&sig=y&se=1&skn=z;UseDevelopmentEmulator=true",
        )
        .unwrap();
        assert_eq!(
            (custom_port.host(), custom_port.port()),
            ("localhost", 5673)
        );
    }

    #[test]
    fn invalid_connection_strings_are_refused() {
        for invalid in [
            "SharedAccessKeyName=a;SharedAccessKey=b",
            "Endpoint=https://contoso.servicebus.windows.net/;SharedAccessKeyName=a;SharedAccessKey=b",
            "Endpoint=sb://contoso.servicebus.windows.net/;SharedAccessKeyName=a",
            "Endpoint=sb://contoso.servicebus.windows.net/;SharedAccessKeyName=a;SharedAccessKey=b;EntityPath=orders",
            "Endpoint=sb://contoso.servicebus.windows.net:port/;SharedAccessKeyName=a;SharedAccessKey=b",
            "Endpoint=sb://localhost;SharedAccessSignature=sig",
        ] {
            assert!(
                ServiceBusConnection::from_connection_string(invalid).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn validation_checks_values_locally() {
        let connection = ServiceBusConnection::from_connection_string(EMULATOR).unwrap();
        let valid = ServiceBusSourceConfig::new(connection.clone(), "events/Subscriptions/billing");
        valid.validate().unwrap();
        for invalid in [
            ServiceBusSourceConfig {
                entity_path: "/orders".into(),
                ..valid.clone()
            },
            ServiceBusSourceConfig {
                entity_path: String::new(),
                ..valid.clone()
            },
            ServiceBusSourceConfig {
                prefetch: 0,
                ..valid.clone()
            },
        ] {
            assert!(invalid.validate().is_err(), "{invalid:?}");
        }
        let mut sink = ServiceBusSinkConfig::new(connection, "orders");
        sink.validate().unwrap();
        sink.max_pending = 0;
        assert!(sink.validate().is_err());
    }

    #[test]
    fn debug_output_hides_secrets() {
        let connection = ServiceBusConnection::from_connection_string(EMULATOR).unwrap();
        let debug = format!("{connection:?}");
        assert!(debug.contains("RootManageSharedAccessKey"));
        assert!(!debug.contains("SAS_KEY_VALUE"));
        let signature =
            ServiceBusCredential::SharedAccessSignature("SharedAccessSignature sig=secret".into());
        assert!(!format!("{signature:?}").contains("secret"));
    }
}
