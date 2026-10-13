use crate::adapters::pending::PendingLimit;
use crate::error::{Error, ensure};
use std::fmt;

/// Port of AMQP over TLS, which Azure Service Bus requires.
const TLS_PORT: u16 = 5671;
/// Port of plain AMQP, which the Service Bus emulator serves.
const PLAIN_PORT: u16 = 5672;

/// How a handle authenticates to a Service Bus namespace.
///
/// The `Debug` output hides the key and the signature.
#[derive(Clone)]
pub(super) enum Credential {
    /// A shared access policy name and key. The adapter signs a token for each
    /// entity it uses and renews it before it expires.
    SharedAccessKey { name: String, key: String },
    /// A signed token whose resource covers the entities. It cannot be
    /// renewed, so the connection fails once it expires.
    SharedAccessSignature(String),
}

impl fmt::Debug for Credential {
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

/// The namespace and credential of a [`ServiceBus`](super::ServiceBus) handle.
#[derive(Clone, Debug)]
pub(super) struct Settings {
    /// The fully qualified namespace, optionally followed by `:port`.
    pub(super) namespace: String,
    pub(super) credential: Option<Credential>,
    /// Plain AMQP to port 5672 instead of AMQP over TLS to port 5671.
    pub(super) emulator: bool,
}

impl Settings {
    pub(super) fn new(namespace: String) -> Self {
        Self {
            namespace,
            credential: None,
            emulator: false,
        }
    }

    /// Reads a connection string as the Azure portal and the emulator show it.
    pub(super) fn from_connection_string(connection_string: &str) -> Result<Self, Error> {
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
                     pass the entity to the source or sink",
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
            (Some(name), Some(key), None) => Credential::SharedAccessKey {
                name: name.to_owned(),
                key: key.to_owned(),
            },
            (None, None, Some(signature)) => {
                Credential::SharedAccessSignature(signature.to_owned())
            }
            _ => {
                return Err(Error::config(
                    "Service Bus connection string needs SharedAccessKeyName and SharedAccessKey, \
                 or SharedAccessSignature",
                ));
            }
        };
        let settings = Self {
            namespace: namespace.to_owned(),
            credential: Some(credential),
            emulator,
        };
        settings.validate()?;
        Ok(settings)
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
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
            None => {
                return Err(Error::config(
                    "Service Bus needs a shared access key or signature",
                ));
            }
            Some(Credential::SharedAccessKey { name, key }) => ensure!(
                !name.is_empty() && !key.is_empty(),
                Error::config,
                "Service Bus shared access key needs a name and a key"
            ),
            Some(Credential::SharedAccessSignature(signature)) => ensure!(
                signature.starts_with("SharedAccessSignature "),
                Error::config,
                "Service Bus shared access signature must start with \"SharedAccessSignature \""
            ),
        }
        Ok(())
    }

    /// The validated credential.
    pub(super) fn credential(&self) -> Result<&Credential, Error> {
        self.credential
            .as_ref()
            .ok_or_else(|| Error::config("Service Bus needs a shared access key or signature"))
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

/// Settings of a [`ServiceBusSource`](super::ServiceBusSource).
#[derive(Clone, Debug)]
pub(super) struct SourceConfig {
    pub(super) entity_path: String,
    pub(super) prefetch: u32,
}

impl SourceConfig {
    pub(super) fn new(entity_path: String) -> Self {
        Self {
            entity_path,
            prefetch: 10,
        }
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
        validate_entity_path(&self.entity_path)?;
        ensure!(
            self.prefetch > 0,
            Error::config,
            "Service Bus prefetch must be greater than zero"
        );
        Ok(())
    }
}

/// Settings of a [`ServiceBusSink`](super::ServiceBusSink).
#[derive(Clone, Debug)]
pub(super) struct SinkConfig {
    pub(super) entity_path: String,
    pub(super) max_pending: usize,
}

impl SinkConfig {
    pub(super) fn new(entity_path: String) -> Self {
        Self {
            entity_path,
            max_pending: 1000,
        }
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
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
        let emulator = Settings::from_connection_string(EMULATOR).unwrap();
        assert_eq!((emulator.host(), emulator.port()), ("localhost", 5672));
        assert!(matches!(
            emulator.credential,
            Some(Credential::SharedAccessKey { ref name, ref key })
                if name == "RootManageSharedAccessKey" && key == "SAS_KEY_VALUE"
        ));

        let azure = Settings::from_connection_string(
            "Endpoint=sb://contoso.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=a2V5=",
        )
        .unwrap();
        assert_eq!(
            (azure.host(), azure.port(), azure.emulator),
            ("contoso.servicebus.windows.net", 5671, false)
        );

        let custom_port = Settings::from_connection_string(
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
                Settings::from_connection_string(invalid).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn validation_checks_values_locally() {
        let valid = SourceConfig::new("events/Subscriptions/billing".into());
        valid.validate().unwrap();
        for invalid in [
            SourceConfig::new("/orders".into()),
            SourceConfig::new(String::new()),
            SourceConfig {
                prefetch: 0,
                ..valid.clone()
            },
        ] {
            assert!(invalid.validate().is_err(), "{invalid:?}");
        }
        let mut sink = SinkConfig::new("orders".into());
        sink.validate().unwrap();
        sink.max_pending = 0;
        assert!(sink.validate().is_err());
        assert!(
            Settings::new("contoso.servicebus.windows.net".into())
                .validate()
                .is_err()
        );
    }

    #[test]
    fn debug_output_hides_secrets() {
        let connection = Settings::from_connection_string(EMULATOR).unwrap();
        let debug = format!("{connection:?}");
        assert!(debug.contains("RootManageSharedAccessKey"));
        assert!(!debug.contains("SAS_KEY_VALUE"));
        let signature =
            Credential::SharedAccessSignature("SharedAccessSignature sig=secret".into());
        assert!(!format!("{signature:?}").contains("secret"));
    }
}
