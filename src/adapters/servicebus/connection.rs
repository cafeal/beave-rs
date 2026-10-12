//! AMQP connections to a namespace, authorized for a set of entities.
use super::{auth::SAS_TOKEN_TYPE, config::ServiceBusConnection};
use crate::error::{Context as _, Error, ensure};
use fe2o3_amqp::{
    Session,
    connection::{Connection as AmqpConnection, ConnectionHandle},
    sasl_profile::SaslProfile,
    session::SessionHandle,
    types::primitives::Timestamp,
};
use fe2o3_amqp_cbs::{client::CbsClient, token::CbsToken};
use rustls::{ClientConfig, RootCertStore, crypto::ring, pki_types::ServerName};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{net::TcpStream, task::JoinHandle, time::timeout};
use tokio_rustls::TlsConnector;
use tracing::warn;

/// Longest time opening a connection and authorizing its entities may take.
const OPEN_TIMEOUT: Duration = Duration::from_secs(60);
/// Longest time closing a connection may take.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the connection may stay silent before either side considers it
/// lost. Service Bus answers with heartbeats.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// How long before a token expires it is renewed.
const RENEW_BEFORE_EXPIRY: Duration = Duration::from_secs(10 * 60);
/// Delay before a failed token renewal is attempted again.
const RENEW_RETRY: Duration = Duration::from_secs(10);

/// One AMQP connection with one session, whose entities have been authorized
/// through claims-based security. A background task renews their tokens.
pub(super) struct Connection {
    connection: ConnectionHandle<()>,
    pub(super) session: SessionHandle<()>,
    renewal: Option<JoinHandle<()>>,
}

impl Connection {
    /// Connects to the namespace and authorizes `entities`, such as `orders`
    /// and `orders/$management`.
    pub(super) async fn open(
        config: &ServiceBusConnection,
        entities: &[String],
    ) -> Result<Self, Error> {
        timeout(OPEN_TIMEOUT, Self::open_now(config, entities))
            .await
            .context("connecting to Service Bus timed out")?
    }

    async fn open_now(config: &ServiceBusConnection, entities: &[String]) -> Result<Self, Error> {
        let mut connection = connect(config).await?;
        let session = Session::begin(&mut connection)
            .await
            .context("beginning a Service Bus session");
        let mut session = match session {
            Ok(session) => session,
            Err(error) => {
                let _ = connection.close().await;
                return Err(error);
            }
        };
        let audiences: Vec<String> = entities
            .iter()
            .map(|entity| format!("sb://{}/{entity}", config.host()))
            .collect();
        let authorized = async {
            let mut cbs = CbsClient::attach(&mut session)
                .await
                .context("attaching the Service Bus claims-based security link")?;
            let expires = put_tokens(&mut cbs, config, &audiences).await?;
            Ok::<_, Error>((cbs, expires))
        }
        .await;
        let (cbs, expires) = match authorized {
            Ok(authorized) => authorized,
            Err(error) => {
                let _ = session.close().await;
                let _ = connection.close().await;
                return Err(error);
            }
        };
        let renewal = config
            .credential
            .renewable()
            .then(|| tokio::spawn(renew(cbs, config.clone(), audiences, expires)));
        Ok(Self {
            connection,
            session,
            renewal,
        })
    }

    /// Ends the session and closes the connection, which detaches its links.
    pub(super) async fn close(mut self) {
        if let Some(renewal) = self.renewal.take() {
            renewal.abort();
        }
        let _ = timeout(CLOSE_TIMEOUT, async {
            let _ = self.session.close().await;
            let _ = self.connection.close().await;
        })
        .await;
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(renewal) = self.renewal.take() {
            renewal.abort();
        }
    }
}

/// A name unique within this process, for a container or a link.
pub(super) fn unique_name(kind: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "beavers-{kind}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// Opens the transport and the AMQP connection, authenticating anonymously:
/// entities are authorized afterwards with tokens.
async fn connect(config: &ServiceBusConnection) -> Result<ConnectionHandle<()>, Error> {
    let host = config.host();
    let stream = TcpStream::connect((host, config.port()))
        .await
        .with_context(|| format!("connecting to Service Bus at {}", config.namespace))?;
    let builder = AmqpConnection::builder()
        .container_id(unique_name("connection"))
        .hostname(host)
        .sasl_profile(SaslProfile::Anonymous)
        .idle_time_out(IDLE_TIMEOUT.as_millis() as u32);
    let connection = if config.emulator {
        builder.open_with_stream(stream).await
    } else {
        let server_name = ServerName::try_from(host.to_owned())
            .context("Service Bus namespace is not a valid TLS server name")?;
        let stream = tls_connector()?
            .connect(server_name, stream)
            .await
            .context("establishing TLS with Service Bus")?;
        builder.open_with_stream(stream).await
    };
    connection.context("opening the Service Bus AMQP connection")
}

/// Verifies the namespace against the platform's root certificates.
fn tls_connector() -> Result<TlsConnector, Error> {
    let mut roots = RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    roots.add_parsable_certificates(native.certs);
    ensure!(
        !roots.is_empty(),
        Error::msg,
        "failed to load the platform's root certificates"
    );
    let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .context("configuring TLS for Service Bus")?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(config)))
}

/// Authorizes every audience and returns when the earliest token expires.
async fn put_tokens(
    cbs: &mut CbsClient,
    config: &ServiceBusConnection,
    audiences: &[String],
) -> Result<SystemTime, Error> {
    let mut earliest = None::<SystemTime>;
    for audience in audiences {
        let token = config.credential.token(audience, SystemTime::now())?;
        let expires = token
            .expires
            .duration_since(UNIX_EPOCH)
            .context("the clock is before the Unix epoch")?
            .as_millis() as i64;
        cbs.put_token(
            audience.as_str(),
            CbsToken::new(
                token.value,
                SAS_TOKEN_TYPE,
                Timestamp::from_milliseconds(expires),
            ),
        )
        .await
        .with_context(|| format!("authorizing {audience} with Service Bus"))?;
        earliest = Some(earliest.map_or(token.expires, |earliest| earliest.min(token.expires)));
    }
    earliest.context("no Service Bus entity to authorize")
}

/// Puts new tokens before the current ones expire. A failed renewal is retried
/// until the tokens expire, when Service Bus detaches the links and the source
/// or sink reconnects.
async fn renew(
    mut cbs: CbsClient,
    config: ServiceBusConnection,
    audiences: Vec<String>,
    mut expires: SystemTime,
) {
    loop {
        let due = expires
            .checked_sub(RENEW_BEFORE_EXPIRY)
            .and_then(|due| due.duration_since(SystemTime::now()).ok())
            .unwrap_or(RENEW_RETRY);
        tokio::time::sleep(due).await;
        match put_tokens(&mut cbs, &config, &audiences).await {
            Ok(renewed) => expires = renewed,
            Err(error) => {
                warn!(error = %format!("{error:#}"), "renewing Service Bus tokens failed")
            }
        }
    }
}
