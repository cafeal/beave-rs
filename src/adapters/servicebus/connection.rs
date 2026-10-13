//! The AMQP connection a handle shares, authorized for the entities its links
//! use.
use super::{auth::SAS_TOKEN_TYPE, config::Settings};
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
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::TcpStream,
    sync::{Mutex, MutexGuard},
    task::JoinHandle,
    time::timeout,
};
use tokio_rustls::TlsConnector;
use tracing::warn;

/// Longest time opening a connection and authorizing its entities may take.
const OPEN_TIMEOUT: Duration = Duration::from_secs(60);
/// How long the connection may stay silent before either side considers it
/// lost. Service Bus answers with heartbeats.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// How long before a token expires it is renewed.
const RENEW_BEFORE_EXPIRY: Duration = Duration::from_secs(10 * 60);
/// Delay before a failed token renewal is attempted again.
const RENEW_RETRY: Duration = Duration::from_secs(10);
/// Longest time between checks for tokens due for renewal.
const RENEW_POLL: Duration = Duration::from_secs(60);

/// One AMQP connection with one session that the links of a handle share.
/// Entities are authorized through claims-based security as links need them,
/// and a background task renews their tokens.
pub(super) struct Connection {
    settings: Settings,
    connection: ConnectionHandle<()>,
    session: Mutex<SessionHandle<()>>,
    authorizations: Arc<Mutex<Authorizations>>,
    /// Set when authorizing failed, so the handle opens a new connection.
    broken: AtomicBool,
    renewal: Option<JoinHandle<()>>,
}

/// The claims-based security link and the audiences put through it, with the
/// expiry of each audience's token.
struct Authorizations {
    cbs: CbsClient,
    expiries: HashMap<String, SystemTime>,
}

impl Connection {
    /// Connects to the namespace. Settings must have been validated.
    pub(super) async fn open(settings: &Settings) -> Result<Self, Error> {
        timeout(OPEN_TIMEOUT, Self::open_now(settings))
            .await
            .context("connecting to Service Bus timed out")?
    }

    async fn open_now(settings: &Settings) -> Result<Self, Error> {
        let mut connection = connect(settings).await?;
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
        let cbs = CbsClient::attach(&mut session)
            .await
            .context("attaching the Service Bus claims-based security link");
        let cbs = match cbs {
            Ok(cbs) => cbs,
            Err(error) => {
                let _ = session.close().await;
                let _ = connection.close().await;
                return Err(error);
            }
        };
        let authorizations = Arc::new(Mutex::new(Authorizations {
            cbs,
            expiries: HashMap::new(),
        }));
        let renewal = settings
            .credential()?
            .renewable()
            .then(|| tokio::spawn(renew(Arc::downgrade(&authorizations), settings.clone())));
        Ok(Self {
            settings: settings.clone(),
            connection,
            session: Mutex::new(session),
            authorizations,
            broken: AtomicBool::new(false),
            renewal,
        })
    }

    /// Whether the connection or its session has stopped, or authorizing on
    /// it failed.
    pub(super) async fn is_lost(&self) -> bool {
        self.broken.load(Ordering::Acquire)
            || self.connection.is_closed()
            || self.session.lock().await.is_ended()
    }

    /// Authorizes `entities`, such as `orders` and `orders/$management`, unless
    /// they hold a token that has not expired yet.
    pub(super) async fn authorize(&self, entities: &[String]) -> Result<(), Error> {
        let authorized = timeout(OPEN_TIMEOUT, async {
            let mut authorizations = self.authorizations.lock().await;
            let now = SystemTime::now();
            for entity in entities {
                let audience = format!("sb://{}/{entity}", self.settings.host());
                if authorizations
                    .expiries
                    .get(&audience)
                    .is_some_and(|expires| *expires > now)
                {
                    continue;
                }
                let expires = put_token(&mut authorizations.cbs, &self.settings, &audience).await?;
                authorizations.expiries.insert(audience, expires);
            }
            Ok::<_, Error>(())
        })
        .await
        .context("authorizing with Service Bus timed out")
        .and_then(|authorized| authorized);
        if authorized.is_err() {
            self.broken.store(true, Ordering::Release);
        }
        authorized
    }

    /// The session, locked for attaching a link.
    pub(super) async fn session(&self) -> MutexGuard<'_, SessionHandle<()>> {
        self.session.lock().await
    }
}

/// Dropping the handles ends the session and closes the connection.
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
async fn connect(config: &Settings) -> Result<ConnectionHandle<()>, Error> {
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

/// Authorizes `audience` and returns when its token expires.
async fn put_token(
    cbs: &mut CbsClient,
    settings: &Settings,
    audience: &str,
) -> Result<SystemTime, Error> {
    let token = settings.credential()?.token(audience, SystemTime::now())?;
    let expires = token
        .expires
        .duration_since(UNIX_EPOCH)
        .context("the clock is before the Unix epoch")?
        .as_millis() as i64;
    cbs.put_token(
        audience,
        CbsToken::new(
            token.value,
            SAS_TOKEN_TYPE,
            Timestamp::from_milliseconds(expires),
        ),
    )
    .await
    .with_context(|| format!("authorizing {audience} with Service Bus"))?;
    Ok(token.expires)
}

/// Puts new tokens before the current ones expire, until the connection is
/// dropped. A failed renewal is retried until the token expires, when Service
/// Bus detaches the entity's links and their source or sink reconnects.
async fn renew(authorizations: Weak<Mutex<Authorizations>>, settings: Settings) {
    loop {
        let Some(shared) = authorizations.upgrade() else {
            return;
        };
        let mut retry = false;
        let next = {
            let mut authorizations = shared.lock().await;
            let Authorizations { cbs, expiries } = &mut *authorizations;
            let now = SystemTime::now();
            for (audience, expires) in expiries.iter_mut() {
                if due(*expires, now) > Duration::ZERO {
                    continue;
                }
                match put_token(cbs, &settings, audience).await {
                    Ok(renewed) => *expires = renewed,
                    Err(error) => {
                        retry = true;
                        warn!(error = %format!("{error:#}"), "renewing a Service Bus token failed")
                    }
                }
            }
            let now = SystemTime::now();
            expiries
                .values()
                .map(|expires| due(*expires, now))
                .min()
                .unwrap_or(RENEW_POLL)
        };
        drop(shared);
        let wait = if retry { RENEW_RETRY } else { next };
        tokio::time::sleep(wait.min(RENEW_POLL)).await;
    }
}

/// How long until a token expiring at `expires` is due for renewal.
fn due(expires: SystemTime, now: SystemTime) -> Duration {
    expires
        .checked_sub(RENEW_BEFORE_EXPIRY)
        .and_then(|due| due.duration_since(now).ok())
        .unwrap_or(Duration::ZERO)
}
