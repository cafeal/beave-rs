//! The HTTP client and connection pool that the sinks of a handle share.
use super::{
    config::{DEFAULT_REQUEST_TIMEOUT, SinkConfig, headers, validate_timeout},
    sink::HttpSink,
};
use crate::error::{BoxError, Context, Error};
use bytes::Bytes;
use http_body_util::Full;
use hyper::HeaderMap;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use rustls::{ClientConfig, RootCertStore, crypto::ring};
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

pub(super) type Pool = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

/// An HTTP client that the sinks built from it share.
///
/// The sinks of one handle send their requests through one connection pool,
/// which keeps idle connections to each destination for reuse. The pool is
/// created on the first request; `https` requests verify servers against the
/// platform's root certificates, which are loaded only when a sink uses an
/// `https` URL. Idle connections close once this handle and every sink built
/// from it have been dropped.
///
/// Each builder method returns a handle with a pool of its own, so configure
/// the handle before building sinks from it. `Debug` output shows header names
/// only.
///
/// ```no_run
/// use beavers::{Json, adapters::http::{HttpClient, HttpMethod}};
///
/// let http = HttpClient::new().header("authorization", "Bearer token");
/// let orders = http.sink::<Json, String>("https://billing.example.com/orders");
/// let refunds = http
///     .sink::<Json, String>("https://billing.example.com/refunds")
///     .method(HttpMethod::Put);
/// ```
#[derive(Clone)]
pub struct HttpClient {
    shared: Arc<Shared>,
}

#[derive(Clone)]
struct Settings {
    headers: Vec<(String, String)>,
    timeout: Duration,
}

struct Shared {
    settings: Settings,
    plain: Mutex<Option<Pool>>,
    tls: Mutex<Option<Pool>>,
}

impl HttpClient {
    /// A client without default headers and with a 30 second timeout.
    pub fn new() -> Self {
        Self::from_settings(Settings {
            headers: Vec::new(),
            timeout: DEFAULT_REQUEST_TIMEOUT,
        })
    }

    /// Adds a header sent with every request of every sink, such as
    /// `authorization`. A sink's own header of the same name replaces it.
    pub fn header(self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.configure(|settings| settings.headers.push((name.into(), value.into())))
    }

    /// Maximum time for one attempt, from connecting until the response body
    /// is read, for sinks that do not set their own. An attempt that times out
    /// is retried. Defaults to 30 seconds.
    pub fn timeout(self, timeout: Duration) -> Self {
        self.configure(|settings| settings.timeout = timeout)
    }

    /// A sink that sends every output to `url`, an absolute `http` or `https`
    /// URL, and encodes with the codec's default value. Credentials belong in a
    /// header, not in the URL.
    ///
    /// Defaults: [`HttpMethod::Post`](super::HttpMethod::Post), the handle's
    /// headers, and the handle's timeout.
    pub fn sink<C: Default, T>(&self, url: impl Into<String>) -> HttpSink<C, T> {
        self.sink_with_codec(C::default(), url)
    }

    /// A sink like [`sink`](Self::sink) that encodes with `codec`.
    pub fn sink_with_codec<C, T>(&self, codec: C, url: impl Into<String>) -> HttpSink<C, T> {
        HttpSink::new(self.clone(), SinkConfig::new(url.into()), codec)
    }

    /// Checks the settings without connecting.
    ///
    /// Fails for an invalid header name or value, or a zero timeout.
    pub fn validate(&self) -> Result<(), Error> {
        self.headers()?;
        validate_timeout(self.shared.settings.timeout)
    }

    /// The headers sent with every request.
    pub(super) fn headers(&self) -> Result<HeaderMap, Error> {
        headers(&self.shared.settings.headers)
    }

    pub(super) fn default_timeout(&self) -> Duration {
        self.shared.settings.timeout
    }

    /// The pool for `https` or plain `http` destinations, created on first
    /// use.
    pub(super) fn pool(&self, https: bool) -> Result<Pool, BoxError> {
        let slot = if https {
            &self.shared.tls
        } else {
            &self.shared.plain
        };
        let mut slot = slot.lock().unwrap();
        if let Some(pool) = &*slot {
            return Ok(pool.clone());
        }
        let pool = Client::builder(TokioExecutor::new()).build(connector(https)?);
        *slot = Some(pool.clone());
        Ok(pool)
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
                plain: Mutex::new(None),
                tls: Mutex::new(None),
            }),
        }
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Shows header names only, because their values can hold credentials.
impl fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let settings = &self.shared.settings;
        let headers: Vec<_> = settings.headers.iter().map(|(name, _)| name).collect();
        f.debug_struct("HttpClient")
            .field("headers", &headers)
            .field("timeout", &settings.timeout)
            .finish_non_exhaustive()
    }
}

/// Verifies `https` servers against the platform's root certificates, which
/// are loaded only for the `https` pool.
fn connector(https: bool) -> Result<HttpsConnector<HttpConnector>, BoxError> {
    let provider = Arc::new(ring::default_provider());
    let builder = HttpsConnectorBuilder::new();
    let builder = if https {
        builder
            .with_provider_and_native_roots(provider)
            .context("failed to load the platform's root certificates")?
    } else {
        builder.with_tls_config(
            ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()?
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth(),
        )
    };
    Ok(builder.https_or_http().enable_http1().build())
}
