use super::{config::HttpSinkConfig, metrics::ClientMetrics, record::HttpPublish};
use crate::{
    codec::Encoder,
    error::{BoxError, Context, Error, ensure},
    sink::{PublishRejected, Sink},
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{
    HeaderMap, Method, Request, StatusCode, Uri,
    body::Incoming,
    header::{HeaderName, HeaderValue},
};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use rustls::{ClientConfig, RootCertStore, crypto::ring};
use std::{
    collections::HashSet,
    marker::PhantomData,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

/// Bytes of an unsuccessful response body kept for the error message.
const ERROR_BODY_EXCERPT: usize = 512;

type HttpClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

/// An encoded request body with its complete, validated header set.
/// Preparing an output performs all codec work once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpPrepared {
    headers: HeaderMap,
    body: Bytes,
}

impl HttpPrepared {
    /// Encoded request body.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// Request headers with lowercase names, configured ones included.
    pub fn headers(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes()))
    }
}

/// The validated request target shared by every output.
struct Target {
    uri: Uri,
    method: Method,
    headers: HeaderMap,
    /// `METHOD scheme://host/path` for error messages, without the query.
    display: String,
}

struct State {
    client: Mutex<Option<HttpClient>>,
    closed: AtomicBool,
}

/// Sends each output as one HTTP request and succeeds when the destination
/// answers with a `2xx` status.
///
/// Failed connections, timeouts, `408 Request Timeout`,
/// `429 Too Many Requests`, and `5xx` responses are retried under the
/// subscription's `publish_retry` policy. Every other response, including
/// redirects, is a [`PublishRejected`] error that the error policy routes as
/// [`FailureKind::PublishRejected`](crate::FailureKind::PublishRejected).
pub struct HttpSink<C, T> {
    config: HttpSinkConfig,
    codec: Arc<C>,
    target: Arc<Target>,
    metrics: Arc<ClientMetrics>,
    state: Arc<State>,
    marker: PhantomData<fn(T)>,
}

impl<C, T> Clone for HttpSink<C, T> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            codec: self.codec.clone(),
            target: self.target.clone(),
            metrics: self.metrics.clone(),
            state: self.state.clone(),
            marker: PhantomData,
        }
    }
}

impl<C: Default, T> HttpSink<C, T> {
    /// Creates a sink with the default codec; see [`HttpSink::with_codec`].
    pub fn new(config: HttpSinkConfig) -> Result<Self, Error> {
        Self::with_codec(config, C::default())
    }
}

impl<C, T> HttpSink<C, T> {
    /// Validates the configuration. The client connects on the first publish.
    pub fn with_codec(config: HttpSinkConfig, codec: C) -> Result<Self, Error> {
        config.validate()?;
        let uri = config.uri()?;
        let mut headers = HeaderMap::new();
        for (name, value) in &config.headers {
            headers.append(
                HeaderName::from_bytes(name.as_bytes()).expect("validated header name"),
                HeaderValue::from_str(value).expect("validated header value"),
            );
        }
        let location = format!(
            "{}://{}{}",
            uri.scheme_str().unwrap_or_default(),
            uri.authority().map(|a| a.as_str()).unwrap_or_default(),
            uri.path()
        );
        let method = config.method.as_hyper();
        Ok(Self {
            metrics: Arc::new(ClientMetrics::new(&location)),
            target: Arc::new(Target {
                display: format!("{method} {location}"),
                uri,
                method,
                headers,
            }),
            codec: Arc::new(codec),
            state: Arc::new(State {
                client: Mutex::new(None),
                closed: AtomicBool::new(false),
            }),
            config,
            marker: PhantomData,
        })
    }

    fn client(&self) -> Result<HttpClient, BoxError> {
        ensure!(
            !self.state.closed.load(Ordering::Acquire),
            Error::closed,
            "HTTP sink is closed"
        );
        let mut slot = self.state.client.lock().unwrap();
        if slot.is_none() {
            let connector = connector(self.target.uri.scheme_str() == Some("https"))?;
            *slot = Some(Client::builder(TokioExecutor::new()).build(connector));
        }
        Ok(slot.as_ref().unwrap().clone())
    }

    async fn send(&self, client: HttpClient, output: &HttpPrepared) -> Result<(), BoxError> {
        let mut request = Request::new(Full::new(output.body.clone()));
        *request.method_mut() = self.target.method.clone();
        *request.uri_mut() = self.target.uri.clone();
        *request.headers_mut() = output.headers.clone();
        let started = Instant::now();
        let exchange = async {
            let response = client.request(request).await?;
            let (parts, body) = response.into_parts();
            let keep = if parts.status.is_success() {
                0
            } else {
                ERROR_BODY_EXCERPT
            };
            Ok::<_, BoxError>((parts.status, read_body(body, keep).await))
        };
        let (status, body) = match tokio::time::timeout(self.config.timeout, exchange).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                self.metrics.attempt("error", started);
                return Err(
                    Error::wrap(format!("HTTP {} failed", self.target.display), error).into(),
                );
            }
            Err(_) => {
                self.metrics.attempt("timeout", started);
                return Err(Error::msg(format!(
                    "HTTP {} timed out after {:?}",
                    self.target.display, self.config.timeout
                ))
                .into());
            }
        };
        self.metrics.attempt(status.as_str(), started);
        if status.is_success() {
            return Ok(());
        }
        let excerpt = String::from_utf8_lossy(&body);
        let excerpt = excerpt.trim();
        let error = if excerpt.is_empty() {
            Error::msg(format!("HTTP {} answered {status}", self.target.display))
        } else {
            Error::msg(format!(
                "HTTP {} answered {status}: {excerpt}",
                self.target.display
            ))
        };
        if retryable(status) {
            Err(error.into())
        } else {
            Err(PublishRejected::wrap(error))
        }
    }
}

impl<C, T> Sink<HttpPublish<T>> for HttpSink<C, T>
where
    C: Encoder<T>,
    T: Send + Sync + 'static,
{
    type Prepared = HttpPrepared;

    fn prepare(&self, record: HttpPublish<T>) -> Result<Self::Prepared, BoxError> {
        let body = Bytes::from(self.codec.encode(&record.body)?);
        let mut headers = self.target.headers.clone();
        let mut replaced = HashSet::new();
        for (name, value) in record.headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| Error::invalid_record(format!("invalid HTTP header name {name:?}")))?;
            let value = HeaderValue::from_bytes(&value).map_err(|_| {
                Error::invalid_record(format!("invalid value for HTTP header {name:?}"))
            })?;
            if replaced.insert(name.clone()) {
                headers.remove(&name);
            }
            headers.append(name, value);
        }
        Ok(HttpPrepared { headers, body })
    }

    /// Succeeds once the destination answers with a `2xx` status. Dropping the
    /// future abandons the request; the destination may still have processed
    /// it, so a retried output can arrive twice.
    async fn publish(&self, output: &Self::Prepared) -> Result<(), BoxError> {
        let client = self.client()?;
        self.send(client, output).await
    }

    /// Refuses further publications. Idle connections close once no request
    /// in progress holds the client.
    async fn close(&self) -> Result<(), BoxError> {
        self.state.closed.store(true, Ordering::Release);
        self.state.client.lock().unwrap().take();
        Ok(())
    }
}

/// Statuses that can succeed when the same request is sent again.
fn retryable(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
}

/// Reads a response body to its end so the connection can be reused, keeping
/// at most `keep` bytes. A body that fails midway ends the read early.
async fn read_body(mut body: Incoming, keep: usize) -> Vec<u8> {
    let mut kept = Vec::new();
    while let Some(Ok(frame)) = body.frame().await {
        if let Ok(data) = frame.into_data() {
            let take = keep.saturating_sub(kept.len()).min(data.len());
            kept.extend_from_slice(&data[..take]);
        }
    }
    kept
}

/// Verifies `https` servers against the platform's root certificates, which
/// are loaded only when the URL uses `https`.
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
