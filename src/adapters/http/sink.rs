use super::{
    config::{HttpSinkConfig, header},
    record::HttpPublish,
};
use crate::{codec::Encoder, sink::Sink};
use anyhow::Context as _;
use bytes::Bytes;
use reqwest::{Client, Method, Url, header::HeaderMap, redirect::Policy};
use std::{
    marker::PhantomData,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

/// Longest part of an error response body quoted in a publish error.
const MAX_ERROR_BODY_CHARS: usize = 512;

/// A request ready to send. Preparing an output encodes its body and resolves
/// its URL and headers once, so a retry sends the same request.
#[derive(Clone, Debug)]
pub struct HttpPrepared {
    url: Url,
    headers: HeaderMap,
    body: Bytes,
}

impl HttpPrepared {
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// The configured headers followed by the output's own.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

/// The configuration, parsed once.
struct Endpoint {
    config: HttpSinkConfig,
    url: Url,
    method: Method,
    headers: HeaderMap,
}

struct State {
    client: Mutex<Option<Client>>,
    closed: AtomicBool,
}

/// Sends each output as an HTTP request and succeeds when the endpoint answers
/// with a `2xx` status.
///
/// Every other status, including a redirect, a connection failure, and a
/// timeout fail the publication, which the subscription retries under its
/// `publish_retry` policy. A request whose response was lost may have been processed, so the
/// endpoint can receive an output more than once.
pub struct HttpSink<C, T> {
    endpoint: Arc<Endpoint>,
    codec: Arc<C>,
    state: Arc<State>,
    marker: PhantomData<fn(T)>,
}

impl<C, T> Clone for HttpSink<C, T> {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            codec: self.codec.clone(),
            state: self.state.clone(),
            marker: PhantomData,
        }
    }
}

impl<C: Default, T> HttpSink<C, T> {
    pub fn new(config: HttpSinkConfig) -> anyhow::Result<Self> {
        Self::with_codec(config, C::default())
    }
}

impl<C, T> HttpSink<C, T> {
    /// Validates the configuration. The client connects on the first publish.
    pub fn with_codec(config: HttpSinkConfig, codec: C) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            endpoint: Arc::new(Endpoint {
                url: config.endpoint()?,
                method: config.request_method()?,
                headers: config.default_headers()?,
                config,
            }),
            codec: Arc::new(codec),
            state: Arc::new(State {
                client: Mutex::new(None),
                closed: AtomicBool::new(false),
            }),
            marker: PhantomData,
        })
    }

    fn client(&self) -> anyhow::Result<Client> {
        let mut client = self.state.client.lock().unwrap();
        if let Some(client) = &*client {
            return Ok(client.clone());
        }
        let created = connect(&self.endpoint.config)?;
        *client = Some(created.clone());
        Ok(created)
    }
}

impl<C, T> Sink<HttpPublish<T>> for HttpSink<C, T>
where
    C: Encoder<T>,
    T: Send + Sync + 'static,
{
    type Prepared = HttpPrepared;

    fn prepare(&self, output: HttpPublish<T>) -> anyhow::Result<Self::Prepared> {
        let mut url = self.endpoint.url.clone();
        if let Some(path) = &output.path {
            let (path, query) = match path.split_once('?') {
                Some((path, query)) => (path, Some(query)),
                None => (path.as_str(), None),
            };
            anyhow::ensure!(
                path.starts_with('/'),
                "HTTP publish path {path:?} must start with '/'"
            );
            url.set_path(path);
            url.set_query(query);
        }
        let mut headers = self.endpoint.headers.clone();
        for (name, value) in &output.headers {
            let (name, value) = header(name, value)?;
            headers.append(name, value);
        }
        let body = self.codec.encode(&output.body)?.into();
        Ok(HttpPrepared { url, headers, body })
    }

    async fn publish(&self, output: &Self::Prepared) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.state.closed.load(Ordering::Acquire),
            "HTTP sink is closed"
        );
        let response = self
            .client()?
            .request(self.endpoint.method.clone(), output.url.clone())
            .headers(output.headers.clone())
            .body(output.body.clone())
            .send()
            .await
            .with_context(|| format!("HTTP request to {} failed", output.url))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let body = response.text().await.unwrap_or_default();
        let body: String = body.chars().take(MAX_ERROR_BODY_CHARS).collect();
        anyhow::bail!("HTTP endpoint {} answered {status}: {body}", output.url)
    }

    async fn close(&self) -> anyhow::Result<()> {
        self.state.closed.store(true, Ordering::Release);
        self.state.client.lock().unwrap().take();
        Ok(())
    }
}

/// Builds an HTTP/1.1 client that verifies server certificates with the
/// platform's trust store.
fn connect(config: &HttpSinkConfig) -> anyhow::Result<Client> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = rustls_platform_verifier::Verifier::new(provider.clone())
        .context("failed to load the platform's TLS trust store")?;
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"http/1.1".to_vec()];
    Client::builder()
        .tls_backend_preconfigured(tls)
        .redirect(Policy::none())
        .timeout(config.request_timeout)
        .connect_timeout(config.connect_timeout)
        .build()
        .context("failed to build the HTTP client")
}
