use super::{
    config::HttpSourceConfig,
    record::HttpRecord,
    server::{Pending, serve},
};
use crate::{
    codec::Decoder,
    message::SourceMessage,
    shutdown::CancellationToken,
    source::{Receive, ReceiveError, Source},
};
use hyper::StatusCode;
use std::{
    marker::PhantomData,
    mem,
    net::{self, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};

enum State {
    Bound(net::TcpListener),
    Serving {
        requests: mpsc::Receiver<Pending>,
        server: JoinHandle<()>,
    },
    Closed,
}

/// An HTTP/1.1 server whose `POST` requests become deliveries.
///
/// The listener is bound on construction, so address errors surface immediately
/// and [`local_addr`](Self::local_addr) is known before the subscription runs.
/// The server starts on the first receive. Each request is answered only when
/// its delivery completes: `204 No Content` on ACK, and
/// `503 Service Unavailable` when the delivery is dropped without ACK.
pub struct HttpSource<C, T> {
    config: HttpSourceConfig,
    codec: Arc<C>,
    local_addr: SocketAddr,
    state: State,
    shutdown: CancellationToken,
    marker: PhantomData<T>,
}

impl<C: Default, T> HttpSource<C, T> {
    pub fn new(config: HttpSourceConfig) -> anyhow::Result<Self> {
        Self::with_codec(config, C::default())
    }
}

impl<C, T> HttpSource<C, T> {
    pub fn with_codec(config: HttpSourceConfig, codec: C) -> anyhow::Result<Self> {
        config.validate()?;
        let listener = net::TcpListener::bind(config.bind)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            local_addr: listener.local_addr()?,
            config,
            codec: Arc::new(codec),
            state: State::Bound(listener),
            shutdown: CancellationToken::new(),
            marker: PhantomData,
        })
    }

    /// The bound address, including the port chosen for port 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    fn start(&mut self) -> Result<(), ReceiveError> {
        let state = mem::replace(&mut self.state, State::Closed);
        let State::Bound(listener) = state else {
            self.state = state;
            return Ok(());
        };
        let listener =
            TcpListener::from_std(listener).map_err(|error| ReceiveError::Fatal(error.into()))?;
        let (sender, requests) = mpsc::channel(1);
        let server = tokio::spawn(serve(
            listener,
            sender,
            self.config.max_body_bytes,
            self.shutdown.clone(),
        ));
        self.state = State::Serving { requests, server };
        Ok(())
    }
}

impl<C, T> Source for HttpSource<C, T>
where
    C: Decoder<T>,
    T: Clone + Send + Sync + 'static,
{
    type Message = HttpMessage<C, T>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        self.start()?;
        let State::Serving { requests, .. } = &mut self.state else {
            return Ok(Receive::End);
        };
        Ok(match requests.recv().await {
            Some(Pending { record, respond }) => Receive::Message(HttpMessage {
                record,
                respond,
                codec: self.codec.clone(),
                decode_failed: AtomicBool::new(false),
                marker: PhantomData,
            }),
            None => Receive::End,
        })
    }

    /// Stops accepting connections and answers requests not yet received with
    /// `503`, then waits until open connections finish their current request.
    async fn close(&mut self) -> anyhow::Result<()> {
        self.shutdown.cancel();
        if let State::Serving { requests, server } = mem::replace(&mut self.state, State::Closed) {
            drop(requests);
            server.await?;
        }
        Ok(())
    }
}

impl<C, T> Drop for HttpSource<C, T> {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// One request waiting for its response until the delivery is acknowledged
/// or dropped.
pub struct HttpMessage<C, T> {
    record: HttpRecord<Vec<u8>>,
    respond: tokio::sync::oneshot::Sender<StatusCode>,
    codec: Arc<C>,
    decode_failed: AtomicBool,
    marker: PhantomData<T>,
}

impl<C, T> SourceMessage for HttpMessage<C, T>
where
    C: Decoder<T>,
    T: Clone + Send + Sync + 'static,
{
    type Item = HttpRecord<T>;
    /// The request with its undecoded body.
    type Raw = HttpRecord<Vec<u8>>;

    fn decode(&self) -> anyhow::Result<HttpRecord<T>> {
        let body = self
            .codec
            .decode(&self.record.body)
            .inspect_err(|_| self.decode_failed.store(true, Ordering::Relaxed))?;
        Ok(HttpRecord {
            path: self.record.path.clone(),
            query: self.record.query.clone(),
            headers: self.record.headers.clone(),
            body,
            metadata: self.record.metadata.clone(),
        })
    }

    /// Answers `204 No Content`, or `400 Bad Request` when the body failed to
    /// decode and the error policy discarded or dead-lettered it. A client that
    /// disconnected before the response still counts as acknowledged.
    async fn ack(self) -> anyhow::Result<()> {
        let status = if self.decode_failed.load(Ordering::Relaxed) {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::NO_CONTENT
        };
        let _ = self.respond.send(status);
        Ok(())
    }

    fn raw(&self) -> HttpRecord<Vec<u8>> {
        self.record.clone()
    }

    /// Request headers with UTF-8 values, such as `traceparent`.
    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        self.record
            .headers
            .iter()
            .filter_map(|(name, value)| Some((name.as_str(), std::str::from_utf8(value).ok()?)))
            .collect()
    }
}
