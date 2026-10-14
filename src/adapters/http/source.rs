use super::{
    config::{ResponseTiming, SourceConfig},
    metrics::ServerMetrics,
    record::HttpRecord,
    server::{Intake, Pending, serve},
};
use crate::{
    codec::{Codec, Decoder},
    error::{BoxError, Context, Error},
    message::SourceMessage,
    shutdown::CancellationToken,
    source::{Receive, ReceiveError, Source},
};
use hyper::StatusCode;
use std::{
    io, mem,
    net::{self, SocketAddr},
    sync::Arc,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

enum State<T> {
    Bound(net::TcpListener),
    Serving {
        requests: mpsc::Receiver<Pending<T>>,
        server: JoinHandle<()>,
    },
    Closed,
}

/// An HTTP/1.1 server whose `POST` requests become deliveries.
///
/// The listener is bound on construction, so address errors surface immediately
/// and [`local_addr`](Self::local_addr) is known before the subscription runs.
/// The server starts on the first receive, which also validates the settings,
/// and decodes each body before it
/// becomes a delivery; a body that fails to decode is answered with
/// `400 Bad Request` and the codec error. With [`ResponseTiming::Ack`], each
/// request is answered only when its delivery completes: `200 OK` on
/// ACK, and `503 Service Unavailable` when the delivery is dropped without ACK.
/// With [`ResponseTiming::Receive`], it is answered `202 Accepted` on receive.
pub struct HttpSource<C: Codec> {
    config: SourceConfig,
    codec: Arc<C>,
    local_addr: SocketAddr,
    state: State<C::Value>,
    shutdown: CancellationToken,
}

impl<C: Codec + Default> HttpSource<C> {
    /// Binds the listener to `bind` and decodes bodies with the codec's default
    /// value. Port 0 selects a free port, which
    /// [`local_addr`](Self::local_addr) reports.
    ///
    /// Defaults: a `max_body_bytes` of 1 MiB and [`ResponseTiming::Ack`].
    ///
    /// Fails when the address cannot be bound.
    pub fn new(bind: SocketAddr) -> Result<Self, Error> {
        Self::with_codec(C::default(), bind)
    }
}

impl<C: Codec> HttpSource<C> {
    /// A source like [`new`](Self::new) that decodes bodies with `codec`.
    pub fn with_codec(codec: C, bind: SocketAddr) -> Result<Self, Error> {
        let bind_listener = || -> io::Result<(net::TcpListener, SocketAddr)> {
            let listener = net::TcpListener::bind(bind)?;
            listener.set_nonblocking(true)?;
            let local_addr = listener.local_addr()?;
            Ok((listener, local_addr))
        };
        let (listener, local_addr) =
            bind_listener().with_context(|| format!("binding the HTTP source to {bind}"))?;
        Ok(Self {
            local_addr,
            config: SourceConfig::default(),
            codec: Arc::new(codec),
            state: State::Bound(listener),
            shutdown: CancellationToken::new(),
        })
    }

    /// Larger request bodies are answered with `413 Payload Too Large` without
    /// becoming deliveries. Defaults to 1 MiB.
    pub fn max_body_bytes(mut self, max: usize) -> Self {
        self.config.max_body_bytes = max;
        self
    }

    /// When a received request is answered. Defaults to
    /// [`ResponseTiming::Ack`].
    pub fn response(mut self, response: ResponseTiming) -> Self {
        self.config.response = response;
        self
    }

    /// Checks the settings; fails when `max_body_bytes` is zero.
    pub fn validate(&self) -> Result<(), Error> {
        self.config.validate()
    }

    /// The bound address, including the port chosen for port 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl<C> HttpSource<C>
where
    C: Decoder<Value: Send + 'static>,
{
    fn start(&mut self) -> Result<(), ReceiveError> {
        if !matches!(self.state, State::Bound(_)) {
            return Ok(());
        }
        self.validate()
            .map_err(|error| ReceiveError::Fatal(error.into()))?;
        let State::Bound(listener) = mem::replace(&mut self.state, State::Closed) else {
            unreachable!("checked above");
        };
        let listener =
            TcpListener::from_std(listener).map_err(|error| ReceiveError::Fatal(error.into()))?;
        let (sender, requests) = mpsc::channel(1);
        let intake = Intake {
            requests: sender,
            codec: self.codec.clone(),
            max_body_bytes: self.config.max_body_bytes,
            metrics: Arc::new(ServerMetrics::new(self.local_addr)),
        };
        let server = tokio::spawn(serve(listener, intake, self.shutdown.clone()));
        self.state = State::Serving { requests, server };
        Ok(())
    }
}

impl<C> Source for HttpSource<C>
where
    C: Decoder<Value: Clone + Send + Sync + 'static>,
{
    type Message = HttpMessage<C::Value>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        self.start()?;
        let State::Serving { requests, .. } = &mut self.state else {
            return Ok(Receive::End);
        };
        Ok(match requests.recv().await {
            Some(Pending { raw, body, respond }) => Receive::Message(HttpMessage {
                raw,
                body,
                respond: match self.config.response {
                    ResponseTiming::Ack => Some(respond),
                    ResponseTiming::Receive => {
                        let _ = respond.send(StatusCode::ACCEPTED);
                        None
                    }
                },
            }),
            None => Receive::End,
        })
    }

    /// Stops accepting connections, closes idle ones, and answers requests not
    /// yet received with `503`. Requests already received keep their
    /// connections until their deliveries complete.
    fn stop_receiving(&mut self) {
        self.shutdown.cancel();
        if let State::Serving { requests, .. } = &mut self.state {
            requests.close();
            while requests.try_recv().is_ok() {}
        }
    }

    /// Stops receiving as [`stop_receiving`](Source::stop_receiving) does, then
    /// waits until open connections finish their current request.
    async fn close(&mut self) -> Result<(), BoxError> {
        self.shutdown.cancel();
        if let State::Serving { requests, server } = mem::replace(&mut self.state, State::Closed) {
            drop(requests);
            server.await?;
        }
        Ok(())
    }
}

impl<C: Codec> Drop for HttpSource<C> {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// One decoded request waiting for its response until the delivery is
/// acknowledged or dropped.
pub struct HttpMessage<T> {
    raw: HttpRecord<Vec<u8>>,
    body: T,
    /// `None` once the request was answered on receive.
    respond: Option<oneshot::Sender<StatusCode>>,
}

impl<T: Clone + Send + Sync + 'static> SourceMessage for HttpMessage<T> {
    type Item = HttpRecord<T>;
    /// The request with its undecoded body.
    type Raw = HttpRecord<Vec<u8>>;

    /// The body was decoded when the request arrived, so this never fails.
    fn decode(&self) -> Result<HttpRecord<T>, BoxError> {
        Ok(HttpRecord {
            path: self.raw.path.clone(),
            query: self.raw.query.clone(),
            headers: self.raw.headers.clone(),
            body: self.body.clone(),
            metadata: self.raw.metadata.clone(),
        })
    }

    /// With [`ResponseTiming::Ack`], answers `200 OK`. A client that
    /// disconnected before the response still counts as acknowledged.
    async fn ack(self) -> Result<(), BoxError> {
        if let Some(respond) = self.respond {
            let _ = respond.send(StatusCode::OK);
        }
        Ok(())
    }

    fn raw(&self) -> HttpRecord<Vec<u8>> {
        self.raw.clone()
    }

    /// Request headers with UTF-8 values, such as `traceparent`.
    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        self.raw
            .headers
            .iter()
            .filter_map(|(name, value)| Some((name.as_str(), std::str::from_utf8(value).ok()?)))
            .collect()
    }
}
