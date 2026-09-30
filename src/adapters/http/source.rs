use super::{
    config::{HttpSourceConfig, ResponseTiming},
    record::HttpRecord,
    server::{Intake, Pending, serve},
};
use crate::{
    codec::Decoder,
    message::SourceMessage,
    shutdown::CancellationToken,
    source::{Receive, ReceiveError, Source},
};
use hyper::StatusCode;
use std::{
    mem,
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
/// The server starts on the first receive and decodes each body before it
/// becomes a delivery; a body that fails to decode is answered with
/// `400 Bad Request` and the codec error. With [`ResponseTiming::Ack`], each
/// request is answered only when its delivery completes: `200 OK` on
/// ACK, and `503 Service Unavailable` when the delivery is dropped without ACK.
/// With [`ResponseTiming::Receive`], it is answered `202 Accepted` on receive.
pub struct HttpSource<C, T> {
    config: HttpSourceConfig,
    codec: Arc<C>,
    local_addr: SocketAddr,
    state: State<T>,
    shutdown: CancellationToken,
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
        })
    }

    /// The bound address, including the port chosen for port 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl<C, T> HttpSource<C, T>
where
    C: Decoder<T>,
    T: Send + 'static,
{
    fn start(&mut self) -> Result<(), ReceiveError> {
        let state = mem::replace(&mut self.state, State::Closed);
        let State::Bound(listener) = state else {
            self.state = state;
            return Ok(());
        };
        let listener =
            TcpListener::from_std(listener).map_err(|error| ReceiveError::Fatal(error.into()))?;
        let (sender, requests) = mpsc::channel(1);
        let intake = Intake {
            requests: sender,
            codec: self.codec.clone(),
            max_body_bytes: self.config.max_body_bytes,
        };
        let server = tokio::spawn(serve(listener, intake, self.shutdown.clone()));
        self.state = State::Serving { requests, server };
        Ok(())
    }
}

impl<C, T> Source for HttpSource<C, T>
where
    C: Decoder<T>,
    T: Clone + Send + Sync + 'static,
{
    type Message = HttpMessage<T>;

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
    fn decode(&self) -> anyhow::Result<HttpRecord<T>> {
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
    async fn ack(self) -> anyhow::Result<()> {
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
