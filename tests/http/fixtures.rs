use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{
    Request, Response, StatusCode, body::Incoming, server::conn::http1, service::service_fn,
};
use hyper_util::rt::TokioIo;
use metrics_util::{
    CompositeKey,
    debugging::{DebuggingRecorder, Snapshotter},
};
use std::{
    collections::VecDeque,
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tokio::{net::TcpListener, task::JoinHandle};

pub(crate) fn snapshotter() -> &'static Snapshotter {
    static SNAPSHOTTER: OnceLock<Snapshotter> = OnceLock::new();
    SNAPSHOTTER.get_or_init(|| {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().unwrap();
        snapshotter
    })
}

pub(crate) fn labels(key: &CompositeKey) -> Vec<(String, String)> {
    key.key()
        .labels()
        .map(|label| (label.key().to_owned(), label.value().to_owned()))
        .collect()
}

/// A request received by [`Destination`].
#[derive(Clone, Debug)]
pub(crate) struct Received {
    pub(crate) method: String,
    pub(crate) target: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

impl Received {
    pub(crate) fn header(&self, name: &str) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|(header, _)| header == name)
            .map(|(_, value)| value.as_str())
            .collect()
    }
}

/// A scripted answer: status, body, and a delay before responding.
pub(crate) type Reply = (u16, &'static str, Duration);

pub(crate) fn reply(status: u16) -> Reply {
    (status, "", Duration::ZERO)
}

/// An HTTP server that answers with scripted replies, then `200 OK`.
pub(crate) struct Destination {
    pub(crate) addr: SocketAddr,
    received: Arc<Mutex<Vec<Received>>>,
    server: JoinHandle<()>,
}

impl Destination {
    pub(crate) async fn start(replies: impl IntoIterator<Item = Reply>) -> Self {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let replies = Arc::new(Mutex::new(replies.into_iter().collect::<VecDeque<_>>()));
        let received = Arc::new(Mutex::new(Vec::new()));
        let server = tokio::spawn({
            let received = received.clone();
            async move {
                loop {
                    let (stream, _) = listener.accept().await.unwrap();
                    let replies = replies.clone();
                    let received = received.clone();
                    let service = service_fn(move |request| {
                        answer(request, replies.clone(), received.clone())
                    });
                    tokio::spawn(
                        http1::Builder::new().serve_connection(TokioIo::new(stream), service),
                    );
                }
            }
        });
        Self {
            addr,
            received,
            server,
        }
    }

    pub(crate) fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    pub(crate) fn received(&self) -> Vec<Received> {
        self.received.lock().unwrap().clone()
    }
}

impl Drop for Destination {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn answer(
    request: Request<Incoming>,
    replies: Arc<Mutex<VecDeque<Reply>>>,
    received: Arc<Mutex<Vec<Received>>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let (parts, body) = request.into_parts();
    let body = body.collect().await.unwrap().to_bytes().to_vec();
    received.lock().unwrap().push(Received {
        method: parts.method.to_string(),
        target: parts.uri.to_string(),
        headers: parts
            .headers
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_owned()))
            .collect(),
        body,
    });
    let next = replies.lock().unwrap().pop_front();
    let (status, body, delay) = next.unwrap_or((200, "", Duration::ZERO));
    tokio::time::sleep(delay).await;
    let mut response = Response::new(Full::from(body));
    *response.status_mut() = StatusCode::from_u16(status).unwrap();
    Ok(response)
}
