use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

/// A request received by [`Endpoint`].
#[derive(Clone, Debug)]
pub(crate) struct Received {
    pub(crate) method: String,
    pub(crate) target: String,
    /// Lowercase names in received order.
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

/// An HTTP/1.1 endpoint that records requests and answers them with the given
/// statuses in order, then with `fallback`.
pub(crate) struct Endpoint {
    pub(crate) addr: SocketAddr,
    pub(crate) received: Arc<Mutex<Vec<Received>>>,
    server: JoinHandle<()>,
}

impl Endpoint {
    pub(crate) async fn start(statuses: impl IntoIterator<Item = u16>, fallback: u16) -> Self {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let received = Arc::new(Mutex::new(Vec::new()));
        let statuses = Arc::new(Mutex::new(statuses.into_iter().collect::<VecDeque<_>>()));
        let recorded = received.clone();
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::spawn(serve(stream, recorded.clone(), statuses.clone(), fallback));
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

    pub(crate) fn requests(&self) -> Vec<Received> {
        self.received.lock().unwrap().clone()
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Serves the requests of one keep-alive connection.
async fn serve(
    stream: TcpStream,
    received: Arc<Mutex<Vec<Received>>>,
    statuses: Arc<Mutex<VecDeque<u16>>>,
    fallback: u16,
) {
    let mut stream = BufReader::new(stream);
    loop {
        let mut line = String::new();
        if stream.read_line(&mut line).await.unwrap_or(0) == 0 {
            return;
        }
        let mut parts = line.split_whitespace();
        let method = parts.next().unwrap().to_owned();
        let target = parts.next().unwrap().to_owned();
        let mut headers = Vec::new();
        loop {
            let mut line = String::new();
            stream.read_line(&mut line).await.unwrap();
            let line = line.trim_end();
            if line.is_empty() {
                break;
            }
            let (name, value) = line.split_once(':').unwrap();
            headers.push((name.to_ascii_lowercase(), value.trim().to_owned()));
        }
        let length = headers
            .iter()
            .find(|(name, _)| name == "content-length")
            .map_or(0, |(_, value)| value.parse().unwrap());
        let mut body = vec![0; length];
        stream.read_exact(&mut body).await.unwrap();
        received.lock().unwrap().push(Received {
            method,
            target,
            headers,
            body,
        });
        let status = statuses.lock().unwrap().pop_front().unwrap_or(fallback);
        let reply = format!("status {status}");
        let response = format!(
            "HTTP/1.1 {status} Test\r\ncontent-length: {}\r\n\r\n{reply}",
            reply.len()
        );
        if stream.write_all(response.as_bytes()).await.is_err() {
            return;
        }
    }
}
