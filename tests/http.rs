#![cfg(feature = "http")]

use beavers::{
    App, CancellationToken, DeadLetter, FailureKind, HandlerError, InMemorySink, Json, Receive,
    Source, SourceMessage, Subscription, Utf8,
    adapters::http::{HttpRecord, HttpSource, HttpSourceConfig, ResponseTiming},
};
use serde::Deserialize;
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::JoinHandle,
};

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct Order {
    id: u64,
}

fn config() -> HttpSourceConfig {
    HttpSourceConfig::new(SocketAddr::from(([127, 0, 0, 1], 0)))
}

/// Sends one request over its own connection and returns the response status.
async fn request(
    addr: SocketAddr,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> u16 {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut head = format!(
        "{method} {target} HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\ncontent-length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    response.split(' ').nth(1).unwrap().parse().unwrap()
}

async fn post(addr: SocketAddr, body: &[u8]) -> u16 {
    request(addr, "POST", "/", &[], body).await
}

fn run(app: App, shutdown: &CancellationToken) -> JoinHandle<anyhow::Result<()>> {
    tokio::spawn(app.run_until(shutdown.clone()))
}

#[test]
fn config_rejects_an_empty_body_limit() {
    let mut config = config();
    config.max_body_bytes = 0;
    assert!(config.validate().is_err());
    assert!(HttpSource::<Utf8, String>::new(config).is_err());
}

#[tokio::test]
async fn acknowledged_request_is_answered_with_ok() {
    let source = HttpSource::<Json, Order>::new(config()).unwrap();
    let addr = source.local_addr();
    assert_ne!(addr.port(), 0);
    let sink = InMemorySink::default();
    let shutdown = CancellationToken::new();
    let app = run(
        App::new().subscribe(
            "acknowledged_request_is_answered_with_ok",
            source,
            sink.clone(),
            |record: HttpRecord<Order>| async move {
                let tenant = record.header("X-Tenant").map(<[u8]>::to_vec);
                Ok((record.path, record.query, tenant, record.body))
            },
        ),
        &shutdown,
    );

    let status = request(
        addr,
        "POST",
        "/orders?source=web",
        &[("x-tenant", "acme")],
        br#"{"id":7}"#,
    )
    .await;

    assert_eq!(status, 200);
    assert_eq!(
        sink.values(),
        vec![(
            "/orders".to_owned(),
            Some("source=web".to_owned()),
            Some(b"acme".to_vec()),
            Order { id: 7 },
        )]
    );
    shutdown.cancel();
    app.await.unwrap().unwrap();
}

#[tokio::test]
async fn delivery_dropped_without_ack_is_answered_with_service_unavailable() {
    let source = HttpSource::<Utf8, String>::new(config()).unwrap();
    let addr = source.local_addr();
    let shutdown = CancellationToken::new();
    let app = run(
        App::new().subscribe(
            "delivery_dropped_without_ack_is_answered_with_service_unavailable",
            source,
            InMemorySink::<String>::default(),
            |_: HttpRecord<String>| async move {
                Err::<String, _>(HandlerError::Reject(anyhow::anyhow!("refused")))
            },
        ),
        &shutdown,
    );

    assert_eq!(post(addr, b"order").await, 503);
    // Without a dead-letter sink the rejection stops the subscription.
    assert!(app.await.unwrap().is_err());
}

#[tokio::test]
async fn undecodable_body_is_answered_with_bad_request_without_a_delivery() {
    let source = HttpSource::<Json, Order>::new(config()).unwrap();
    let addr = source.local_addr();
    let sink = InMemorySink::default();
    let shutdown = CancellationToken::new();
    let app = run(
        App::new().subscribe(
            "undecodable_body_is_answered_with_bad_request_without_a_delivery",
            source,
            sink.clone(),
            |record: HttpRecord<Order>| async move { Ok(record.body.id) },
        ),
        &shutdown,
    );

    // The default error policy would stop on a decode failure; the request
    // never reaches it.
    assert_eq!(post(addr, b"not json").await, 400);
    assert_eq!(post(addr, br#"{"id":1}"#).await, 200);
    assert_eq!(sink.values(), vec![1]);
    shutdown.cancel();
    app.await.unwrap().unwrap();
}

#[tokio::test]
async fn dead_letter_keeps_the_raw_request() {
    let source = HttpSource::<Utf8, String>::new(config()).unwrap();
    let addr = source.local_addr();
    let dlq = InMemorySink::default();
    let shutdown = CancellationToken::new();
    let app = run(
        App::new().subscription(
            Subscription::new(
                "dead_letter_keeps_the_raw_request",
                source,
                InMemorySink::<String>::default(),
                |_: HttpRecord<String>| async move {
                    Err::<String, _>(HandlerError::Reject(anyhow::anyhow!("refused")))
                },
            )
            .dlq(dlq.clone()),
        ),
        &shutdown,
    );

    let status = request(addr, "POST", "/events", &[("x-id", "9")], b"payload").await;

    assert_eq!(status, 200);
    let [dead_letter]: [DeadLetter<HttpRecord<String>, HttpRecord<Vec<u8>>>; 1] =
        dlq.values().try_into().unwrap();
    assert_eq!(dead_letter.failure, FailureKind::Rejected);
    assert_eq!(dead_letter.raw.path, "/events");
    assert_eq!(dead_letter.raw.body, b"payload");
    assert_eq!(dead_letter.raw.header("x-id"), Some(&b"9"[..]));
    shutdown.cancel();
    app.await.unwrap().unwrap();
}

#[tokio::test]
async fn invalid_requests_do_not_become_deliveries() {
    let mut config = config();
    config.max_body_bytes = 8;
    let mut source = HttpSource::<Utf8, String>::new(config).unwrap();
    let addr = source.local_addr();
    let client = tokio::spawn(async move {
        (
            request(addr, "GET", "/", &[], b"").await,
            post(addr, b"longer than eight bytes").await,
            post(addr, b"ok").await,
        )
    });

    let Receive::Message(message) = source.receive().await.unwrap() else {
        panic!("expected a delivery");
    };
    assert_eq!(message.decode().unwrap().body, "ok");
    message.ack().await.unwrap();
    assert_eq!(client.await.unwrap(), (405, 413, 200));
    source.close().await.unwrap();
}

#[tokio::test]
async fn received_request_exposes_raw_form_and_trace_headers() {
    let mut source = HttpSource::<Utf8, String>::new(config()).unwrap();
    let addr = source.local_addr();
    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let client = tokio::spawn(async move {
        request(addr, "POST", "/", &[("traceparent", traceparent)], b"hi").await
    });

    let Receive::Message(message) = source.receive().await.unwrap() else {
        panic!("expected a delivery");
    };
    assert!(
        message
            .propagation_fields()
            .contains(&("traceparent", traceparent))
    );
    assert_eq!(message.raw().body, b"hi");
    assert_eq!(message.raw().metadata().remote_addr.ip(), addr.ip());
    drop(message);
    assert_eq!(client.await.unwrap(), 503);
    source.close().await.unwrap();
}

#[tokio::test]
async fn close_answers_queued_requests_and_stops_listening() {
    let mut source = HttpSource::<Utf8, String>::new(config()).unwrap();
    let addr = source.local_addr();
    let first = tokio::spawn(post(addr, b"first"));
    let Receive::Message(message) = source.receive().await.unwrap() else {
        panic!("expected a delivery");
    };
    // The second request waits in the queue, never received.
    let queued = tokio::spawn(post(addr, b"queued"));
    tokio::time::sleep(Duration::from_millis(50)).await;

    let close = tokio::spawn(async move {
        source.close().await.unwrap();
        source
    });
    assert_eq!(queued.await.unwrap(), 503);
    message.ack().await.unwrap();
    assert_eq!(first.await.unwrap(), 200);
    let mut source = close.await.unwrap();

    assert!(TcpStream::connect(addr).await.is_err());
    assert!(matches!(source.receive().await.unwrap(), Receive::End));
    source.close().await.unwrap();
}

#[tokio::test]
async fn receive_timing_answers_accepted_before_processing() {
    let mut config = config();
    config.response = ResponseTiming::Receive;
    let mut source = HttpSource::<Json, Order>::new(config).unwrap();
    let addr = source.local_addr();
    let accepted = tokio::spawn(post(addr, br#"{"id":1}"#));
    let Receive::Message(message) = source.receive().await.unwrap() else {
        panic!("expected a delivery");
    };
    // Answered while the delivery is still unacknowledged.
    assert_eq!(accepted.await.unwrap(), 202);
    drop(message);

    assert_eq!(post(addr, b"not json").await, 400);
    let next = tokio::spawn(post(addr, br#"{"id":2}"#));
    let Receive::Message(message) = source.receive().await.unwrap() else {
        panic!("expected a delivery");
    };
    assert_eq!(message.decode().unwrap().body, Order { id: 2 });
    assert_eq!(next.await.unwrap(), 202);
    message.ack().await.unwrap();
    source.close().await.unwrap();
}
