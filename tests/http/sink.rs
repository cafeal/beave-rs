use super::fixtures::{Destination, labels, reply, snapshotter};
use beavers::{
    App, DeadLetter, FailureKind, InMemorySink, IterSource, Json, PropagationCarrier, RetryPolicy,
    Sink, Subscription, Utf8,
    adapters::http::{HttpMethod, HttpPublish, HttpSink, HttpSinkConfig},
};
use metrics_util::debugging::DebugValue;
use serde::Serialize;
use std::{net::TcpListener, sync::atomic::Ordering, time::Duration};

#[derive(Clone, Debug, Serialize)]
struct Order {
    id: u64,
}

fn fast() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        initial_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
        ..RetryPolicy::default()
    }
}

fn sink(url: String) -> HttpSink<Utf8, String> {
    HttpSink::new(HttpSinkConfig::new(url)).unwrap()
}

#[test]
fn config_rejects_invalid_targets() {
    let invalid = [
        HttpSinkConfig::new("localhost:8080/orders"),
        HttpSinkConfig::new("ftp://localhost/orders"),
        HttpSinkConfig::new("http://user:secret@localhost/orders"),
        HttpSinkConfig {
            headers: vec![("bad name".to_owned(), "x".to_owned())],
            ..HttpSinkConfig::new("http://localhost/orders")
        },
        HttpSinkConfig {
            timeout: Duration::ZERO,
            ..HttpSinkConfig::new("http://localhost/orders")
        },
    ];
    for config in invalid {
        assert!(config.validate().is_err(), "{config:?}");
        assert!(HttpSink::<Utf8, String>::new(config).is_err());
    }
    assert!(
        HttpSinkConfig::new("https://localhost:8443/orders?v=1")
            .validate()
            .is_ok()
    );
}

#[test]
fn prepare_merges_configured_and_output_headers() {
    let config = HttpSinkConfig {
        headers: vec![
            ("content-type".to_owned(), "text/plain".to_owned()),
            ("authorization".to_owned(), "Bearer token".to_owned()),
        ],
        ..HttpSinkConfig::new("http://localhost/orders")
    };
    let sink = HttpSink::<Utf8, String>::new(config).unwrap();
    let prepared = sink
        .prepare(
            HttpPublish::new("hello".to_owned())
                .with_header("Content-Type", "text/markdown")
                .with_header("x-tag", "a")
                .with_header("x-tag", "b"),
        )
        .unwrap();

    assert_eq!(prepared.body(), b"hello");
    let mut headers: Vec<_> = prepared.headers().collect();
    headers.sort();
    assert_eq!(
        headers,
        vec![
            ("authorization", &b"Bearer token"[..]),
            ("content-type", &b"text/markdown"[..]),
            ("x-tag", &b"a"[..]),
            ("x-tag", &b"b"[..]),
        ]
    );
    let invalid = HttpPublish::new(String::new()).with_header("x-bad", b"line\nbreak".to_vec());
    assert!(sink.prepare(invalid).is_err());
}

#[test]
fn propagation_fields_replace_headers_of_the_same_name() {
    let mut record = HttpPublish::new(()).with_header("Traceparent", "old");
    record.set_propagation_field("traceparent", "new".to_owned());
    assert_eq!(
        record.headers,
        vec![("traceparent".to_owned(), b"new".to_vec())]
    );
}

#[tokio::test]
async fn outputs_are_sent_with_configured_method_url_and_headers() {
    let destination = Destination::start([]).await;
    let config = HttpSinkConfig {
        method: HttpMethod::Put,
        headers: vec![("content-type".to_owned(), "application/json".to_owned())],
        ..HttpSinkConfig::new(destination.url("/orders?source=kafka"))
    };
    let sink = HttpSink::<Json, Order>::new(config).unwrap();
    let source = IterSource::new([1, 2]);
    let acks = source.acknowledgements();

    App::new()
        .subscribe(
            "outputs_are_sent_with_configured_method_url_and_headers",
            source,
            sink,
            |id: u64| async move {
                Ok(HttpPublish::new(Order { id }).with_header("idempotency-key", id.to_string()))
            },
        )
        .run()
        .await
        .unwrap();

    let received = destination.received();
    assert_eq!(received.len(), 2);
    for (request, id) in received.iter().zip(1..) {
        assert_eq!(request.method, "PUT");
        assert_eq!(request.target, "/orders?source=kafka");
        assert_eq!(request.header("content-type"), vec!["application/json"]);
        assert_eq!(request.header("idempotency-key"), vec![id.to_string()]);
        assert_eq!(request.body, format!(r#"{{"id":{id}}}"#).into_bytes());
    }
    assert_eq!(acks.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn transient_failures_are_retried_until_accepted() {
    let destination = Destination::start([reply(503), reply(429)]).await;
    let source = IterSource::new(["order".to_owned()]);
    let acks = source.acknowledgements();

    App::new()
        .subscription(
            Subscription::new(
                "transient_failures_are_retried_until_accepted",
                source,
                sink(destination.url("/")),
                |body: String| async move { Ok(HttpPublish::new(body)) },
            )
            .publish_retry(fast()),
        )
        .run()
        .await
        .unwrap();

    assert_eq!(destination.received().len(), 3);
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn timed_out_attempt_is_retried() {
    let destination = Destination::start([(200, "", Duration::from_secs(5))]).await;
    let config = HttpSinkConfig {
        timeout: Duration::from_millis(100),
        ..HttpSinkConfig::new(destination.url("/"))
    };
    let source = IterSource::new(["order".to_owned()]);
    let acks = source.acknowledgements();

    App::new()
        .subscription(
            Subscription::new(
                "timed_out_attempt_is_retried",
                source,
                HttpSink::<Utf8, String>::new(config).unwrap(),
                |body: String| async move { Ok(HttpPublish::new(body)) },
            )
            .publish_retry(fast()),
        )
        .run()
        .await
        .unwrap();

    assert_eq!(destination.received().len(), 2);
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unreachable_destination_stops_without_ack() {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let source = IterSource::new(["order".to_owned()]);
    let acks = source.acknowledgements();

    let error = App::new()
        .subscription(
            Subscription::new(
                "unreachable_destination_stops_without_ack",
                source,
                sink(format!("http://127.0.0.1:{port}/")),
                |body: String| async move { Ok(HttpPublish::new(body)) },
            )
            .publish_retry(fast()),
        )
        .run()
        .await
        .unwrap_err();

    assert!(format!("{error:#}").contains("publish retry exhausted"));
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn rejected_output_is_dead_lettered_without_retry() {
    let destination = Destination::start([(422, "id is required\n", Duration::ZERO)]).await;
    let source = IterSource::new(["bad".to_owned(), "good".to_owned()]);
    let acks = source.acknowledgements();
    let dlq = InMemorySink::default();

    App::new()
        .subscription(
            Subscription::new(
                "rejected_output_is_dead_lettered_without_retry",
                source,
                sink(destination.url("/orders")),
                |body: String| async move { Ok(HttpPublish::new(body)) },
            )
            .publish_retry(fast())
            .dlq(dlq.clone()),
        )
        .run()
        .await
        .unwrap();

    let bodies: Vec<_> = destination.received().into_iter().map(|r| r.body).collect();
    assert_eq!(bodies, vec![b"bad".to_vec(), b"good".to_vec()]);
    let [letter]: [DeadLetter<String, ()>; 1] = dlq.values().try_into().unwrap();
    assert_eq!(letter.failure, FailureKind::PublishRejected);
    assert_eq!(letter.input.as_deref(), Some("bad"));
    let expected = format!(
        "POST http://{}/orders answered 422 Unprocessable Entity: id is required",
        destination.addr
    );
    assert!(letter.error.contains(&expected), "{}", letter.error);
    assert_eq!(acks.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn rejected_output_without_dead_letter_sink_stops_without_ack() {
    let destination = Destination::start([reply(404)]).await;
    let source = IterSource::new(["order".to_owned()]);
    let acks = source.acknowledgements();

    let error = App::new()
        .subscribe(
            "rejected_output_without_dead_letter_sink_stops_without_ack",
            source,
            sink(destination.url("/missing")),
            |body: String| async move { Ok(HttpPublish::new(body)) },
        )
        .run()
        .await
        .unwrap_err();

    assert!(format!("{error:#}").contains("404 Not Found"));
    assert_eq!(destination.received().len(), 1);
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn closed_sink_refuses_publication_in_every_clone() {
    let destination = Destination::start([]).await;
    let sink = sink(destination.url("/"));
    let clone = sink.clone();
    let prepared = sink.prepare(HttpPublish::new("order".to_owned())).unwrap();
    sink.publish(&prepared).await.unwrap();

    sink.close().await.unwrap();
    sink.close().await.unwrap();

    assert!(clone.publish(&prepared).await.is_err());
    assert_eq!(destination.received().len(), 1);
}

#[tokio::test]
async fn client_metrics_count_attempts_by_outcome() {
    let snapshotter = snapshotter();
    let destination = Destination::start([reply(500), reply(400)]).await;
    let sink = sink(destination.url("/metrics?token=secret"));
    let prepared = sink.prepare(HttpPublish::new("order".to_owned())).unwrap();
    assert!(sink.publish(&prepared).await.is_err());
    assert!(sink.publish(&prepared).await.is_err());
    sink.publish(&prepared).await.unwrap();

    let url = destination.url("/metrics");
    let mut attempts = Vec::new();
    let mut durations = 0;
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        let labels = labels(&key);
        if !labels.contains(&("url".to_owned(), url.clone())) {
            continue;
        }
        let outcome = labels
            .iter()
            .find(|(name, _)| name == "outcome")
            .map(|(_, value)| value.clone())
            .unwrap();
        match (key.key().name(), value) {
            ("beavers_http_client_requests_total", DebugValue::Counter(count)) => {
                attempts.push((outcome, count));
            }
            ("beavers_http_client_request_duration_seconds", DebugValue::Histogram(samples)) => {
                durations += samples.len();
            }
            _ => {}
        }
    }
    attempts.sort();
    assert_eq!(
        attempts,
        vec![
            ("200".to_owned(), 1),
            ("400".to_owned(), 1),
            ("500".to_owned(), 1)
        ]
    );
    assert_eq!(durations, 3);
}
