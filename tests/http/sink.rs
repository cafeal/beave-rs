use super::fixtures::Endpoint;
use beavers::{
    App, IterSource, Json, RetryPolicy, Sink, Subscription, Utf8,
    adapters::http::{HttpPublish, HttpSink, HttpSinkConfig},
};
use serde::Serialize;
use std::time::Duration;

#[derive(Clone, Serialize)]
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

#[tokio::test]
async fn outputs_are_sent_with_their_path_and_headers() {
    let endpoint = Endpoint::start([], 200).await;
    let sink = HttpSink::<Json, Order>::new(
        HttpSinkConfig::new(endpoint.url("/orders"))
            .header("content-type", "application/json")
            .header("authorization", "Bearer token"),
    )
    .unwrap();
    App::new()
        .subscribe(
            "orders",
            IterSource::new([1, 2]),
            sink,
            |id: u64| async move {
                let publish =
                    HttpPublish::new(Order { id }).header("idempotency-key", id.to_string());
                Ok(if id == 2 {
                    publish.path("/orders/2?notify=1")
                } else {
                    publish
                })
            },
        )
        .run()
        .await
        .unwrap();
    let requests = endpoint.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].target, "/orders");
    assert_eq!(requests[1].target, "/orders/2?notify=1");
    assert_eq!(requests[0].body, br#"{"id":1}"#);
    assert_eq!(requests[0].header("content-type"), ["application/json"]);
    assert_eq!(requests[0].header("authorization"), ["Bearer token"]);
    assert_eq!(requests[1].header("idempotency-key"), ["2"]);
}

#[tokio::test]
async fn the_configured_method_is_used() {
    let endpoint = Endpoint::start([], 204).await;
    let mut config = HttpSinkConfig::new(endpoint.url("/items"));
    config.method = "PUT".to_owned();
    let sink = HttpSink::<Utf8, String>::new(config).unwrap();
    App::new()
        .subscribe(
            "items",
            IterSource::new(["a".to_owned()]),
            sink,
            |value: String| async move { Ok(HttpPublish::new(value)) },
        )
        .run()
        .await
        .unwrap();
    let requests = endpoint.requests();
    assert_eq!(requests[0].method, "PUT");
    assert_eq!(requests[0].body, b"a");
}

#[tokio::test]
async fn unsuccessful_statuses_are_retried() {
    let endpoint = Endpoint::start([503, 500], 200).await;
    let sink = HttpSink::<Utf8, String>::new(HttpSinkConfig::new(endpoint.url("/"))).unwrap();
    App::new()
        .subscription(
            Subscription::new(
                "items",
                IterSource::new(["a".to_owned()]),
                sink,
                |value: String| async move { Ok(HttpPublish::new(value)) },
            )
            .publish_retry(fast()),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(endpoint.requests().len(), 3);
}

#[tokio::test]
async fn exhausted_retries_stop_the_subscription_with_the_status() {
    let endpoint = Endpoint::start([], 400).await;
    let sink = HttpSink::<Utf8, String>::new(HttpSinkConfig::new(endpoint.url("/"))).unwrap();
    let error = App::new()
        .subscription(
            Subscription::new(
                "items",
                IterSource::new(["a".to_owned()]),
                sink,
                |value: String| async move { Ok(HttpPublish::new(value)) },
            )
            .publish_retry(fast()),
        )
        .run()
        .await
        .unwrap_err();
    let error = format!("{error:#}");
    assert!(error.contains("400 Bad Request: status 400"), "{error}");
    assert_eq!(endpoint.requests().len(), 3);
}

#[tokio::test]
async fn an_unreachable_endpoint_fails_the_publication() {
    let endpoint = Endpoint::start([], 200).await;
    let url = endpoint.url("/");
    drop(endpoint);
    let sink = HttpSink::<Utf8, String>::new(HttpSinkConfig::new(url)).unwrap();
    let prepared = sink.prepare(HttpPublish::new("a".to_owned())).unwrap();
    let error = sink.publish(&prepared).await.unwrap_err();
    assert!(format!("{error:#}").contains("HTTP request to"));
}

#[tokio::test]
async fn a_closed_sink_refuses_to_publish() {
    let sink = HttpSink::<Utf8, String>::new(HttpSinkConfig::new("http://127.0.0.1:9/")).unwrap();
    let prepared = sink.prepare(HttpPublish::new("a".to_owned())).unwrap();
    sink.close().await.unwrap();
    sink.close().await.unwrap();
    let error = sink.publish(&prepared).await.unwrap_err();
    assert!(error.to_string().contains("closed"));
}

#[test]
fn invalid_configuration_is_rejected() {
    let invalid = [
        HttpSinkConfig::new("not a url"),
        HttpSinkConfig::new("ftp://example.com/"),
        HttpSinkConfig::new("http://example.com/").header("bad name", "value"),
        HttpSinkConfig {
            method: "BAD METHOD".to_owned(),
            ..HttpSinkConfig::new("http://example.com/")
        },
        HttpSinkConfig {
            request_timeout: Duration::ZERO,
            ..HttpSinkConfig::new("http://example.com/")
        },
    ];
    for config in invalid {
        assert!(HttpSink::<Utf8, String>::new(config).is_err());
    }
}

#[test]
fn invalid_outputs_fail_to_prepare() {
    let sink = HttpSink::<Utf8, String>::new(HttpSinkConfig::new("http://example.com/")).unwrap();
    assert!(
        sink.prepare(HttpPublish::new("a".to_owned()).header("bad name", "value"))
            .is_err()
    );
    assert!(
        sink.prepare(HttpPublish::new("a".to_owned()).path("relative"))
            .is_err()
    );
    let prepared = sink
        .prepare(HttpPublish::new("a".to_owned()).path("/items/1?x=2"))
        .unwrap();
    assert_eq!(prepared.url().as_str(), "http://example.com/items/1?x=2");
}
