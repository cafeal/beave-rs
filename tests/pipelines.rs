//! End-to-end pipelines against the local brokers in `compose.yaml`.
//!
//! These tests are ignored by default. Start the brokers with
//! `docker compose up -d --wait` and run them with `cargo test-live`.
//! `KAFKA_BROKERS`, `PULSAR_URL`, and `PULSAR_ADMIN_ADDR` override the compose
//! defaults.
#![cfg(all(feature = "kafka", feature = "pulsar"))]

use beavers::{
    App, CancellationToken, IterSource, Json, MapMetadata, Receive, ReceiveError, Result, Source,
    SourceMessage, Subscription,
    adapters::{
        kafka::{
            KafkaPublish, KafkaRecord, KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig,
        },
        pulsar::{
            PulsarPublish, PulsarRecord, PulsarSink, PulsarSinkConfig, PulsarSource,
            PulsarSourceConfig,
        },
    },
};
use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    consumer::{BaseConsumer, Consumer},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    env,
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};

const ORDERS: u64 = 6;
const DEADLINE: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct Order {
    id: u64,
    customer: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct Shipment {
    order_id: u64,
}

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos}-{sequence}")
}

fn kafka_brokers() -> String {
    env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into())
}

fn pulsar_url() -> String {
    env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://localhost:6650".into())
}

fn pulsar_topic(name: &str) -> String {
    format!("persistent://public/default/{}", unique_name(name))
}

fn orders() -> impl Iterator<Item = Order> + Send {
    (1..=ORDERS).map(|id| Order {
        id,
        customer: format!("customer-{}", id % 3),
    })
}

fn customer_key(order_id: u64) -> Vec<u8> {
    format!("customer-{}", order_id % 3).into_bytes()
}

async fn ship(order: Order) -> Result<Shipment> {
    Ok(Shipment { order_id: order.id })
}

/// Runs an application until the returned token is cancelled.
fn spawn_app(app: App) -> (CancellationToken, JoinHandle<anyhow::Result<()>>) {
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(app.run_until(shutdown.clone()));
    (shutdown, handle)
}

async fn stop_app(shutdown: CancellationToken, handle: JoinHandle<anyhow::Result<()>>) {
    shutdown.cancel();
    timeout(DEADLINE, handle)
        .await
        .expect("timed out stopping the application")
        .unwrap()
        .unwrap();
}

/// Receives and acknowledges `count` decoded records from a source.
async fn receive<S, T, F>(source: &mut S, count: u64, decode: F) -> Vec<T>
where
    S: Source,
    F: Fn(&S::Message) -> T,
{
    let deadline = Instant::now() + DEADLINE;
    let mut records = Vec::new();
    while (records.len() as u64) < count {
        let received = timeout(
            deadline.saturating_duration_since(Instant::now()),
            source.receive(),
        )
        .await
        .expect("timed out waiting for pipeline output");
        match received {
            Ok(Receive::Message(message)) => {
                records.push(decode(&message));
                message.ack().await.unwrap();
            }
            Ok(Receive::End) => panic!("source ended before it received the pipeline output"),
            Err(ReceiveError::Retry(_)) => sleep(Duration::from_millis(100)).await,
            Err(ReceiveError::Fatal(error)) => panic!("source failed: {error:#}"),
        }
    }
    records
}

async fn read_kafka<T>(topic: &str, count: u64) -> Vec<KafkaRecord<T>>
where
    T: Clone + Send + Sync + for<'de> Deserialize<'de> + 'static,
{
    let mut config =
        KafkaSourceConfig::new(kafka_brokers(), unique_name("beavers-reader"), [topic]);
    config
        .properties
        .insert("auto.offset.reset".into(), "earliest".into());
    let mut source = KafkaSource::<Json, T>::new(config);
    let records = receive(&mut source, count, |message| message.decode().unwrap()).await;
    source.close().await.unwrap();
    records
}

async fn read_pulsar<T>(topic: &str, subscription: &str, count: u64) -> Vec<PulsarRecord<T>>
where
    T: Clone + Send + Sync + for<'de> Deserialize<'de> + 'static,
{
    let mut source =
        PulsarSource::<Json, T>::new(PulsarSourceConfig::new(pulsar_url(), topic, subscription));
    let records = receive(&mut source, count, |message| message.decode().unwrap()).await;
    source.close().await.unwrap();
    records
}

async fn produce_kafka_orders(topic: &str) {
    let sink = KafkaSink::<Json, Order>::new(KafkaSinkConfig::new(kafka_brokers(), topic));
    let app = App::new().subscribe(
        "produce-orders",
        IterSource::new(orders()),
        sink,
        |order: Order| async move {
            let mut output = KafkaPublish::new(order.clone());
            output.key = Some(order.customer.into_bytes());
            output
                .headers
                .push(("kind".into(), Some(b"order".to_vec())));
            Ok(output)
        },
    );
    timeout(DEADLINE, app.run())
        .await
        .expect("timed out producing orders")
        .unwrap();
}

async fn produce_pulsar_orders(topic: &str) {
    let sink = PulsarSink::<Json, Order>::new(PulsarSinkConfig::new(pulsar_url(), topic));
    let app = App::new().subscribe(
        "produce-orders",
        IterSource::new(orders()),
        sink,
        |order: Order| async move {
            let mut output = PulsarPublish::new(order.clone());
            output.key = Some(order.customer.into_bytes());
            output.properties.insert("kind".into(), "order".into());
            Ok(output)
        },
    );
    timeout(DEADLINE, app.run())
        .await
        .expect("timed out producing orders")
        .unwrap();
}

fn committed_kafka_offsets(group: String, topic: String) -> i64 {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", kafka_brokers())
        .set("group.id", group)
        .create()
        .unwrap();
    let metadata = consumer
        .fetch_metadata(Some(&topic), Duration::from_secs(20))
        .unwrap();
    let mut partitions = TopicPartitionList::new();
    for partition in metadata.topics()[0].partitions() {
        partitions.add_partition(&topic, partition.id());
    }
    consumer
        .committed_offsets(partitions, Duration::from_secs(20))
        .unwrap()
        .elements()
        .iter()
        .map(|element| match element.offset() {
            Offset::Offset(offset) => offset,
            Offset::Invalid => 0,
            other => panic!("unexpected committed offset {other:?}"),
        })
        .sum()
}

/// Sends one request to the Pulsar admin REST API and returns the status code
/// and body. The compose broker speaks plain HTTP/1.1.
async fn pulsar_admin(method: &str, path: &str, body: &str) -> (u16, String) {
    let authority = env::var("PULSAR_ADMIN_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let mut stream = TcpStream::connect(&authority).await.unwrap();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|status| status.parse().ok())
        .expect("malformed admin response");
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_owned())
        .unwrap_or_default();
    (status, body)
}

fn admin_path(topic: &str) -> String {
    format!(
        "/admin/v2/persistent/{}",
        topic
            .strip_prefix("persistent://")
            .expect("expected a persistent topic")
    )
}

/// Creates a durable subscription at the earliest position, so a pipeline can
/// be started after its input was produced.
async fn create_pulsar_subscription(topic: &str, subscription: &str) {
    let path = format!("{}/subscription/{subscription}", admin_path(topic));
    let (status, body) = pulsar_admin("PUT", &path, r#"{"ledgerId":-1,"entryId":-1}"#).await;
    assert_eq!(status / 100, 2, "creating subscription failed: {body}");
}

async fn pulsar_backlog(topic: &str, subscription: &str) -> u64 {
    let (status, body) = pulsar_admin("GET", &format!("{}/stats", admin_path(topic)), "").await;
    assert_eq!(status, 200, "reading topic stats failed: {body}");
    // Connection: close responses may still use chunked encoding; the JSON
    // object is the part between the first `{` and the last `}`.
    let json = &body[body.find('{').unwrap()..=body.rfind('}').unwrap()];
    let stats: serde_json::Value = serde_json::from_str(json).unwrap();
    stats["subscriptions"][subscription]["msgBacklog"]
        .as_u64()
        .expect("subscription is missing from topic stats")
}

async fn eventually<F, Fut>(description: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + DEADLINE;
    while !condition().await {
        assert!(
            Instant::now() < deadline,
            "timed out waiting until {description}"
        );
        sleep(Duration::from_millis(200)).await;
    }
}

fn order_ids<T>(records: &[T], id: impl Fn(&T) -> u64) -> BTreeSet<u64> {
    records.iter().map(id).collect()
}

#[tokio::test]
#[ignore = "requires the compose brokers; run with cargo test-live"]
async fn kafka_pipeline_forwards_keys_and_commits_offsets() {
    let input = unique_name("beavers-orders");
    let output = unique_name("beavers-shipments");
    let group = unique_name("beavers-pipeline");
    produce_kafka_orders(&input).await;

    let mut source = KafkaSourceConfig::new(kafka_brokers(), &group, [&input]);
    source
        .properties
        .insert("auto.offset.reset".into(), "earliest".into());
    let (shutdown, app) = spawn_app(
        App::new().subscription(
            Subscription::new(
                "ship-orders",
                KafkaSource::<Json, Order>::new(source),
                KafkaSink::<Json, Shipment>::new(KafkaSinkConfig::new(kafka_brokers(), &output)),
                ship,
            )
            .concurrency(3),
        ),
    );

    let shipments = read_kafka::<Shipment>(&output, ORDERS).await;
    assert_eq!(
        order_ids(&shipments, |record| record.value.as_ref().unwrap().order_id),
        (1..=ORDERS).collect()
    );
    for record in &shipments {
        let order_id = record.value.as_ref().unwrap().order_id;
        assert_eq!(record.key, Some(customer_key(order_id)));
        assert_eq!(
            record.headers,
            vec![("kind".into(), Some(b"order".to_vec()))]
        );
    }

    stop_app(shutdown, app).await;
    let committed = tokio::task::spawn_blocking(move || committed_kafka_offsets(group, input))
        .await
        .unwrap();
    assert_eq!(committed, ORDERS as i64);
}

#[tokio::test]
#[ignore = "requires the compose brokers; run with cargo test-live"]
async fn pulsar_pipeline_forwards_keys_and_acknowledges() {
    let input = pulsar_topic("beavers-orders");
    let output = pulsar_topic("beavers-shipments");
    let subscription = unique_name("beavers-pipeline");
    let reader = unique_name("beavers-reader");
    create_pulsar_subscription(&input, &subscription).await;
    create_pulsar_subscription(&output, &reader).await;
    produce_pulsar_orders(&input).await;

    let (shutdown, app) = spawn_app(
        App::new().subscription(
            Subscription::new(
                "ship-orders",
                PulsarSource::<Json, Order>::new(PulsarSourceConfig::new(
                    pulsar_url(),
                    &input,
                    &subscription,
                )),
                PulsarSink::<Json, Shipment>::new(PulsarSinkConfig::new(pulsar_url(), &output)),
                ship,
            )
            .concurrency(3),
        ),
    );

    let shipments = read_pulsar::<Shipment>(&output, &reader, ORDERS).await;
    assert_eq!(
        order_ids(&shipments, |record| record.value.as_ref().unwrap().order_id),
        (1..=ORDERS).collect()
    );
    for record in &shipments {
        let order_id = record.value.as_ref().unwrap().order_id;
        assert_eq!(record.key, Some(customer_key(order_id)));
        assert_eq!(
            record.properties.get("kind").map(String::as_str),
            Some("order")
        );
    }

    stop_app(shutdown, app).await;
    eventually("the pipeline subscription has no backlog", || async {
        pulsar_backlog(&input, &subscription).await == 0
    })
    .await;
}

#[tokio::test]
#[ignore = "requires the compose brokers; run with cargo test-live"]
async fn kafka_to_pulsar_pipeline_maps_metadata_explicitly() {
    let input = unique_name("beavers-orders");
    let output = pulsar_topic("beavers-bridged-orders");
    let reader = unique_name("beavers-reader");
    create_pulsar_subscription(&output, &reader).await;
    produce_kafka_orders(&input).await;

    let mut source =
        KafkaSourceConfig::new(kafka_brokers(), unique_name("beavers-bridge"), [&input]);
    source
        .properties
        .insert("auto.offset.reset".into(), "earliest".into());
    let (shutdown, app) = spawn_app(
        App::new().subscription(
            Subscription::new(
                "bridge-orders",
                KafkaSource::<Json, Order>::new(source),
                PulsarSink::<Json, Order>::new(PulsarSinkConfig::new(pulsar_url(), &output)),
                |record: KafkaRecord<Order>| async move {
                    Ok(PulsarPublish::new(
                        record.value.expect("orders are not tombstones"),
                    ))
                },
            )
            .middleware(MapMetadata::new(
                |input: &KafkaRecord<Order>, mut output: PulsarPublish<Order>| {
                    output.key = input.key.clone();
                    for (name, value) in &input.headers {
                        if let Some(value) = value {
                            output
                                .properties
                                .insert(name.clone(), String::from_utf8(value.clone())?);
                        }
                    }
                    Ok(output)
                },
            )),
        ),
    );

    let bridged = read_pulsar::<Order>(&output, &reader, ORDERS).await;
    assert_eq!(
        order_ids(&bridged, |record| record.value.as_ref().unwrap().id),
        (1..=ORDERS).collect()
    );
    for record in &bridged {
        let order_id = record.value.as_ref().unwrap().id;
        assert_eq!(record.key, Some(customer_key(order_id)));
        assert_eq!(
            record.properties.get("kind").map(String::as_str),
            Some("order")
        );
    }

    stop_app(shutdown, app).await;
}
