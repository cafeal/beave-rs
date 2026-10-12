#![cfg(feature = "pulsar")]

use beavers::{
    BoxError, Receive, ReceiveError, Sink, Source, SourceMessage, Utf8,
    adapters::pulsar::{
        PulsarAuthentication, PulsarMessage, PulsarMessageId, PulsarMetadata, PulsarPublish,
        PulsarRecord, PulsarSink, PulsarSinkConfig, PulsarSource, PulsarSourceConfig,
        PulsarSubscriptionType,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::JoinSet,
};

#[test]
fn records_keep_delivery_facts_separate_from_application_fields() {
    let record = PulsarRecord {
        value: Some("order-1".to_owned()),
        key: Some(b"customer-7".to_vec()),
        properties: [("kind".to_owned(), "order".to_owned())].into(),
        event_time: Some(42),
        metadata: PulsarMetadata {
            topic: "persistent://public/default/orders".to_owned(),
            message_id: PulsarMessageId {
                ledger_id: 7,
                entry_id: 3,
                partition: -1,
                batch_index: -1,
            },
            publish_time: 41,
        },
    };
    assert_eq!(record.key.as_deref(), Some(b"customer-7".as_slice()));
    assert_eq!(
        record.properties.get("kind").map(String::as_str),
        Some("order")
    );
    assert_eq!(record.event_time, Some(42));
    assert_eq!(record.metadata.topic, "persistent://public/default/orders");
    assert_eq!(record.metadata.publish_time, 41);

    let mut publish = PulsarPublish::new("processed".to_owned());
    publish.key = record.key.clone();
    publish.properties = record.properties.clone();
    publish.event_time = record.event_time;
    assert_eq!(publish.value.as_deref(), Some("processed"));
    assert_eq!(publish.key, record.key);
    assert_eq!(publish.properties, record.properties);
    assert_eq!(publish.event_time, record.event_time);
}
use std::{
    collections::HashSet,
    env,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos}-{sequence}")
}

#[test]
fn tombstones_are_prepared_as_null_values() {
    assert!(
        PulsarSourceConfig::new("pulsar://broker", "topic", "subscription")
            .empty_payload_is_tombstone
    );
    let sink = PulsarSink::<Utf8, String>::new(PulsarSinkConfig::new("pulsar://broker", "topic"));
    let tombstone = sink
        .prepare(PulsarPublish::tombstone(b"customer-7".to_vec()))
        .unwrap();
    assert_eq!(tombstone.value, None);
    assert_eq!(tombstone.key.as_deref(), Some(b"customer-7".as_slice()));
    assert_eq!(
        sink.prepare(PulsarPublish::new(String::new()))
            .unwrap()
            .value,
        Some(Vec::new())
    );
}

#[test]
fn configs_validate_without_connecting() {
    assert!(
        PulsarSourceConfig::new("", "topic", "subscription")
            .validate()
            .is_err()
    );
    assert!(
        PulsarSourceConfig::new("pulsar://broker", "", "subscription")
            .validate()
            .is_err()
    );
    assert!(
        PulsarSourceConfig::new("pulsar://broker", "topic", "")
            .validate()
            .is_err()
    );

    let mut source = PulsarSourceConfig::new("pulsar://broker", "topic", "subscription");
    source.buffer_size = 0;
    assert!(source.validate().is_err());

    let mut authenticated = PulsarSinkConfig::new("pulsar://broker", "topic");
    authenticated.authentication = Some(PulsarAuthentication {
        name: String::new(),
        data: vec![1],
    });
    assert!(authenticated.validate().is_err());
    assert!(
        PulsarSinkConfig::new("pulsar://broker", "")
            .validate()
            .is_err()
    );
    let mut unbounded = PulsarSinkConfig::new("pulsar://broker", "topic");
    unbounded.max_pending = 0;
    assert!(unbounded.validate().is_err());
    let mut unsent = PulsarSinkConfig::new("pulsar://broker", "topic");
    unsent.send_retry.max_attempts = 0;
    assert!(unsent.validate().is_err());
}

fn service_url() -> String {
    env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://127.0.0.1:6650".into())
}

fn unique_topic(prefix: &str) -> String {
    format!("persistent://public/default/{}", unique_name(prefix))
}

/// Sends a `PUT` for `topic` to the admin REST API at `PULSAR_ADMIN_ADDR`
/// (default: `127.0.0.1:8080`). `action` follows the topic path.
async fn put_topic_admin(topic: &str, action: &str, body: &str) -> Result<(), BoxError> {
    let address = env::var("PULSAR_ADMIN_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let path = topic.replacen("persistent://", "/admin/v2/persistent/", 1);
    let request = format!(
        "PUT {path}/{action} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut stream = TcpStream::connect(&address).await?;
    stream.write_all(request.as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    if !(response.starts_with("HTTP/1.1 2")) {
        return Err(format!("{action} of {topic} failed: {response}").into());
    };
    Ok(())
}

/// Creates a durable subscription at the earliest position, so messages
/// published before the source's consumer connects are still delivered to it.
async fn create_subscription(topic: &str, subscription: &str) -> Result<(), BoxError> {
    put_topic_admin(
        topic,
        &format!("subscription/{subscription}"),
        r#"{"ledgerId":-1,"entryId":-1}"#,
    )
    .await
}

/// Closes the topic on its broker, so clients reconnect to it.
async fn unload_topic(topic: &str) -> Result<(), BoxError> {
    put_topic_admin(topic, "unload", "").await
}

async fn next_message(
    source: &mut PulsarSource<Utf8, String>,
    timeout: Duration,
) -> Result<Option<PulsarMessage<Utf8, String>>, BoxError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, source.receive()).await {
            Err(_) => return Ok(None),
            Ok(Ok(Receive::Message(message))) => return Ok(Some(message)),
            Ok(Ok(Receive::End)) => return Err("Pulsar source ended".into()),
            Ok(Err(ReceiveError::Retry(_))) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(Err(ReceiveError::Fatal(error))) => {
                return Err(format!("Pulsar receive failed: {error}").into());
            }
        }
    }
}

async fn publish_all(topic: &str, values: &[&str]) -> Result<(), BoxError> {
    let sink = PulsarSink::<Utf8, String>::new(PulsarSinkConfig::new(service_url(), topic));
    for value in values {
        let mut output = PulsarPublish::new((*value).to_owned());
        output.key = Some(value.as_bytes().to_vec());
        sink.publish(&sink.prepare(output)?).await?;
    }
    sink.close().await
}

fn exclusive_source(topic: &str, subscription: &str) -> PulsarSource<Utf8, String> {
    let mut config = PulsarSourceConfig::new(service_url(), topic, subscription);
    config.subscription_type = PulsarSubscriptionType::Exclusive;
    PulsarSource::new(config)
}

/// Requires the development broker at `PULSAR_URL` and its admin API at
/// `PULSAR_ADMIN_ADDR`. A delivery received before its topic is unloaded is
/// acknowledged once the consumer has reconnected.
#[tokio::test]
#[ignore = "requires a Pulsar broker; run with cargo test --features pulsar -- --ignored"]
async fn acknowledgement_is_retried_across_a_topic_unload() -> Result<(), BoxError> {
    let topic = unique_topic("beavers-pulsar-unload");
    let subscription = unique_name("beavers-pulsar-unload");
    create_subscription(&topic, &subscription).await?;
    let mut source = exclusive_source(&topic, &subscription);
    publish_all(&topic, &["a"]).await?;
    let delivery = next_message(&mut source, Duration::from_secs(20))
        .await?
        .expect("timed out waiting for input");
    unload_topic(&topic).await?;
    tokio::time::timeout(Duration::from_secs(120), delivery.ack())
        .await
        .expect("timed out acknowledging after the unload")?;
    source.close().await?;

    let mut source = exclusive_source(&topic, &subscription);
    assert!(
        next_message(&mut source, Duration::from_secs(2))
            .await?
            .is_none(),
        "the acknowledged delivery was redelivered"
    );
    source.close().await
}

/// Requires the development broker at `PULSAR_URL` and its admin API at
/// `PULSAR_ADMIN_ADDR`. Unloading a topic rejects some of the sends in flight
/// with a persistence error. The sink sends them again, so every submission
/// completes and every message reaches the topic.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a Pulsar broker; run with cargo test --features pulsar -- --ignored"]
async fn submissions_complete_across_topic_unloads() -> Result<(), BoxError> {
    let topic = unique_topic("beavers-pulsar-resend");
    let subscription = unique_name("beavers-pulsar-resend");
    create_subscription(&topic, &subscription).await?;
    let mut source = exclusive_source(&topic, &subscription);

    let sink = PulsarSink::<Utf8, String>::new(PulsarSinkConfig::new(service_url(), &topic));
    let stop = Arc::new(AtomicBool::new(false));
    let publisher = tokio::spawn({
        let stop = stop.clone();
        async move {
            // A completion holds its `max_pending` slot until it is awaited.
            let mut completions = JoinSet::new();
            let mut count = 0;
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..10 {
                    let output = sink.prepare(PulsarPublish::new(count.to_string()))?;
                    completions.spawn(sink.submit(&output).await?.wait());
                    count += 1;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            while let Some(result) = completions.join_next().await {
                result??;
            }
            sink.close().await?;
            Ok::<_, BoxError>(count)
        }
    });
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        unload_topic(&topic).await?;
    }
    stop.store(true, Ordering::Relaxed);
    let count = tokio::time::timeout(Duration::from_secs(120), publisher)
        .await
        .expect("timed out waiting for the broker receipts")??;

    let mut missing: HashSet<_> = (0..count).map(|index| index.to_string()).collect();
    while !missing.is_empty() {
        let message = next_message(&mut source, Duration::from_secs(30))
            .await?
            .unwrap_or_else(|| panic!("{} of {count} messages are missing", missing.len()));
        missing.remove(&message.decode()?.value.expect("a value"));
    }
    source.close().await
}

/// Requires the development broker at `PULSAR_URL`.
#[tokio::test]
#[ignore = "requires a Pulsar broker; run with cargo test --features pulsar -- --ignored"]
async fn tombstones_carry_the_null_value_marker() -> Result<(), BoxError> {
    let topic = unique_topic("beavers-pulsar-tombstone");
    let subscription = unique_name("tombstones");
    create_subscription(&topic, &subscription).await?;
    let mut config = PulsarSourceConfig::new(service_url(), &topic, subscription);
    config.empty_payload_is_tombstone = false;
    let mut source = PulsarSource::<Utf8, String>::new(config);

    let sink = PulsarSink::<Utf8, String>::new(PulsarSinkConfig::new(service_url(), &topic));
    for output in [
        PulsarPublish::tombstone(b"customer-7".to_vec()),
        PulsarPublish::new(String::new()),
    ] {
        sink.publish(&sink.prepare(output)?).await?;
    }
    sink.close().await?;

    let mut values = Vec::new();
    for _ in 0..2 {
        let message = next_message(&mut source, Duration::from_secs(20))
            .await?
            .expect("timed out waiting for Pulsar record");
        values.push(message.decode()?.value);
        message.ack().await?;
    }
    assert_eq!(values, [None, Some(String::new())]);
    source.close().await
}

/// Requires the development broker at `PULSAR_URL` (default:
/// `pulsar://127.0.0.1:6650`).
#[tokio::test]
#[ignore = "requires a Pulsar broker; run with cargo test --features pulsar -- --ignored"]
async fn publish_receive_and_ack_against_pulsar() -> Result<(), BoxError> {
    let service_url = env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://127.0.0.1:6650".into());
    let topic = format!(
        "persistent://public/default/{}",
        unique_name("beavers-pulsar-test")
    );
    let subscription = unique_name("beavers-pulsar-subscription");
    // Create the subscription before publication so it cannot miss the
    // message because a new subscription starts at the latest position.
    create_subscription(&topic, &subscription).await?;

    let sink = PulsarSink::<Utf8, String>::new(PulsarSinkConfig::new(&service_url, &topic));
    let mut source = PulsarSource::<Utf8, String>::new(PulsarSourceConfig::new(
        &service_url,
        &topic,
        &subscription,
    ));

    let receive_task = tokio::spawn(async move {
        let message = loop {
            match source.receive().await {
                Ok(Receive::Message(message)) => break message,
                Ok(Receive::End) => return Err("Pulsar source ended before delivery".into()),
                Err(ReceiveError::Retry(_)) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(ReceiveError::Fatal(error)) => {
                    return Err(format!("Pulsar receive failed: {error}").into());
                }
            }
        };
        let record = message.decode()?;
        let metadata = record.metadata.clone();
        message.ack().await?;
        source.close().await?;
        Ok::<_, BoxError>((record, metadata))
    });

    let mut output = PulsarPublish::new("hello pulsar".to_owned());
    output.key = Some(b"binary-key".to_vec());
    output
        .properties
        .insert("kind".to_owned(), "test".to_owned());
    let prepared = sink.prepare(output)?;
    tokio::time::timeout(Duration::from_secs(20), sink.publish(&prepared))
        .await
        .expect("timed out publishing to Pulsar")?;

    let (record, metadata) = tokio::time::timeout(Duration::from_secs(20), receive_task)
        .await
        .expect("timed out waiting for Pulsar record")??;
    assert_eq!(record.value.as_deref(), Some("hello pulsar"));
    assert_eq!(metadata.topic, topic);
    assert_eq!(record.key.as_deref(), Some(b"binary-key".as_slice()));
    assert!(
        record
            .properties
            .iter()
            .any(|(key, value)| key == "kind" && value == "test")
    );
    sink.close().await?;
    Ok(())
}

/// Requires a reachable development broker (`PULSAR_URL`, default
/// `pulsar://127.0.0.1:6650`).
#[tokio::test]
#[ignore = "requires a Pulsar broker; run with cargo test --features pulsar -- --ignored"]
async fn submitted_messages_complete_on_their_broker_receipts() -> Result<(), BoxError> {
    let topic = unique_topic("beavers-pulsar-submit");
    let mut config = PulsarSinkConfig::new(service_url(), &topic);
    config.max_pending = 2;
    let sink = PulsarSink::<Utf8, String>::new(config);
    let prepared = sink.prepare(PulsarPublish::new("a".to_owned()))?;

    let first = tokio::time::timeout(Duration::from_secs(20), sink.submit(&prepared))
        .await
        .expect("timed out connecting")?;
    let second = sink.submit(&prepared).await?;
    assert!(!first.is_done() && !second.is_done());
    assert!(
        tokio::time::timeout(Duration::from_millis(500), sink.submit(&prepared))
            .await
            .is_err(),
        "a third message was accepted beyond max_pending"
    );
    for completion in [first, second] {
        tokio::time::timeout(Duration::from_secs(20), completion.wait())
            .await
            .expect("timed out waiting for a broker receipt")?;
    }
    tokio::time::timeout(Duration::from_secs(20), sink.publish(&prepared))
        .await
        .expect("timed out publishing after the completions freed their slots")?;
    sink.close().await?;
    Ok(())
}
