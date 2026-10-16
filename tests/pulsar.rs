#![cfg(feature = "pulsar")]

use beavers::{
    BoxError, Distribution, OrderingKey, Receive, ReceiveError, RetryPolicy, Sink, Source,
    SourceMessage, Utf8,
    adapters::pulsar::{
        Pulsar, PulsarAuthentication, PulsarDistribution, PulsarMessage, PulsarPublish,
        PulsarSource, PulsarSubscriptionType,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::JoinSet,
};

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
    let sink = Pulsar::new("pulsar://broker").sink::<Utf8>("topic");
    let tombstone = sink
        .prepare(PulsarPublish::tombstone(b"customer-7".to_vec()))
        .unwrap();
    assert_eq!(tombstone.payload(), None);
    assert_eq!(tombstone.key(), Some(b"customer-7".as_slice()));
    assert_eq!(
        sink.prepare(PulsarPublish::new(String::new()))
            .unwrap()
            .payload(),
        Some(&[][..])
    );
}

#[test]
fn settings_validate_without_connecting() {
    let pulsar = Pulsar::new("pulsar://broker");
    assert!(
        pulsar
            .source::<Utf8>("topic", "subscription")
            .validate()
            .is_ok()
    );
    assert!(pulsar.sink::<Utf8>("topic").validate().is_ok());

    assert!(Pulsar::new("").validate().is_err());
    assert!(Pulsar::new("http://broker").validate().is_err());
    assert!(
        Pulsar::new("")
            .source::<Utf8>("topic", "subscription")
            .validate()
            .is_err()
    );
    assert!(
        pulsar
            .source::<Utf8>("", "subscription")
            .validate()
            .is_err()
    );
    assert!(pulsar.source::<Utf8>("topic", "").validate().is_err());
    assert!(
        pulsar
            .source::<Utf8>("topic", "subscription")
            .buffer_size(0)
            .validate()
            .is_err()
    );

    let authenticated = pulsar.clone().authentication(PulsarAuthentication {
        name: String::new(),
        data: vec![1],
    });
    assert!(authenticated.validate().is_err());
    assert!(authenticated.sink::<Utf8>("topic").validate().is_err());
    assert!(pulsar.sink::<Utf8>("").validate().is_err());
    assert!(
        pulsar
            .sink::<Utf8>("topic")
            .max_pending(0)
            .validate()
            .is_err()
    );
    let unsent = RetryPolicy::default().max_attempts(0);
    assert!(
        pulsar
            .sink::<Utf8>("topic")
            .send_retry(unsent)
            .validate()
            .is_err()
    );
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
    source: &mut PulsarSource<Utf8>,
    timeout: Duration,
) -> Result<Option<PulsarMessage<Utf8>>, BoxError> {
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
    let sink = Pulsar::new(service_url()).sink::<Utf8>(topic);
    for value in values {
        let mut output = PulsarPublish::new((*value).to_owned());
        output.key = Some(value.as_bytes().to_vec());
        sink.publish(&sink.prepare(output)?).await?;
    }
    sink.close().await
}

fn exclusive_source(topic: &str, subscription: &str) -> PulsarSource<Utf8> {
    Pulsar::new(service_url())
        .source(topic, subscription)
        .subscription_type(PulsarSubscriptionType::Exclusive)
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

    let sink = Pulsar::new(service_url()).sink::<Utf8>(&topic);
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
    let pulsar = Pulsar::new(service_url());
    let mut source = pulsar
        .source::<Utf8>(&topic, subscription)
        .empty_payload_is_tombstone(false);
    let sink = pulsar.sink::<Utf8>(&topic);
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

    let pulsar = Pulsar::new(&service_url);
    let sink = pulsar.sink::<Utf8>(&topic);
    let mut source = pulsar.source::<Utf8>(&topic, &subscription);

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
    let sink = Pulsar::new(service_url())
        .sink::<Utf8>(&topic)
        .max_pending(2);
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

/// Requires the development broker at `PULSAR_URL`. The probe reports no
/// connection state before the source subscribes, then reports the consumer
/// connected, and the delivery describes itself with messaging attributes.
#[tokio::test]
#[ignore = "requires a Pulsar broker; run with cargo test --features pulsar -- --ignored"]
async fn probe_reports_the_consumer_connection() -> Result<(), BoxError> {
    let topic = unique_topic("beavers-pulsar-probe");
    let subscription = unique_name("beavers-pulsar-probe");
    create_subscription(&topic, &subscription).await?;
    let mut source = exclusive_source(&topic, &subscription);
    let probe = source.probe().expect("the Pulsar source reports a probe");
    assert_eq!(probe.connected(), None);

    publish_all(&topic, &["a"]).await?;
    let delivery = next_message(&mut source, Duration::from_secs(20))
        .await?
        .expect("timed out waiting for input");
    assert_eq!(probe.connected(), Some(true));
    assert!(delivery.messaging_attributes().is_some());
    delivery.ack().await?;
    source.close().await?;
    assert_eq!(probe.connected(), None);
    Ok(())
}

/// Requires the development broker at `PULSAR_URL` and its admin API at
/// `PULSAR_ADMIN_ADDR`. The distributions key deliveries by partition, by the
/// ordering key over the message key, and fall back to the partition for a
/// message without either.
#[tokio::test]
#[ignore = "requires a Pulsar broker; run with cargo test --features pulsar -- --ignored"]
async fn distributions_key_deliveries_by_partition_or_message_key() -> Result<(), BoxError> {
    let topic = unique_topic("beavers-pulsar-distribution");
    let subscription = unique_name("beavers-pulsar-distribution");
    create_subscription(&topic, &subscription).await?;
    let sink = Pulsar::new(service_url()).sink::<Utf8>(&topic);
    // Each message's key and ordering key.
    let keys = [
        (Some(b"customer".as_slice()), None),
        (Some(b"customer".as_slice()), Some(b"order".as_slice())),
        (None, None),
    ];
    for (key, ordering_key) in keys {
        let mut output = PulsarPublish::new("order".to_owned());
        output.key = key.map(<[u8]>::to_vec);
        output.ordering_key = ordering_key.map(<[u8]>::to_vec);
        sink.publish(&sink.prepare(output)?).await?;
    }
    sink.close().await?;

    let mut source = exclusive_source(&topic, &subscription);
    for (key, ordering_key) in keys {
        let message = next_message(&mut source, Duration::from_secs(20))
            .await?
            .expect("timed out waiting for a delivery");
        let record = message.decode()?;
        let partition = OrderingKey::new(
            record.metadata.topic.as_str(),
            i64::from(record.metadata.message_id.partition),
        );
        assert_eq!(
            PulsarDistribution::Partition.key(&message, &record),
            Some(partition.clone())
        );
        let by_key = match ordering_key.or(key) {
            Some(key) => partition.with_key(key),
            None => partition,
        };
        assert_eq!(
            PulsarDistribution::MessageKey.key(&message, &record),
            Some(by_key)
        );
        message.ack().await?;
    }
    source.close().await
}
