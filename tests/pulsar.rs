#![cfg(feature = "pulsar")]

use beavers::{
    App, CancellationToken, Receive, ReceiveError, Sink, Source, SourceMessage, Subscription,
    TransactionalSink, Utf8,
    adapters::pulsar::{
        PulsarAuthentication, PulsarMessage, PulsarMessageId, PulsarMetadata, PulsarPublish,
        PulsarRecord, PulsarSink, PulsarSinkConfig, PulsarSource, PulsarSourceConfig,
        PulsarSubscriptionType, PulsarTransactionalSink,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
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
    env,
    sync::atomic::{AtomicU64, Ordering},
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
    let mut transactional = PulsarSinkConfig::new("pulsar://broker", "topic");
    transactional.transaction_timeout = Duration::ZERO;
    assert!(transactional.validate().is_err());
}

fn uppercase_pipeline(
    source: PulsarSource<Utf8, String>,
    sink: PulsarTransactionalSink<Utf8, String>,
) -> Subscription<
    PulsarSource<Utf8, String>,
    PulsarTransactionalSink<Utf8, String>,
    PulsarPublish<String>,
> {
    Subscription::forward("uppercase", source, sink, |value: String| async move {
        Ok(value.to_uppercase())
    })
    .transactional()
}

#[test]
fn pulsar_source_and_transactional_sink_form_a_transactional_pair() {
    let source = PulsarSource::new(PulsarSourceConfig::new(
        "pulsar://broker",
        "in",
        "subscription",
    ));
    let sink = PulsarSink::new(PulsarSinkConfig::new("pulsar://broker", "out")).transactional();
    let _ = uppercase_pipeline(source, sink);
}

fn service_url() -> String {
    env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://127.0.0.1:6650".into())
}

fn unique_topic(prefix: &str) -> String {
    format!("persistent://public/default/{}", unique_name(prefix))
}

/// Creates a partitioned topic through the admin REST API at
/// `PULSAR_ADMIN_ADDR` (default: `127.0.0.1:8080`).
async fn create_partitioned_topic(topic: &str, partitions: u32) -> anyhow::Result<()> {
    let address = env::var("PULSAR_ADMIN_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let path = topic.replacen("persistent://", "/admin/v2/persistent/", 1);
    let body = partitions.to_string();
    let request = format!(
        "PUT {path}/partitions HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut stream = TcpStream::connect(&address).await?;
    stream.write_all(request.as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    anyhow::ensure!(
        response.starts_with("HTTP/1.1 2"),
        "creating {topic} failed: {response}"
    );
    Ok(())
}

async fn next_message(
    source: &mut PulsarSource<Utf8, String>,
    timeout: Duration,
) -> anyhow::Result<Option<PulsarMessage<Utf8, String>>> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, source.receive()).await {
            Err(_) => return Ok(None),
            Ok(Ok(Receive::Message(message))) => return Ok(Some(message)),
            Ok(Ok(Receive::End)) => anyhow::bail!("Pulsar source ended"),
            Ok(Err(ReceiveError::Retry(_))) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(Err(ReceiveError::Fatal(error))) => {
                anyhow::bail!("Pulsar receive failed: {error:#}")
            }
        }
    }
}

async fn publish_all(topic: &str, values: &[&str]) -> anyhow::Result<()> {
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

/// Requires a broker at `PULSAR_URL` with `transactionCoordinatorEnabled=true`
/// and its admin API at `PULSAR_ADMIN_ADDR`. Records are copied between
/// partitioned topics in transactions, then the output and the input
/// subscription's backlog are checked.
#[tokio::test]
#[ignore = "requires a Pulsar broker with transactions; run with cargo test --features pulsar -- --ignored"]
async fn transactional_pipeline_commits_outputs_with_acknowledgements() -> anyhow::Result<()> {
    let input = unique_topic("beavers-pulsar-tx-in");
    let output = unique_topic("beavers-pulsar-tx-out");
    create_partitioned_topic(&input, 2).await?;
    create_partitioned_topic(&output, 3).await?;
    let subscription = unique_name("beavers-pulsar-tx");

    // Create both subscriptions before publishing, since a new subscription
    // starts at the latest message.
    let mut reader = exclusive_source(&output, &unique_name("beavers-pulsar-reader"));
    assert!(
        next_message(&mut reader, Duration::from_millis(500))
            .await?
            .is_none()
    );
    let mut source = exclusive_source(&input, &subscription);
    assert!(
        next_message(&mut source, Duration::from_millis(500))
            .await?
            .is_none()
    );
    source.close().await?;
    publish_all(&input, &["a", "b", "c", "d"]).await?;

    let sink = PulsarSink::new(PulsarSinkConfig::new(service_url(), &output)).transactional();
    let shutdown = CancellationToken::new();
    let app = tokio::spawn(
        App::new()
            .subscription(uppercase_pipeline(
                exclusive_source(&input, &subscription),
                sink,
            ))
            .run_until(shutdown.clone()),
    );

    let mut values = Vec::new();
    while values.len() < 4 {
        let message = next_message(&mut reader, Duration::from_secs(60))
            .await?
            .expect("timed out waiting for transactional output");
        let record = message.decode()?;
        assert_eq!(
            record.key.as_deref().map(<[u8]>::to_ascii_uppercase),
            record.value.as_ref().map(|value| value.as_bytes().to_vec())
        );
        values.push(record.value.unwrap());
        message.ack().await?;
    }
    values.sort();
    assert_eq!(values, ["A", "B", "C", "D"]);
    shutdown.cancel();
    app.await??;
    reader.close().await?;

    let mut source = exclusive_source(&input, &subscription);
    assert!(
        next_message(&mut source, Duration::from_secs(2))
            .await?
            .is_none(),
        "committed deliveries were redelivered"
    );
    source.close().await
}

/// Requires a broker at `PULSAR_URL` with `transactionCoordinatorEnabled=true`.
/// A transaction whose acknowledgement fails is aborted, so its output never
/// becomes visible and the delivery can be committed again.
#[tokio::test]
#[ignore = "requires a Pulsar broker with transactions; run with cargo test --features pulsar -- --ignored"]
async fn failed_acknowledgement_aborts_the_transaction() -> anyhow::Result<()> {
    let input = unique_topic("beavers-pulsar-abort-in");
    let output = unique_topic("beavers-pulsar-abort-out");
    let subscription = unique_name("beavers-pulsar-abort");
    let mut reader = exclusive_source(&output, &unique_name("beavers-pulsar-reader"));
    assert!(
        next_message(&mut reader, Duration::from_millis(500))
            .await?
            .is_none()
    );
    let mut source = exclusive_source(&input, &subscription);
    assert!(
        next_message(&mut source, Duration::from_millis(500))
            .await?
            .is_none()
    );
    publish_all(&input, &["a"]).await?;

    let sink = PulsarSink::<Utf8, String>::new(PulsarSinkConfig::new(service_url(), &output))
        .transactional();
    let delivery = next_message(&mut source, Duration::from_secs(20))
        .await?
        .expect("timed out waiting for input");
    let prepared = sink.prepare(PulsarPublish::new("aborted".to_owned()))?;
    source.close().await?;
    assert!(sink.commit(&delivery, &[prepared]).await.is_err());
    assert!(
        next_message(&mut reader, Duration::from_secs(2))
            .await?
            .is_none(),
        "aborted output became visible"
    );

    let mut source = exclusive_source(&input, &subscription);
    let delivery = next_message(&mut source, Duration::from_secs(20))
        .await?
        .expect("the aborted delivery was not redelivered");
    let prepared = sink.prepare(PulsarPublish::new("committed".to_owned()))?;
    sink.commit(&delivery, &[prepared]).await?;
    let message = next_message(&mut reader, Duration::from_secs(20))
        .await?
        .expect("timed out waiting for committed output");
    assert_eq!(message.decode()?.value.as_deref(), Some("committed"));
    message.ack().await?;
    sink.close().await?;
    source.close().await?;
    reader.close().await
}

/// Requires the development broker at `PULSAR_URL`.
#[tokio::test]
#[ignore = "requires a Pulsar broker; run with cargo test --features pulsar -- --ignored"]
async fn tombstones_carry_the_null_value_marker() -> anyhow::Result<()> {
    let topic = unique_topic("beavers-pulsar-tombstone");
    let mut config = PulsarSourceConfig::new(service_url(), &topic, unique_name("tombstones"));
    config.empty_payload_is_tombstone = false;
    let mut source = PulsarSource::<Utf8, String>::new(config);
    assert!(
        next_message(&mut source, Duration::from_millis(500))
            .await?
            .is_none()
    );

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
async fn publish_receive_and_ack_against_pulsar() -> anyhow::Result<()> {
    let service_url = env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://127.0.0.1:6650".into());
    let topic = format!(
        "persistent://public/default/{}",
        unique_name("beavers-pulsar-test")
    );
    let subscription = unique_name("beavers-pulsar-subscription");

    let sink = PulsarSink::<Utf8, String>::new(PulsarSinkConfig::new(&service_url, &topic));
    let mut source = PulsarSource::<Utf8, String>::new(PulsarSourceConfig::new(
        &service_url,
        &topic,
        &subscription,
    ));

    // Establish the subscription before publication so a fresh subscription
    // cannot miss the message because its initial position is latest.
    let receive_task = tokio::spawn(async move {
        let message = loop {
            match source.receive().await {
                Ok(Receive::Message(message)) => break message,
                Ok(Receive::End) => anyhow::bail!("Pulsar source ended before delivery"),
                Err(ReceiveError::Retry(_)) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(ReceiveError::Fatal(error)) => {
                    anyhow::bail!("Pulsar receive failed: {error:#}")
                }
            }
        };
        let record = message.decode()?;
        let metadata = record.metadata.clone();
        message.ack().await?;
        source.close().await?;
        anyhow::Ok((record, metadata))
    });
    tokio::time::sleep(Duration::from_millis(250)).await;

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
