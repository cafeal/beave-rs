#![cfg(feature = "pulsar")]

use beavers::{
    Receive, ReceiveError, Sink, Source, SourceMessage, Utf8,
    adapters::pulsar::{
        PulsarAuthentication, PulsarMetadata, PulsarPublish, PulsarRecord, PulsarSink,
        PulsarSinkConfig, PulsarSource, PulsarSourceConfig,
    },
};

#[test]
fn records_keep_delivery_facts_separate_from_application_fields() {
    let record = PulsarRecord {
        value: "order-1".to_owned(),
        key: Some(b"customer-7".to_vec()),
        properties: [("kind".to_owned(), "order".to_owned())].into(),
        event_time: Some(42),
        metadata: PulsarMetadata {
            topic: "persistent://public/default/orders".to_owned(),
            message_id: Default::default(),
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
    assert_eq!(publish.value, "processed");
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
    assert_eq!(record.value, "hello pulsar");
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
