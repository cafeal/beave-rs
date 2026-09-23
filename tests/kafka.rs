#![cfg(feature = "kafka")]

use beavers::{
    Receive, ReceiveError, Sink, Source, SourceMessage, Utf8,
    adapters::kafka::{
        KafkaPublish, KafkaRecord, KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig,
    },
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos}-{sequence}")
}

#[test]
fn records_and_publishes_keep_delivery_metadata_separate() {
    let metadata = beavers::adapters::kafka::KafkaMetadata {
        topic: "orders".into(),
        partition: 0,
        offset: 1,
        timestamp: None,
    };
    let record = KafkaRecord {
        key: Some(b"customer-7".to_vec()),
        value: Some("order".to_owned()),
        headers: vec![("kind".into(), Some(b"new".to_vec()))],
        metadata,
    };
    assert_eq!(record.metadata().topic, "orders");
    assert_eq!(record.metadata().offset, 1);
    assert_eq!(record.key.as_deref(), Some(b"customer-7".as_slice()));

    let publish = KafkaPublish {
        key: record.key.clone(),
        value: Some("processed order".to_owned()),
        headers: record.headers.clone(),
    };
    assert_eq!(publish.value.as_deref(), Some("processed order"));
    assert_eq!(publish.key, record.key);
    assert_eq!(publish.headers, record.headers);

    let tombstone = KafkaPublish::<String>::tombstone(vec![1, 2, 3]);
    assert_eq!(tombstone.key, Some(vec![1, 2, 3]));
    assert_eq!(tombstone.value, None);

    assert!(KafkaSinkConfig::new("", "topic").validate().is_err());
    assert!(
        KafkaSourceConfig::new("broker", "group", [""])
            .validate()
            .is_err()
    );
}

/// Requires a reachable development broker. It uses `KAFKA_BROKERS` when set,
/// otherwise `localhost:9092`, and creates a unique auto-created topic/group.
#[tokio::test]
#[ignore = "requires a Kafka broker; run with cargo test --features kafka -- --ignored"]
async fn publish_receive_and_ack_against_kafka() {
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let topic = unique_name("beavers-kafka-test");
    let group = unique_name("beavers-kafka-group");

    let sink = KafkaSink::<Utf8, String>::new(KafkaSinkConfig::new(&brokers, &topic));
    let mut output = KafkaPublish::new("hello kafka".to_owned());
    output.key = Some(b"test-key".to_vec());
    output.headers.push(("kind".into(), Some(b"test".to_vec())));
    tokio::time::timeout(
        Duration::from_secs(20),
        sink.publish(&sink.prepare(output).unwrap()),
    )
    .await
    .expect("timed out publishing to Kafka")
    .unwrap();

    let mut source_config = KafkaSourceConfig::new(&brokers, group, [&topic]);
    source_config
        .properties
        .insert("auto.offset.reset".into(), "earliest".into());
    let mut source = KafkaSource::<Utf8, String>::new(source_config);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let message = loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let received = tokio::time::timeout(remaining, source.receive())
            .await
            .expect("timed out waiting for Kafka record");
        match received {
            Ok(Receive::Message(message)) => break message,
            Ok(Receive::End) => {
                panic!("Kafka source ended before it received the published record")
            }
            Err(ReceiveError::Retry(_)) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(ReceiveError::Fatal(error)) => panic!("Kafka source failed: {error:#}"),
        }
    };
    let record = message.decode().unwrap();
    assert_eq!(record.key.as_deref(), Some(b"test-key".as_slice()));
    assert_eq!(record.value.as_deref(), Some("hello kafka"));
    assert_eq!(
        record.headers,
        vec![("kind".into(), Some(b"test".to_vec()))]
    );
    assert_eq!(record.metadata.topic, topic);
    message.ack().await.unwrap();
    source.close().await.unwrap();
    sink.close().await.unwrap();
}
