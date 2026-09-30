#![cfg(feature = "kafka")]

use beavers::{
    App, CancellationToken, Receive, ReceiveError, Sink, Source, SourceMessage, Subscription, Utf8,
    adapters::kafka::{
        KafkaPublish, KafkaRecord, KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig,
        KafkaTransactionalSink,
    },
};
use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    consumer::{BaseConsumer, Consumer},
};
use std::{
    env,
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

#[test]
fn sink_configuration_rejects_a_transactional_id_property() {
    let mut config = KafkaSinkConfig::new("broker", "topic");
    config
        .properties
        .insert("transactional.id".into(), "orders-1".into());
    assert!(config.validate().is_err());
}

#[test]
fn sink_configuration_requires_room_for_a_pending_record() {
    let mut config = KafkaSinkConfig::new("broker", "topic");
    config.max_pending = 0;
    assert!(config.validate().is_err());
}

/// librdkafka queues records without a reachable broker, so acceptance and
/// the pending bound are observable offline.
#[tokio::test]
async fn submit_returns_at_acceptance_and_waits_for_pending_capacity() {
    let mut config = KafkaSinkConfig::new("127.0.0.1:1", "topic");
    config.max_pending = 1;
    config.close_timeout = Duration::from_millis(100);
    let sink = KafkaSink::<Utf8, String>::new(config);
    let prepared = sink.prepare(KafkaPublish::new("a".to_owned())).unwrap();

    let first = tokio::time::timeout(Duration::from_secs(5), sink.submit(&prepared))
        .await
        .expect("submission waited for a delivery report")
        .unwrap();
    assert!(!first.is_done());
    assert!(
        tokio::time::timeout(Duration::from_millis(200), sink.submit(&prepared))
            .await
            .is_err(),
        "a second record was accepted beyond max_pending"
    );

    drop(first);
    let second = tokio::time::timeout(Duration::from_secs(5), sink.submit(&prepared))
        .await
        .expect("an abandoned completion kept its pending slot")
        .unwrap();
    drop(second);
    let _ = sink.close().await;
    assert!(sink.submit(&prepared).await.is_err());
}

#[tokio::test]
async fn transactional_sink_requires_a_transactional_id_before_connecting() {
    let sink =
        KafkaSink::<Utf8, String>::new(KafkaSinkConfig::new("broker", "topic")).transactional(" ");
    let prepared = sink.prepare(KafkaPublish::new("a".to_owned())).unwrap();
    let error = sink.publish(&prepared).await.unwrap_err();
    assert!(error.to_string().contains("transactional ID is required"));
}

fn uppercase_pipeline(
    source: KafkaSource<Utf8, String>,
    sink: KafkaTransactionalSink<Utf8, String>,
) -> Subscription<
    KafkaSource<Utf8, String>,
    KafkaTransactionalSink<Utf8, String>,
    KafkaPublish<String>,
> {
    Subscription::forward("uppercase", source, sink, |value: String| async move {
        Ok(value.to_uppercase())
    })
    .transactional()
}

#[test]
fn kafka_source_and_transactional_sink_form_a_transactional_pair() {
    let source = KafkaSource::new(KafkaSourceConfig::new("broker", "group", ["in"]));
    let sink = KafkaSink::new(KafkaSinkConfig::new("broker", "out")).transactional("orders-1");
    let _ = uppercase_pipeline(source, sink);
}

/// Requires a reachable development broker. It uses `KAFKA_BROKERS` when set,
/// otherwise `localhost:9092`. Records are copied between unique topics in
/// transactions, then the output and the group's committed offset are checked.
#[tokio::test]
#[ignore = "requires a Kafka broker; run with cargo test --features kafka -- --ignored"]
async fn transactional_pipeline_commits_outputs_with_offsets() {
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let input = unique_name("beavers-kafka-tx-in");
    let output = unique_name("beavers-kafka-tx-out");
    let group = unique_name("beavers-kafka-tx-group");

    let producer = KafkaSink::<Utf8, String>::new(KafkaSinkConfig::new(&brokers, &input));
    for value in ["a", "b", "c"] {
        let prepared = producer
            .prepare(KafkaPublish::new(value.to_owned()))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(20), producer.publish(&prepared))
            .await
            .expect("timed out publishing to Kafka")
            .unwrap();
    }
    producer.close().await.unwrap();

    let mut source_config = KafkaSourceConfig::new(&brokers, &group, [&input]);
    source_config
        .properties
        .insert("auto.offset.reset".into(), "earliest".into());
    let sink = KafkaSink::new(KafkaSinkConfig::new(&brokers, &output))
        .transactional(unique_name("beavers-kafka-tx"));
    let shutdown = CancellationToken::new();
    let app = tokio::spawn(
        App::new()
            .subscription(uppercase_pipeline(KafkaSource::new(source_config), sink))
            .run_until(shutdown.clone()),
    );

    let mut reader_config =
        KafkaSourceConfig::new(&brokers, unique_name("beavers-reader"), [&output]);
    reader_config
        .properties
        .insert("auto.offset.reset".into(), "earliest".into());
    reader_config
        .properties
        .insert("isolation.level".into(), "read_committed".into());
    let mut reader = KafkaSource::<Utf8, String>::new(reader_config);
    let mut values = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while values.len() < 3 {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, reader.receive())
            .await
            .expect("timed out waiting for transactional output")
        {
            Ok(Receive::Message(message)) => {
                values.push(message.decode().unwrap().value.unwrap());
                message.ack().await.unwrap();
            }
            Ok(Receive::End) => panic!("Kafka reader ended"),
            Err(ReceiveError::Retry(_)) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(ReceiveError::Fatal(error)) => panic!("Kafka reader failed: {error:#}"),
        }
    }
    values.sort();
    assert_eq!(values, ["A", "B", "C"]);
    reader.close().await.unwrap();
    shutdown.cancel();
    app.await.unwrap().unwrap();

    let offsets = tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", &group)
            .create()
            .unwrap();
        let metadata = consumer
            .fetch_metadata(Some(&input), Duration::from_secs(20))
            .unwrap();
        let mut partitions = TopicPartitionList::new();
        for partition in metadata.topics()[0].partitions() {
            partitions.add_partition(&input, partition.id());
        }
        consumer
            .committed_offsets(partitions, Duration::from_secs(20))
            .unwrap()
    })
    .await
    .unwrap();
    // The auto-created input topic can have several partitions, so the
    // committed offsets together must cover all three input records.
    let committed: i64 = offsets
        .elements()
        .iter()
        .map(|element| match element.offset() {
            Offset::Offset(offset) => offset,
            Offset::Invalid => 0,
            other => panic!("unexpected committed offset {other:?}"),
        })
        .sum();
    assert_eq!(committed, 3);
}

/// Requires a reachable development broker. It uses `KAFKA_BROKERS` when set,
/// otherwise `localhost:9092`, and creates a unique auto-created topic/group.
#[tokio::test]
#[ignore = "requires a Kafka broker; run with cargo test --features kafka -- --ignored"]
async fn publish_receive_and_ack_against_kafka() {
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
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

/// Requires a reachable development broker. It uses `KAFKA_BROKERS` when set,
/// otherwise `localhost:9092`.
#[tokio::test]
#[ignore = "requires a Kafka broker; run with cargo test --features kafka -- --ignored"]
async fn submitted_records_complete_on_their_delivery_reports() {
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let mut config = KafkaSinkConfig::new(&brokers, unique_name("beavers-kafka-submit"));
    config.max_pending = 2;
    let sink = KafkaSink::<Utf8, String>::new(config);
    let prepared = sink.prepare(KafkaPublish::new("a".to_owned())).unwrap();

    let first = sink.submit(&prepared).await.unwrap();
    let second = sink.submit(&prepared).await.unwrap();
    assert!(!first.is_done() && !second.is_done());
    assert!(
        tokio::time::timeout(Duration::from_millis(500), sink.submit(&prepared))
            .await
            .is_err(),
        "a third record was accepted beyond max_pending"
    );
    for completion in [first, second] {
        tokio::time::timeout(Duration::from_secs(20), completion.wait())
            .await
            .expect("timed out waiting for a delivery report")
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(20), sink.publish(&prepared))
        .await
        .expect("timed out publishing after the completions freed their slots")
        .unwrap();
    sink.close().await.unwrap();
}
