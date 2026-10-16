#![cfg(feature = "kafka")]

use beavers::{
    App, CancellationToken, Distribution, OrderingKey, Receive, ReceiveError, Sink, Source,
    SourceMessage, Subscription, Utf8,
    adapters::kafka::{
        Kafka, KafkaDistribution, KafkaMessage, KafkaPublish, KafkaSource, KafkaTransactionalSink,
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
    let key = Some(b"customer-7".to_vec());
    let headers = vec![("kind".to_owned(), Some(b"new".to_vec()))];
    let publish = KafkaPublish {
        key: key.clone(),
        value: Some("processed order".to_owned()),
        headers: headers.clone(),
    };
    assert_eq!(publish.value.as_deref(), Some("processed order"));
    assert_eq!(publish.key, key);
    assert_eq!(publish.headers, headers);

    let tombstone = KafkaPublish::<String>::tombstone(vec![1, 2, 3]);
    assert_eq!(tombstone.key, Some(vec![1, 2, 3]));
    assert_eq!(tombstone.value, None);

    let kafka = Kafka::new("broker");
    assert!(kafka.sink::<Utf8>("topic").validate().is_ok());
    assert!(kafka.source::<Utf8>("group", ["topic"]).validate().is_ok());
    assert!(Kafka::new("").sink::<Utf8>("topic").validate().is_err());
    assert!(kafka.sink::<Utf8>("").validate().is_err());
    assert!(kafka.source::<Utf8>("group", [""]).validate().is_err());
    assert!(
        kafka
            .source::<Utf8>("group", Vec::<String>::new())
            .validate()
            .is_err()
    );
}

#[test]
fn handle_rejects_a_transactional_id_property() {
    assert!(
        Kafka::new("broker")
            .property("transactional.id", "orders-1")
            .validate()
            .is_err()
    );
    assert!(
        Kafka::new("broker")
            .producer_property("transactional.id", "orders-1")
            .validate()
            .is_err()
    );
}

#[test]
fn graceful_handoff_must_end_before_the_poll_interval() {
    let kafka = Kafka::new("broker");
    let source = || kafka.source::<Utf8>("group", ["topic"]);
    assert!(
        source()
            .graceful_handoff(Duration::from_secs(10))
            .validate()
            .is_ok()
    );
    assert!(
        source()
            .graceful_handoff(Duration::ZERO)
            .validate()
            .is_err()
    );
    assert!(
        source()
            .graceful_handoff(Duration::from_secs(300))
            .validate()
            .is_err()
    );
    assert!(
        source()
            .property("max.poll.interval.ms", "5000")
            .graceful_handoff(Duration::from_secs(10))
            .validate()
            .is_err()
    );
    assert!(
        Kafka::new("broker")
            .property("max.poll.interval.ms", "20000")
            .source::<Utf8>("group", ["topic"])
            .graceful_handoff(Duration::from_secs(10))
            .validate()
            .is_ok()
    );
}

#[test]
fn sink_requires_room_for_a_pending_record() {
    let sink = Kafka::new("broker").sink::<Utf8>("topic").max_pending(0);
    assert!(sink.validate().is_err());
}

#[test]
fn debug_output_hides_property_values() {
    let kafka = Kafka::new("broker").sasl_plain("user", "secret");
    let debug = format!("{kafka:?}");
    assert!(debug.contains("sasl.password"));
    assert!(!debug.contains("secret"));
}

/// librdkafka queues records without a reachable broker, so acceptance and
/// the pending bound are observable offline.
#[tokio::test]
async fn submit_returns_at_acceptance_and_waits_for_pending_capacity() {
    let sink = Kafka::new("127.0.0.1:1")
        .sink::<Utf8>("topic")
        .max_pending(1)
        .close_timeout(Duration::from_millis(100));
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
    let sink = Kafka::new("broker")
        .sink::<Utf8>("topic")
        .transactional(" ");
    let prepared = sink.prepare(KafkaPublish::new("a".to_owned())).unwrap();
    let error = sink.publish(&prepared).await.unwrap_err();
    assert!(error.to_string().contains("transactional ID is required"));
}

fn uppercase_pipeline(
    source: KafkaSource<Utf8>,
    sink: KafkaTransactionalSink<Utf8>,
) -> Subscription<KafkaSource<Utf8>, KafkaTransactionalSink<Utf8>, KafkaPublish<String>> {
    Subscription::new("uppercase", source, sink, |value: String| async move {
        Ok(value.to_uppercase())
    })
    .transactional()
}

#[test]
fn kafka_source_and_transactional_sink_form_a_transactional_pair() {
    let kafka = Kafka::new("broker");
    let source = kafka.source("group", ["in"]);
    let sink = kafka.sink("out").transactional("orders-1");
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

    let kafka = Kafka::new(&brokers);
    let producer = kafka.sink::<Utf8>(&input);
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

    let source = kafka
        .source(&group, [&input])
        .property("auto.offset.reset", "earliest");
    let sink = kafka
        .sink(&output)
        .transactional(unique_name("beavers-kafka-tx"));
    let shutdown = CancellationToken::new();
    let app = tokio::spawn(
        App::new()
            .subscription(uppercase_pipeline(source, sink))
            .run_until(shutdown.clone()),
    );

    let mut reader = kafka
        .source::<Utf8>(unique_name("beavers-reader"), [&output])
        .property("auto.offset.reset", "earliest")
        .property("isolation.level", "read_committed");
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

    let kafka = Kafka::new(&brokers);
    let sink = kafka.sink::<Utf8>(&topic);
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

    let mut source = kafka
        .source::<Utf8>(group, [&topic])
        .property("auto.offset.reset", "earliest");
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
    let sink = Kafka::new(&brokers)
        .sink::<Utf8>(unique_name("beavers-kafka-submit"))
        .max_pending(2);
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

/// Publishes `values` with one key, so they land in one partition in order
/// whatever the topic's partition count.
async fn publish_texts(kafka: &Kafka, topic: &str, values: &[&str]) {
    let sink = kafka.sink::<Utf8>(topic);
    for value in values {
        let mut output = KafkaPublish::new((*value).to_owned());
        output.key = Some(b"same-partition".to_vec());
        let prepared = sink.prepare(output).unwrap();
        tokio::time::timeout(Duration::from_secs(20), sink.publish(&prepared))
            .await
            .expect("timed out publishing to Kafka")
            .unwrap();
    }
    sink.close().await.unwrap();
}

/// The next record within `wait`, retrying transient receive errors.
async fn receive_within(
    source: &mut KafkaSource<Utf8>,
    wait: Duration,
) -> Option<KafkaMessage<Utf8>> {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, source.receive()).await {
            Err(_) => return None,
            Ok(Ok(Receive::Message(message))) => return Some(message),
            Ok(Ok(Receive::End)) => panic!("Kafka source ended"),
            Ok(Err(ReceiveError::Retry(_))) => tokio::time::sleep(Duration::from_millis(100)).await,
            Ok(Err(ReceiveError::Fatal(error))) => panic!("Kafka source failed: {error:#}"),
        }
    }
}

/// Requires a reachable development broker. It uses `KAFKA_BROKERS` when set,
/// otherwise `localhost:9092`.
#[tokio::test]
#[ignore = "requires a Kafka broker; run with cargo test --features kafka -- --ignored"]
async fn a_paused_partition_receives_nothing_until_resumed() {
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let topic = unique_name("beavers-kafka-pause");
    let kafka = Kafka::new(&brokers);
    publish_texts(&kafka, &topic, &["first"]).await;

    let mut source = kafka
        .source::<Utf8>(unique_name("beavers-kafka-pause-group"), [&topic])
        .property("auto.offset.reset", "earliest");
    let first = receive_within(&mut source, Duration::from_secs(20))
        .await
        .expect("timed out waiting for the first record");
    let first_record = first.decode().unwrap().metadata;
    let key = first.ordering_key().unwrap();
    assert_eq!(
        key,
        OrderingKey::new(topic.as_str(), i64::from(first_record.partition))
    );
    assert!(source.pause(&key));
    publish_texts(&kafka, &topic, &["second"]).await;
    if let Some(message) = receive_within(&mut source, Duration::from_secs(3)).await {
        panic!(
            "a paused partition delivered {:?}",
            message.decode().unwrap()
        );
    }

    source.resume(&key);
    let second = receive_within(&mut source, Duration::from_secs(20))
        .await
        .expect("timed out waiting for the record after resuming");
    let second_record = second.decode().unwrap();
    assert_eq!(second_record.value.as_deref(), Some("second"));
    assert_eq!(second_record.metadata.partition, first_record.partition);
    assert_eq!(second_record.metadata.offset, first_record.offset + 1);
    first.ack().await.unwrap();
    second.ack().await.unwrap();
    source.close().await.unwrap();
}

/// Requires a reachable development broker. It uses `KAFKA_BROKERS` when set,
/// otherwise `localhost:9092`.
#[tokio::test]
#[ignore = "requires a Kafka broker; run with cargo test --features kafka -- --ignored"]
async fn distributions_key_records_by_partition_or_message_key() {
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let topic = unique_name("beavers-kafka-distribution");
    let kafka = Kafka::new(&brokers);
    let sink = kafka.sink::<Utf8>(&topic);
    for key in [Some(b"customer-7".to_vec()), None] {
        let mut output = KafkaPublish::new("order".to_owned());
        output.key = key;
        let prepared = sink.prepare(output).unwrap();
        tokio::time::timeout(Duration::from_secs(20), sink.publish(&prepared))
            .await
            .expect("timed out publishing to Kafka")
            .unwrap();
    }
    sink.close().await.unwrap();

    let mut source = kafka
        .source::<Utf8>(unique_name("beavers-kafka-distribution-group"), [&topic])
        .property("auto.offset.reset", "earliest");
    for _ in 0..2 {
        let message = receive_within(&mut source, Duration::from_secs(20))
            .await
            .expect("timed out waiting for a record");
        let record = message.decode().unwrap();
        let partition = OrderingKey::new(topic.as_str(), i64::from(record.metadata.partition));
        assert_eq!(
            KafkaDistribution::Partition.key(&message, &record),
            Some(partition.clone())
        );
        let by_key = match &record.key {
            Some(key) => partition.with_key(key.as_slice()),
            None => partition,
        };
        assert_eq!(
            KafkaDistribution::MessageKey.key(&message, &record),
            Some(by_key)
        );
        message.ack().await.unwrap();
    }
    source.close().await.unwrap();
}

/// Requires a reachable development broker. It uses `KAFKA_BROKERS` when set,
/// otherwise `localhost:9092`. A second consumer joining the group revokes the
/// first consumer's partition while one of its records is unacknowledged; the
/// graceful handoff lets that acknowledgement commit before the revoke.
#[tokio::test]
#[ignore = "requires a Kafka broker; run with cargo test --features kafka -- --ignored"]
async fn graceful_handoff_commits_a_delivery_acknowledged_during_the_revoke() {
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let topic = unique_name("beavers-kafka-handoff");
    let group = unique_name("beavers-kafka-handoff-group");
    let kafka = Kafka::new(&brokers);
    publish_texts(&kafka, &topic, &["first", "second"]).await;

    let source = {
        let (kafka, group, topic) = (kafka.clone(), group.clone(), topic.clone());
        move || {
            kafka
                .source::<Utf8>(group.clone(), [&topic])
                .property("auto.offset.reset", "earliest")
                .graceful_handoff(Duration::from_secs(20))
        }
    };
    let mut first = source();
    let message = receive_within(&mut first, Duration::from_secs(20))
        .await
        .expect("timed out waiting for the first record");
    let handed_off = message.decode().unwrap().metadata;
    let handoff = message.handoff().unwrap();
    let revoked = message.revocation().unwrap();

    // Joining the group starts a rebalance that revokes the first consumer's
    // partition; receiving drives the second consumer's join.
    let joining = tokio::spawn(async move {
        let mut second = source();
        let received = receive_within(&mut second, Duration::from_secs(40)).await;
        let position = received.as_ref().map(|message| {
            let metadata = message.decode().unwrap().metadata;
            (metadata.partition, metadata.offset)
        });
        drop(received);
        second.close().await.unwrap();
        position
    });
    tokio::time::timeout(Duration::from_secs(30), handoff.cancelled())
        .await
        .expect("the partition was not handed off");
    assert!(!revoked.is_cancelled());
    message.ack().await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), revoked.cancelled())
        .await
        .expect("the handoff did not end after the acknowledgement");

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group)
        .create()
        .unwrap();
    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(&topic, handed_off.partition);
    let committed = consumer
        .committed_offsets(partitions, Duration::from_secs(10))
        .unwrap();
    let offset = committed.elements()[0].offset();
    assert!(
        offset == Offset::Offset(handed_off.offset + 1)
            || offset == Offset::Offset(handed_off.offset + 2),
        "the acknowledged record was not committed: {offset:?}"
    );
    // Whichever consumer owns the partition next starts after the record.
    let next = joining.await.unwrap();
    assert_ne!(next, Some((handed_off.partition, handed_off.offset)));
    first.close().await.unwrap();
}

/// Requires a reachable development broker. It uses `KAFKA_BROKERS` when set,
/// otherwise `localhost:9092`. The probe reads the connection state from
/// librdkafka statistics, which arrive while the source polls its consumer.
#[tokio::test]
#[ignore = "requires a Kafka broker; run with cargo test --features kafka -- --ignored"]
async fn probe_reports_the_connection() {
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let topic = unique_name("beavers-kafka-probe");
    let kafka = Kafka::new(&brokers);
    let sink = kafka.sink::<Utf8>(&topic);
    let prepared = sink.prepare(KafkaPublish::new("a".to_owned())).unwrap();
    tokio::time::timeout(Duration::from_secs(20), sink.publish(&prepared))
        .await
        .expect("timed out publishing to Kafka")
        .unwrap();

    let group = unique_name("beavers-kafka-probe");
    let mut source = kafka
        .source::<Utf8>(&group, [&topic])
        .property("auto.offset.reset", "earliest")
        .property("statistics.interval.ms", "200");
    let probe = source.probe().expect("the Kafka source reports a probe");
    assert_eq!(probe.connected(), None);
    let message = loop {
        match tokio::time::timeout(Duration::from_secs(20), source.receive())
            .await
            .expect("timed out waiting for the Kafka record")
        {
            Ok(Receive::Message(message)) => break message,
            Ok(Receive::End) => panic!("Kafka source ended"),
            Err(ReceiveError::Retry(_)) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(ReceiveError::Fatal(error)) => panic!("Kafka source failed: {error:#}"),
        }
    };
    let metadata = message.decode().unwrap().metadata;
    assert_eq!(
        message.messaging_attributes(),
        Some(
            beavers::MessagingAttributes::new("kafka")
                .destination(&topic)
                .partition(metadata.partition.into())
                .kafka_offset(metadata.offset)
                .consumer_group(&group)
        )
    );
    message.ack().await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while probe.connected() != Some(true) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the probe never reported the connection"
        );
        if let Ok(received) =
            tokio::time::timeout(Duration::from_millis(100), source.receive()).await
        {
            panic!("unexpected receive: {:?}", received.map(|_| ()).err());
        }
    }
    source.close().await.unwrap();
    assert_eq!(probe.connected(), None);
    sink.close().await.unwrap();
}
