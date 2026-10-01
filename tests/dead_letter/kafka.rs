use anyhow::Context;
use beavers::{
    App, ChannelRaw, DeadLetter, ErrorPolicy, FailureKind, HandlerError, InMemorySink, Json,
    Result, Subscription,
    adapters::kafka::{KafkaDeadLetter, KafkaInherit, KafkaMetadata, KafkaPublish, KafkaRecord},
    channel,
    testing::{KafkaTestSource, kafka_record},
};

type Header = (String, Option<Vec<u8>>);

fn header(name: &str, value: &str) -> Header {
    (name.to_owned(), Some(value.as_bytes().to_vec()))
}

fn order(offset: i64, value: &str) -> KafkaRecord<Vec<u8>> {
    let mut record = kafka_record("orders", 3, offset, value);
    record.key = Some(b"customer-7".to_vec());
    record.headers = vec![header("traceparent", "00-trace")];
    record.metadata.timestamp = Some(1_000);
    record
}

/// The record a consumer of the dead-letter topic receives for `publish`.
fn received(publish: KafkaPublish<Vec<u8>>, offset: i64) -> KafkaRecord<Vec<u8>> {
    KafkaRecord {
        key: publish.key,
        value: publish.value,
        headers: publish.headers,
        metadata: KafkaMetadata {
            topic: "orders-dlq".into(),
            partition: 0,
            offset,
            timestamp: Some(5_000),
        },
    }
}

async fn reject_odd(record: KafkaRecord<u32>) -> Result<u32> {
    match record.value {
        Some(value) if value % 2 == 0 => Ok(value),
        _ => Err(HandlerError::Reject(anyhow::anyhow!("odd value"))),
    }
}

/// Runs `reject_odd` over `records` and returns the dead-letter records.
async fn dead_letter(name: &str, records: Vec<KafkaRecord<Vec<u8>>>) -> Vec<KafkaPublish<Vec<u8>>> {
    let dlq = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                name,
                KafkaTestSource::<Json, u32>::new(records),
                InMemorySink::default(),
                reject_odd,
            )
            .dlq_with(dlq.clone(), |dead| Ok(KafkaPublish::from_dead_letter(dead)))
            .error_policy(ErrorPolicy::dead_letter_all()),
        )
        .run()
        .await
        .unwrap();
    dlq.values()
}

#[tokio::test]
async fn dead_letters_keep_the_original_record_and_add_failure_headers() {
    let published = dead_letter("orders", vec![order(7, "3"), order(8, "x")]).await;

    assert_eq!(
        published[0],
        KafkaPublish {
            key: Some(b"customer-7".to_vec()),
            value: Some(b"3".to_vec()),
            headers: vec![
                header("traceparent", "00-trace"),
                header("beavers-dlq-subscription", "orders"),
                header("beavers-dlq-failure", "rejected"),
                header("beavers-dlq-error", "odd value"),
                header("beavers-dlq-attempts", "1"),
                header("beavers-dlq-count", "1"),
                header("beavers-dlq-origin-topic", "orders"),
                header("beavers-dlq-origin-partition", "3"),
                header("beavers-dlq-origin-offset", "7"),
                header("beavers-dlq-origin-timestamp", "1000"),
            ],
        }
    );
    let decode = KafkaDeadLetter::from_record(&received(published[1].clone(), 1))
        .unwrap()
        .unwrap();
    assert_eq!(decode.details.failure, FailureKind::Decode);
    assert_eq!(decode.details.attempts, 0);
    assert_eq!(decode.origin.offset, 8);
}

#[tokio::test]
async fn dead_lettering_a_dead_letter_keeps_its_origin_and_counts_again() {
    let first = dead_letter("orders", vec![order(7, "3")]).await;
    let second = dead_letter("orders-redrive", vec![received(first[0].clone(), 0)]).await;

    let parsed = KafkaDeadLetter::from_record(&received(second[0].clone(), 1))
        .unwrap()
        .unwrap();
    assert_eq!(parsed.details.subscription, "orders-redrive");
    assert_eq!(parsed.details.count, 2);
    assert_eq!(parsed.origin, order(7, "3").metadata);
    let failure_headers = second[0]
        .headers
        .iter()
        .filter(|(name, _)| name == "beavers-dlq-failure")
        .count();
    assert_eq!(failure_headers, 1);
    assert_eq!(second[0].headers[0], header("traceparent", "00-trace"));
}

#[test]
fn records_without_dead_letter_headers_are_not_dead_letters() {
    assert_eq!(KafkaDeadLetter::from_record(&order(7, "3")).unwrap(), None);
}

#[tokio::test]
async fn malformed_dead_letter_headers_are_errors_and_get_replaced() {
    let mut record = received(
        dead_letter("orders", vec![order(7, "3")]).await.remove(0),
        0,
    );
    for (name, value) in &mut record.headers {
        if name == "beavers-dlq-count" {
            *value = Some(b"many".to_vec());
        }
    }
    assert!(KafkaDeadLetter::from_record(&record).is_err());

    let replaced = dead_letter("orders-redrive", vec![record]).await;
    let parsed = KafkaDeadLetter::from_record(&received(replaced[0].clone(), 1))
        .unwrap()
        .unwrap();
    assert_eq!(parsed.details.count, 1);
    assert_eq!(parsed.origin.topic, "orders-dlq");
}

#[tokio::test]
async fn inheritance_skips_dead_letter_headers() {
    let dead = dead_letter("orders", vec![order(7, "3")]).await;
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                "orders-redrive",
                KafkaTestSource::<Json, u32>::new([received(dead[0].clone(), 0)]),
                sink.clone(),
                |record: KafkaRecord<u32>| async move {
                    Ok(KafkaPublish::new(record.value.unwrap_or_default() + 1))
                },
            )
            .middleware(KafkaInherit::new()),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(
        sink.values(),
        [KafkaPublish {
            key: Some(b"customer-7".to_vec()),
            value: Some(4),
            headers: vec![header("traceparent", "00-trace")],
        }]
    );
}

#[tokio::test]
async fn chained_dead_letters_forward_the_first_upstream_record() {
    let (to_score, fetched) = channel(4);
    let dlq = InMemorySink::default();
    App::new()
        .subscribe(
            "fetch",
            KafkaTestSource::<Json, u32>::new([order(7, "3")]),
            to_score,
            |record: KafkaRecord<u32>| async move { Ok(record.value.unwrap_or_default()) },
        )
        .subscription(
            Subscription::new(
                "score",
                fetched,
                InMemorySink::default(),
                |_: u32| async move { Err::<u32, _>(HandlerError::Reject(anyhow::anyhow!("odd"))) },
            )
            .dlq_with(dlq.clone(), |dead: DeadLetter<u32, ChannelRaw>| {
                let dead = dead.try_map_raw(|raw| {
                    raw.downcast_ref::<KafkaRecord<Vec<u8>>>()
                        .cloned()
                        .context("no Kafka record")
                })?;
                Ok(KafkaPublish::from_dead_letter(dead))
            }),
        )
        .run()
        .await
        .unwrap();

    let parsed = KafkaDeadLetter::from_record(&received(dlq.values().remove(0), 0))
        .unwrap()
        .unwrap();
    assert_eq!(parsed.details.subscription, "score");
    assert_eq!(parsed.origin, order(7, "3").metadata);
}
