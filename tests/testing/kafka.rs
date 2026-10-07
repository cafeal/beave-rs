use beavers::{
    App, DeadLetter, ErrorPolicy, FailureKind, HandlerError, InMemorySink, Json, OrderingKey,
    Receive, Source, SourceMessage, Subscription,
    adapters::kafka::{KafkaInherit, KafkaMetadata, KafkaPublish, KafkaRecord},
    testing::{KafkaTestSource, kafka_record},
};

type Header = (String, Option<Vec<u8>>);

fn header(name: &str, value: &[u8]) -> Header {
    (name.to_owned(), Some(value.to_vec()))
}

fn order(offset: i64, value: &str) -> KafkaRecord<Vec<u8>> {
    let mut record = kafka_record("orders", 3, offset, value);
    record.key = Some(b"customer-7".to_vec());
    record.headers = vec![
        header("traceparent", b"00-trace"),
        header("binary", &[0xff]),
    ];
    record
}

#[test]
fn kafka_record_fills_the_delivery_location() {
    assert_eq!(
        kafka_record("orders", 3, 42, "7"),
        KafkaRecord {
            key: None,
            value: Some(b"7".to_vec()),
            headers: Vec::new(),
            metadata: KafkaMetadata {
                topic: "orders".into(),
                partition: 3,
                offset: 42,
                timestamp: None,
            },
        }
    );
}

#[tokio::test]
async fn deliveries_follow_the_kafka_adapter() {
    let mut tombstone = order(43, "");
    tombstone.value = None;
    let mut source = KafkaTestSource::<Json, u32>::new([order(42, "7"), tombstone]);

    let Receive::Message(message) = source.receive().await.unwrap() else {
        panic!("expected a delivery");
    };
    let decoded = message.decode().unwrap();
    assert_eq!(decoded.value, Some(7));
    assert_eq!(decoded.key.as_deref(), Some(&b"customer-7"[..]));
    assert_eq!(decoded.metadata.offset, 42);
    assert_eq!(message.raw(), order(42, "7"));
    assert_eq!(message.ordering_key(), Some(OrderingKey::new("orders", 3)));
    assert_eq!(message.propagation_fields(), [("traceparent", "00-trace")]);

    let Receive::Message(message) = source.receive().await.unwrap() else {
        panic!("expected a delivery");
    };
    assert_eq!(message.decode().unwrap().value, None);
}

#[tokio::test]
async fn handlers_and_middleware_see_the_fabricated_metadata() {
    let source = KafkaTestSource::<Json, u32>::new([order(42, "7"), order(43, "8")]);
    let deliveries = source.deliveries();
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                "handlers_and_middleware_see_the_fabricated_metadata",
                source,
                sink.clone(),
                |record: KafkaRecord<u32>| async move {
                    let offset = record.metadata.offset;
                    Ok(KafkaPublish::new(format!(
                        "{offset}:{}",
                        record.value.unwrap()
                    )))
                },
            )
            .middleware(KafkaInherit::new()),
        )
        .run()
        .await
        .unwrap();

    let headers = vec![
        header("traceparent", b"00-trace"),
        header("binary", &[0xff]),
    ];
    assert_eq!(
        sink.values(),
        ["42:7", "43:8"].map(|value| KafkaPublish {
            key: Some(b"customer-7".to_vec()),
            value: Some(value.to_owned()),
            headers: headers.clone(),
        })
    );
    assert_eq!(deliveries.acknowledged(), [order(42, "7"), order(43, "8")]);
}

#[tokio::test]
async fn dead_letters_keep_the_raw_kafka_record() {
    let source =
        KafkaTestSource::<Json, u32>::new([order(42, "not json"), order(43, "5"), order(44, "6")]);
    let deliveries = source.deliveries();
    let sink = InMemorySink::<KafkaPublish<u32>>::default();
    let dlq = InMemorySink::<DeadLetter<KafkaRecord<u32>, KafkaRecord<Vec<u8>>>>::default();
    App::new()
        .subscription(
            Subscription::new(
                "dead_letters_keep_the_raw_kafka_record",
                source,
                sink.clone(),
                |record: KafkaRecord<u32>| async move {
                    let value = record.value.unwrap_or_default();
                    if value % 2 == 1 {
                        return Err(HandlerError::Reject(anyhow::anyhow!("odd")));
                    }
                    Ok(KafkaPublish::new(value))
                },
            )
            .dlq(dlq.clone())
            .error_policy(ErrorPolicy::dead_letter_all()),
        )
        .run()
        .await
        .unwrap();

    assert_eq!(sink.values(), [KafkaPublish::new(6)]);
    let dead_letters = dlq.values();
    let failures: Vec<_> = dead_letters
        .iter()
        .map(|dead| (dead.failure, dead.raw.metadata.offset))
        .collect();
    assert_eq!(
        failures,
        [(FailureKind::Decode, 42), (FailureKind::Rejected, 43)]
    );
    assert_eq!(dead_letters[0].raw, order(42, "not json"));
    assert!(dead_letters[0].input.is_none());
    assert_eq!(dead_letters[1].input.as_ref().unwrap().value, Some(5));
    assert!(deliveries.all_acknowledged());
}
