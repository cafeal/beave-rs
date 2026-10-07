use beavers::{
    App, Emit, InMemorySink, IterSource, PropagationCarrier, Result, Subscription, Tombstones,
    adapters::kafka::{KafkaInherit, KafkaMetadata, KafkaPublish, KafkaRecord},
};
use std::sync::atomic::Ordering;

type Header = (String, Option<Vec<u8>>);

fn header(name: &str, value: &str) -> Header {
    (name.to_owned(), Some(value.as_bytes().to_vec()))
}

fn record() -> KafkaRecord<String> {
    KafkaRecord {
        key: Some(b"customer-7".to_vec()),
        value: Some("order".to_owned()),
        headers: vec![header("trace", "abc"), header("kind", "new")],
        metadata: KafkaMetadata {
            topic: "orders".into(),
            partition: 3,
            offset: 42,
            timestamp: Some(1_000),
        },
    }
}

#[tokio::test]
async fn inherits_key_and_headers_for_every_emitted_record() {
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new_emitting(
                "inherits_key_and_headers_for_every_emitted_record",
                IterSource::new([record()]),
                sink.clone(),
                |record: KafkaRecord<String>| async move {
                    let value = record.value.unwrap_or_default();
                    let mut explicit = KafkaPublish::new(format!("{value}-audit"));
                    explicit.key = Some(b"audit".to_vec());
                    explicit.headers = vec![header("kind", "audit")];
                    Ok(Emit::Many(vec![KafkaPublish::new(value), explicit]))
                },
            )
            .middleware(KafkaInherit::new()),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(
        sink.values(),
        vec![
            KafkaPublish {
                key: Some(b"customer-7".to_vec()),
                value: Some("order".to_owned()),
                headers: vec![header("trace", "abc"), header("kind", "new")],
            },
            KafkaPublish {
                key: Some(b"audit".to_vec()),
                value: Some("order-audit".to_owned()),
                headers: vec![header("trace", "abc"), header("kind", "audit")],
            },
        ]
    );
}

#[tokio::test]
async fn disabled_fields_are_not_inherited() {
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                "disabled_fields_are_not_inherited",
                IterSource::new([record()]),
                sink.clone(),
                |record: KafkaRecord<String>| async move {
                    Ok(KafkaPublish::new(record.value.unwrap_or_default()))
                },
            )
            .middleware(KafkaInherit::new().without_key().without_headers()),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![KafkaPublish::new("order".to_owned())]);
}

#[tokio::test]
async fn value_handlers_inherit_kafka_metadata_by_default() {
    let sink = InMemorySink::<KafkaPublish<_>>::default();
    App::new()
        .subscription(Subscription::new_emitting(
            "value_handlers_inherit_kafka_metadata_by_default",
            IterSource::new([record()]),
            sink.clone(),
            |value: String| async move { Ok(Emit::Many(vec![value.len(), 0])) },
        ))
        .run()
        .await
        .unwrap();
    let headers = vec![header("trace", "abc"), header("kind", "new")];
    assert_eq!(
        sink.values(),
        vec![
            KafkaPublish {
                key: Some(b"customer-7".to_vec()),
                value: Some(5),
                headers: headers.clone(),
            },
            KafkaPublish {
                key: Some(b"customer-7".to_vec()),
                value: Some(0),
                headers,
            },
        ]
    );
}

#[tokio::test]
async fn subscribe_selects_the_handler_shape_from_its_signature() {
    async fn by_value(value: String) -> Result<String> {
        Ok(value.to_uppercase())
    }
    async fn record_to_value(record: KafkaRecord<String>) -> Result<String> {
        Ok(format!(
            "{}@{}",
            record.value.unwrap_or_default(),
            record.metadata.offset
        ))
    }
    async fn value_to_publish(value: String) -> Result<KafkaPublish<String>> {
        let mut output = KafkaPublish::new(value);
        output.key = Some(b"explicit".to_vec());
        Ok(output)
    }
    async fn by_record(record: KafkaRecord<String>) -> Result<KafkaPublish<String>> {
        Ok(KafkaPublish::new(record.value.unwrap_or_default()))
    }

    let sinks: [InMemorySink<KafkaPublish<String>>; 4] = Default::default();
    App::new()
        .subscribe(
            "by_value",
            IterSource::new([record()]),
            sinks[0].clone(),
            by_value,
        )
        .subscribe(
            "record_to_value",
            IterSource::new([record()]),
            sinks[1].clone(),
            record_to_value,
        )
        .subscribe(
            "value_to_publish",
            IterSource::new([record()]),
            sinks[2].clone(),
            value_to_publish,
        )
        .subscribe(
            "by_record",
            IterSource::new([record()]),
            sinks[3].clone(),
            by_record,
        )
        .run()
        .await
        .unwrap();

    let inherited = |value: &str| KafkaPublish {
        key: Some(b"customer-7".to_vec()),
        value: Some(value.to_owned()),
        headers: vec![header("trace", "abc"), header("kind", "new")],
    };
    // Plain output values inherit the platform's default metadata.
    assert_eq!(sinks[0].values(), vec![inherited("ORDER")]);
    assert_eq!(sinks[1].values(), vec![inherited("order@42")]);
    // Publish records are published as returned.
    let mut explicit = KafkaPublish::new("order".to_owned());
    explicit.key = Some(b"explicit".to_vec());
    assert_eq!(sinks[2].values(), vec![explicit]);
    assert_eq!(
        sinks[3].values(),
        vec![KafkaPublish::new("order".to_owned())]
    );
}

#[tokio::test]
async fn value_outputs_can_be_sent_to_a_value_sink() {
    let sink = InMemorySink::<usize>::default();
    App::new()
        .subscribe(
            "value_outputs_can_be_sent_to_a_value_sink",
            IterSource::new([record()]),
            sink.clone(),
            |value: String| async move { Ok(value.len()) },
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![5]);
}

#[tokio::test]
async fn value_handlers_reject_null_kafka_values() {
    let mut tombstone = record();
    tombstone.value = None;
    let source = IterSource::new([tombstone.clone()]);
    let acks = source.acknowledgements();
    let sink = InMemorySink::<KafkaPublish<String>>::default();
    let dlq = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                "value_handlers_reject_null_kafka_values",
                source,
                sink.clone(),
                |value: String| async move { Ok(value) },
            )
            .dlq_with(dlq.clone(), |dead_letter| Ok(dead_letter.input.unwrap())),
        )
        .run()
        .await
        .unwrap();
    assert!(sink.values().is_empty());
    assert_eq!(dlq.values(), vec![tombstone]);
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn tombstone_policies_run_before_the_value_handler() {
    let mut tombstone = record();
    tombstone.value = None;
    let mut keyless = tombstone.clone();
    keyless.key = None;
    let source = IterSource::new([tombstone, record(), keyless.clone()]);
    let acks = source.acknowledgements();
    let sink = InMemorySink::<KafkaPublish<_>>::default();
    let dlq = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                "tombstone_policies_run_before_the_value_handler",
                source,
                sink.clone(),
                |value: String| async move { Ok(value.len()) },
            )
            .middleware(Tombstones::propagate())
            .dlq_with(dlq.clone(), |dead_letter| Ok(dead_letter.input.unwrap())),
        )
        .run()
        .await
        .unwrap();
    let headers = vec![header("trace", "abc"), header("kind", "new")];
    assert_eq!(
        sink.values(),
        vec![
            KafkaPublish {
                key: Some(b"customer-7".to_vec()),
                value: None,
                headers: headers.clone(),
            },
            KafkaPublish {
                key: Some(b"customer-7".to_vec()),
                value: Some(5),
                headers,
            },
        ]
    );
    assert_eq!(dlq.values(), vec![keyless]);
    assert_eq!(acks.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn skipped_tombstones_are_acknowledged_without_output() {
    let mut tombstone = record();
    tombstone.value = None;
    let source = IterSource::new([tombstone]);
    let acks = source.acknowledgements();
    let sink = InMemorySink::<KafkaPublish<String>>::default();
    App::new()
        .subscription(
            Subscription::new(
                "skipped_tombstones_are_acknowledged_without_output",
                source,
                sink.clone(),
                |value: String| async move { Ok(value) },
            )
            .middleware(Tombstones::skip()),
        )
        .run()
        .await
        .unwrap();
    assert!(sink.values().is_empty());
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}

#[test]
fn propagation_fields_replace_every_header_of_that_name() {
    let mut output = KafkaPublish::new("order".to_owned());
    output.headers = vec![
        header("traceparent", "inherited"),
        header("kind", "new"),
        header("traceparent", "duplicate"),
    ];
    output.set_propagation_field("traceparent", "current".into());
    assert_eq!(
        output.headers,
        vec![header("kind", "new"), header("traceparent", "current")]
    );
}
