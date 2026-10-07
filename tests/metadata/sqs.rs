use beavers::{
    App, InMemorySink, IterSource, PropagationCarrier, Subscription,
    adapters::sqs::{
        SqsAttributeValue, SqsAttributes, SqsInherit, SqsMetadata, SqsPublish, SqsRecord,
    },
};

fn record() -> SqsRecord<String> {
    SqsRecord {
        value: "order".to_owned(),
        attributes: SqsAttributes::from([
            ("trace".to_owned(), SqsAttributeValue::from("abc")),
            ("kind".to_owned(), SqsAttributeValue::from("new")),
        ]),
        metadata: SqsMetadata {
            queue_url: "https://sqs/123/orders.fifo".into(),
            message_id: "m-1".into(),
            receive_count: 1,
            sent_timestamp: Some(1),
            first_receive_timestamp: Some(2),
            message_group_id: Some("customer-7".into()),
            deduplication_id: Some("d-1".into()),
            sequence_number: Some("10".into()),
        },
    }
}

#[tokio::test]
async fn inherits_attributes_without_overriding_explicit_ones() {
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                "inherits_attributes_without_overriding_explicit_ones",
                IterSource::new([record()]),
                sink.clone(),
                |record: SqsRecord<String>| async move {
                    let mut output = SqsPublish::new(record.value);
                    output
                        .attributes
                        .insert("kind".into(), SqsAttributeValue::from("processed"));
                    Ok(output)
                },
            )
            .middleware(SqsInherit::new()),
        )
        .run()
        .await
        .unwrap();
    let mut expected = SqsPublish::new("order".to_owned());
    expected.attributes = SqsAttributes::from([
        ("trace".to_owned(), SqsAttributeValue::from("abc")),
        ("kind".to_owned(), SqsAttributeValue::from("processed")),
    ]);
    assert_eq!(sink.values(), vec![expected]);
}

#[tokio::test]
async fn the_message_group_is_inherited_only_when_enabled() {
    for (inherit, group) in [
        (SqsInherit::new(), None),
        (
            SqsInherit::new().without_attributes().with_message_group(),
            Some("customer-7"),
        ),
    ] {
        let sink = InMemorySink::default();
        App::new()
            .subscription(
                Subscription::new(
                    "the_message_group_is_inherited_only_when_enabled",
                    IterSource::new([record()]),
                    sink.clone(),
                    |record: SqsRecord<String>| async move { Ok(SqsPublish::new(record.value)) },
                )
                .middleware(inherit),
            )
            .run()
            .await
            .unwrap();
        let outputs: Vec<SqsPublish<String>> = sink.values();
        assert_eq!(outputs[0].message_group_id.as_deref(), group);
        assert_eq!(outputs[0].deduplication_id, None);
    }
}

#[tokio::test]
async fn value_handlers_inherit_sqs_attributes_by_default() {
    let sink = InMemorySink::<SqsPublish<_>>::default();
    App::new()
        .subscription(Subscription::new(
            "value_handlers_inherit_sqs_attributes_by_default",
            IterSource::new([record()]),
            sink.clone(),
            |value: String| async move { Ok(value.to_uppercase()) },
        ))
        .run()
        .await
        .unwrap();
    let mut expected = SqsPublish::new("ORDER".to_owned());
    expected.attributes = record().attributes;
    assert_eq!(sink.values(), vec![expected]);
}

#[test]
fn propagation_fields_replace_attributes() {
    let mut output = SqsPublish::new("order".to_owned());
    output.attributes.insert(
        "traceparent".into(),
        SqsAttributeValue::Binary(b"old".to_vec()),
    );
    output.set_propagation_field("traceparent", "current".into());
    assert_eq!(
        output.attributes["traceparent"],
        SqsAttributeValue::from("current")
    );
    assert_eq!(output.attributes.len(), 1);
}
