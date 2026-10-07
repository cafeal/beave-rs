use beavers::{
    App, InMemorySink, IterSource, PropagationCarrier, Subscription,
    adapters::rabbitmq::{
        RabbitMqHeaders, RabbitMqInherit, RabbitMqMetadata, RabbitMqProperties, RabbitMqPublish,
        RabbitMqRecord, RabbitMqValue,
    },
};

fn record() -> RabbitMqRecord<String> {
    RabbitMqRecord {
        value: "order".to_owned(),
        headers: RabbitMqHeaders::from([
            ("trace".to_owned(), RabbitMqValue::from("abc")),
            ("kind".to_owned(), RabbitMqValue::from("new")),
        ]),
        properties: RabbitMqProperties {
            content_type: Some("text/plain".into()),
            correlation_id: Some("c-1".into()),
            priority: Some(4),
            message_id: Some("m-1".into()),
            timestamp: Some(1_700_000_000),
            expiration: Some("60000".into()),
            reply_to: Some("replies".into()),
            ..RabbitMqProperties::default()
        },
        metadata: RabbitMqMetadata {
            queue: "orders".into(),
            exchange: "shop".into(),
            routing_key: "order.created".into(),
            redelivered: false,
            persistent: true,
            delivery_tag: 1,
        },
    }
}

/// The output `RabbitMqInherit::new()` produces from `record()` for `value`.
fn inherited(value: &str) -> RabbitMqPublish<String> {
    let mut expected = RabbitMqPublish::new(value.to_owned());
    expected.headers = record().headers;
    expected.properties = RabbitMqProperties {
        content_type: Some("text/plain".into()),
        correlation_id: Some("c-1".into()),
        priority: Some(4),
        ..RabbitMqProperties::default()
    };
    expected
}

#[tokio::test]
async fn inherits_headers_and_descriptive_properties_only() {
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                "inherits_headers_and_descriptive_properties_only",
                IterSource::new([record()]),
                sink.clone(),
                |record: RabbitMqRecord<String>| async move {
                    let mut output = RabbitMqPublish::new(record.value);
                    output
                        .headers
                        .insert("kind".into(), RabbitMqValue::from("processed"));
                    output.properties.content_type = Some("application/json".into());
                    Ok(output)
                },
            )
            .middleware(RabbitMqInherit::new()),
        )
        .run()
        .await
        .unwrap();
    let mut expected = inherited("order");
    expected
        .headers
        .insert("kind".into(), RabbitMqValue::from("processed"));
    expected.properties.content_type = Some("application/json".into());
    assert_eq!(sink.values(), vec![expected]);
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
                |record: RabbitMqRecord<String>| async move { Ok(RabbitMqPublish::new(record.value)) },
            )
            .middleware(RabbitMqInherit::new().without_headers().without_properties()),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(
        sink.values(),
        vec![RabbitMqPublish::new("order".to_owned())]
    );
}

#[tokio::test]
async fn value_handlers_inherit_rabbitmq_metadata_by_default() {
    let sink = InMemorySink::<RabbitMqPublish<_>>::default();
    App::new()
        .subscription(Subscription::new(
            "value_handlers_inherit_rabbitmq_metadata_by_default",
            IterSource::new([record()]),
            sink.clone(),
            |value: String| async move { Ok(value.to_uppercase()) },
        ))
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![inherited("ORDER")]);
}

#[test]
fn propagation_fields_replace_headers() {
    let mut output = RabbitMqPublish::new("order".to_owned());
    output.headers.insert(
        "traceparent".into(),
        RabbitMqValue::Bytes(b"inherited".to_vec()),
    );
    output.set_propagation_field("traceparent", "current".into());
    assert_eq!(
        output.headers["traceparent"],
        RabbitMqValue::from("current")
    );
    assert_eq!(output.headers.len(), 1);
}
