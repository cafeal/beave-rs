use beavers::{
    App, InMemorySink, IterSource, PropagationCarrier, Subscription,
    adapters::pubsub::{PubSubAttributes, PubSubInherit, PubSubPublish, PubSubRecord},
    testing::pubsub_record,
};

fn record() -> PubSubRecord<String> {
    let mut metadata = pubsub_record("projects/p/subscriptions/orders", "m-1", "").metadata;
    metadata.publish_time = Some(1);
    metadata.delivery_attempt = Some(2);
    PubSubRecord {
        value: "order".to_owned(),
        attributes: PubSubAttributes::from([
            ("trace".to_owned(), "abc".to_owned()),
            ("kind".to_owned(), "new".to_owned()),
        ]),
        ordering_key: Some("customer-7".into()),
        metadata,
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
                |record: PubSubRecord<String>| async move {
                    let mut output = PubSubPublish::new(record.value);
                    output.attributes.insert("kind".into(), "processed".into());
                    Ok(output)
                },
            )
            .middleware(PubSubInherit::new()),
        )
        .run()
        .await
        .unwrap();
    let mut expected = PubSubPublish::new("order".to_owned());
    expected.attributes = PubSubAttributes::from([
        ("trace".to_owned(), "abc".to_owned()),
        ("kind".to_owned(), "processed".to_owned()),
    ]);
    assert_eq!(sink.values(), vec![expected]);
}

#[tokio::test]
async fn the_ordering_key_is_inherited_only_when_enabled() {
    for (inherit, key) in [
        (PubSubInherit::new(), None),
        (
            PubSubInherit::new()
                .without_attributes()
                .with_ordering_key(),
            Some("customer-7"),
        ),
    ] {
        let sink = InMemorySink::default();
        App::new()
            .subscription(
                Subscription::new(
                    "the_ordering_key_is_inherited_only_when_enabled",
                    IterSource::new([record()]),
                    sink.clone(),
                    |record: PubSubRecord<String>| async move {
                        Ok(PubSubPublish::new(record.value))
                    },
                )
                .middleware(inherit),
            )
            .run()
            .await
            .unwrap();
        let outputs: Vec<PubSubPublish<String>> = sink.values();
        assert_eq!(outputs[0].ordering_key.as_deref(), key);
        assert_eq!(outputs[0].attributes.is_empty(), key.is_some());
    }
}

#[tokio::test]
async fn value_handlers_inherit_pubsub_attributes_by_default() {
    let sink = InMemorySink::<PubSubPublish<_>>::default();
    App::new()
        .subscription(Subscription::new(
            "value_handlers_inherit_pubsub_attributes_by_default",
            IterSource::new([record()]),
            sink.clone(),
            |value: String| async move { Ok(value.to_uppercase()) },
        ))
        .run()
        .await
        .unwrap();
    let mut expected = PubSubPublish::new("ORDER".to_owned());
    expected.attributes = record().attributes;
    assert_eq!(sink.values(), vec![expected]);
}

#[test]
fn propagation_fields_replace_attributes() {
    let mut output = PubSubPublish::new("order".to_owned());
    output.attributes.insert("traceparent".into(), "old".into());
    output.set_propagation_field("traceparent", "current".into());
    assert_eq!(output.attributes["traceparent"], "current");
    assert_eq!(output.attributes.len(), 1);
}
