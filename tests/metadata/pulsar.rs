use beavers::{
    App, InMemorySink, IterSource, Subscription,
    adapters::pulsar::{PulsarInherit, PulsarMetadata, PulsarPublish, PulsarRecord},
};

fn record() -> PulsarRecord<String> {
    PulsarRecord {
        value: "order".to_owned(),
        key: Some(b"customer-7".to_vec()),
        properties: [
            ("trace".to_owned(), "abc".to_owned()),
            ("kind".to_owned(), "new".to_owned()),
        ]
        .into(),
        event_time: Some(42),
        metadata: PulsarMetadata {
            topic: "persistent://public/default/orders".to_owned(),
            message_id: Default::default(),
            publish_time: 41,
        },
    }
}

#[tokio::test]
async fn inherits_application_fields_without_overriding_explicit_ones() {
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                IterSource::new([record()]),
                sink.clone(),
                |record: PulsarRecord<String>| async move {
                    let mut output = PulsarPublish::new(record.value);
                    output.properties.insert("kind".into(), "processed".into());
                    output.event_time = Some(50);
                    Ok(output)
                },
            )
            .middleware(PulsarInherit::new()),
        )
        .run()
        .await
        .unwrap();
    let mut expected = PulsarPublish::new("order".to_owned());
    expected.key = Some(b"customer-7".to_vec());
    expected.properties = [
        ("trace".to_owned(), "abc".to_owned()),
        ("kind".to_owned(), "processed".to_owned()),
    ]
    .into();
    expected.event_time = Some(50);
    assert_eq!(sink.values(), vec![expected]);
}

#[tokio::test]
async fn disabled_fields_are_not_inherited() {
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                IterSource::new([record()]),
                sink.clone(),
                |record: PulsarRecord<String>| async move { Ok(PulsarPublish::new(record.value)) },
            )
            .middleware(
                PulsarInherit::new()
                    .without_key()
                    .without_properties()
                    .without_event_time(),
            ),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![PulsarPublish::new("order".to_owned())]);
}
