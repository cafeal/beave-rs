use beavers::{
    App, InMemorySink, IterSource, PropagationCarrier, Subscription, Tombstones,
    adapters::pulsar::{PulsarInherit, PulsarMetadata, PulsarPublish, PulsarRecord},
};
use std::sync::atomic::Ordering;

fn record() -> PulsarRecord<String> {
    PulsarRecord {
        value: Some("order".to_owned()),
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
                    let mut output = PulsarPublish::new(record.value.unwrap_or_default());
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
                |record: PulsarRecord<String>| async move {
                    Ok(PulsarPublish::new(record.value.unwrap_or_default()))
                },
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

#[tokio::test]
async fn value_handlers_inherit_pulsar_metadata_by_default() {
    let sink = InMemorySink::default();
    App::new()
        .subscription(Subscription::forward(
            IterSource::new([record()]),
            sink.clone(),
            |value: String| async move { Ok(value.to_uppercase()) },
        ))
        .run()
        .await
        .unwrap();
    let input = record();
    let mut expected = PulsarPublish::new("ORDER".to_owned());
    expected.key = input.key;
    expected.properties = input.properties;
    expected.event_time = input.event_time;
    assert_eq!(sink.values(), vec![expected]);
}

#[tokio::test]
async fn tombstone_policies_apply_to_pulsar_null_values() {
    let mut tombstone = record();
    tombstone.value = None;
    for (skip, published_by_dlq) in [(true, 0), (false, 1)] {
        let source = IterSource::new([tombstone.clone(), record()]);
        let acks = source.acknowledgements();
        let sink = InMemorySink::default();
        let dlq = InMemorySink::default();
        let policy = if skip {
            Tombstones::skip()
        } else {
            Tombstones::reject()
        };
        App::new()
            .subscription(
                Subscription::forward(source, sink.clone(), |value: String| async move {
                    Ok(value.len())
                })
                .middleware(policy)
                .dlq_with(dlq.clone(), |dead_letter| Ok(dead_letter.input.unwrap())),
            )
            .run()
            .await
            .unwrap();
        assert_eq!(sink.values().len(), 1);
        assert_eq!(dlq.values().len(), published_by_dlq);
        assert_eq!(acks.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn propagated_pulsar_tombstones_keep_the_key_and_inherited_fields() {
    let mut tombstone = record();
    tombstone.value = None;
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::forward(
                IterSource::new([tombstone.clone()]),
                sink.clone(),
                |value: String| async move { Ok(value) },
            )
            .middleware(Tombstones::propagate()),
        )
        .run()
        .await
        .unwrap();
    let mut expected = PulsarPublish::<String>::tombstone(b"customer-7".to_vec());
    expected.properties = tombstone.properties;
    expected.event_time = tombstone.event_time;
    assert_eq!(sink.values(), vec![expected]);
}

#[test]
fn propagation_fields_replace_properties() {
    let mut output = PulsarPublish::new("order".to_owned());
    output
        .properties
        .insert("traceparent".into(), "inherited".into());
    output.set_propagation_field("traceparent", "current".into());
    assert_eq!(output.properties["traceparent"], "current");
    assert_eq!(output.properties.len(), 1);
}
