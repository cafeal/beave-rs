use beavers::{
    App, InMemorySink, IterSource, PropagationCarrier, Subscription,
    adapters::servicebus::{
        ServiceBusApplicationProperties, ServiceBusInherit, ServiceBusProperties,
        ServiceBusPublish, ServiceBusRecord, ServiceBusValue,
    },
    testing::servicebus_record,
};
use std::time::Duration;

fn record() -> ServiceBusRecord<String> {
    let mut metadata = servicebus_record("orders", 10, "").metadata;
    metadata.enqueued_time = Some(1);
    metadata.lock_token = "00000000-0000-0000-0000-000000000001".into();
    ServiceBusRecord {
        value: "order".to_owned(),
        application_properties: ServiceBusApplicationProperties::from([
            ("trace".to_owned(), ServiceBusValue::from("abc")),
            ("kind".to_owned(), ServiceBusValue::from("new")),
        ]),
        properties: ServiceBusProperties {
            message_id: Some("m-1".into()),
            correlation_id: Some("c-1".into()),
            session_id: Some("customer-7".into()),
            subject: Some("order".into()),
            content_type: Some("application/json".into()),
            time_to_live: Some(Duration::from_secs(60)),
            partition_key: Some("customer-7".into()),
            ..ServiceBusProperties::default()
        },
        metadata,
    }
}

#[tokio::test]
async fn inherits_application_properties_without_overriding_explicit_ones() {
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                "inherits_application_properties_without_overriding_explicit_ones",
                IterSource::new([record()]),
                sink.clone(),
                |record: ServiceBusRecord<String>| async move {
                    let mut output = ServiceBusPublish::new(record.value);
                    output
                        .application_properties
                        .insert("kind".into(), ServiceBusValue::from("processed"));
                    Ok(output)
                },
            )
            .middleware(ServiceBusInherit::new()),
        )
        .run()
        .await
        .unwrap();
    let mut expected = ServiceBusPublish::new("order".to_owned());
    expected.application_properties = ServiceBusApplicationProperties::from([
        ("trace".to_owned(), ServiceBusValue::from("abc")),
        ("kind".to_owned(), ServiceBusValue::from("processed")),
    ]);
    assert_eq!(sink.values(), vec![expected]);
}

#[tokio::test]
async fn the_session_is_inherited_only_when_enabled() {
    for (inherit, session) in [
        (ServiceBusInherit::new(), None),
        (
            ServiceBusInherit::new()
                .without_application_properties()
                .with_session(),
            Some("customer-7"),
        ),
    ] {
        let sink = InMemorySink::default();
        App::new()
            .subscription(
                Subscription::new(
                    "the_session_is_inherited_only_when_enabled",
                    IterSource::new([record()]),
                    sink.clone(),
                    |record: ServiceBusRecord<String>| async move {
                        Ok(ServiceBusPublish::new(record.value))
                    },
                )
                .middleware(inherit),
            )
            .run()
            .await
            .unwrap();
        let outputs: Vec<ServiceBusPublish<String>> = sink.values();
        let properties = &outputs[0].properties;
        assert_eq!(properties.session_id.as_deref(), session);
        assert_eq!(properties.partition_key.as_deref(), session);
        assert_eq!(properties.message_id, None);
        assert_eq!(properties.time_to_live, None);
    }
}

#[tokio::test]
async fn value_handlers_inherit_service_bus_application_properties_by_default() {
    let sink = InMemorySink::<ServiceBusPublish<_>>::default();
    App::new()
        .subscription(Subscription::new(
            "value_handlers_inherit_service_bus_application_properties_by_default",
            IterSource::new([record()]),
            sink.clone(),
            |value: String| async move { Ok(value.to_uppercase()) },
        ))
        .run()
        .await
        .unwrap();
    let mut expected = ServiceBusPublish::new("ORDER".to_owned());
    expected.application_properties = record().application_properties;
    assert_eq!(sink.values(), vec![expected]);
}

#[test]
fn propagation_fields_replace_application_properties() {
    let mut output = ServiceBusPublish::new("order".to_owned());
    output
        .application_properties
        .insert("traceparent".into(), ServiceBusValue::I64(1));
    output.set_propagation_field("traceparent", "current".into());
    assert_eq!(
        output.application_properties["traceparent"],
        ServiceBusValue::from("current")
    );
    assert_eq!(output.application_properties.len(), 1);
}
