use beavers::{
    Json, Receive, Source, SourceMessage,
    testing::{
        PubSubTestSource, ServiceBusTestSource, SqsTestSource, pubsub_record, servicebus_record,
        sqs_record,
    },
};

async fn message_keys<S: Source>(mut source: S) -> Vec<Option<Vec<u8>>> {
    let mut keys = Vec::new();
    while let Receive::Message(message) = source.receive().await.unwrap() {
        keys.push(message.message_key().map(<[u8]>::to_vec));
    }
    keys
}

#[tokio::test]
async fn an_sqs_message_is_keyed_by_its_message_group() {
    let queue = "https://sqs.us-east-1.amazonaws.com/1/orders.fifo";
    let mut grouped = sqs_record(queue, "m-1", "1");
    grouped.metadata.message_group_id = Some("customer-7".into());
    let source = SqsTestSource::<Json<u32>>::new([grouped, sqs_record(queue, "m-2", "2")]);
    assert_eq!(
        message_keys(source).await,
        [Some(b"customer-7".to_vec()), None]
    );
}

#[tokio::test]
async fn a_pubsub_message_is_keyed_by_its_ordering_key() {
    let mut ordered = pubsub_record("orders", "m-1", "1");
    ordered.ordering_key = Some("customer-7".into());
    let source = PubSubTestSource::<Json<u32>>::new([ordered, pubsub_record("orders", "m-2", "2")]);
    assert_eq!(
        message_keys(source).await,
        [Some(b"customer-7".to_vec()), None]
    );
}

#[tokio::test]
async fn a_servicebus_message_is_keyed_by_its_session() {
    let mut session = servicebus_record("orders", 1, "1");
    session.properties.session_id = Some("customer-7".into());
    let source =
        ServiceBusTestSource::<Json<u32>>::new([session, servicebus_record("orders", 2, "2")]);
    assert_eq!(
        message_keys(source).await,
        [Some(b"customer-7".to_vec()), None]
    );
}
