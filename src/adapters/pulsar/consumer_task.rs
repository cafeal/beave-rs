use super::record::PulsarMetadata;
use crate::source::ReceiveError;
use futures_util::StreamExt;
use pulsar::{Consumer, TokioExecutor, consumer::Message, message::proto::MessageIdData};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

pub(super) enum ConsumerCommand {
    Ack {
        topic: String,
        id: MessageIdData,
        result: oneshot::Sender<anyhow::Result<()>>,
    },
    Close(oneshot::Sender<anyhow::Result<()>>),
}

pub(super) struct RawDelivery {
    pub bytes: Vec<u8>,
    pub key: Option<Vec<u8>>,
    pub properties: HashMap<String, String>,
    pub event_time: Option<u64>,
    pub metadata: PulsarMetadata,
}

type DeliveryResult = Result<RawDelivery, ReceiveError>;

pub(super) fn spawn_consumer(
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    capacity: usize,
) -> (
    mpsc::Receiver<DeliveryResult>,
    mpsc::Sender<ConsumerCommand>,
) {
    let (delivery_tx, delivery_rx) = mpsc::channel(capacity);
    let (command_tx, command_rx) = mpsc::channel(capacity);
    tokio::spawn(run_consumer(consumer, delivery_tx, command_rx));
    (delivery_rx, command_tx)
}

async fn run_consumer(
    mut consumer: Consumer<Vec<u8>, TokioExecutor>,
    delivery_tx: mpsc::Sender<DeliveryResult>,
    mut command_rx: mpsc::Receiver<ConsumerCommand>,
) {
    let mut pending = None;
    loop {
        if let Some(delivery) = pending.take() {
            tokio::select! {
                biased;
                command = command_rx.recv() => {
                    if process_command(command, &mut consumer).await { return; }
                }
                sent = delivery_tx.send(delivery) => {
                    if sent.is_err() { return; }
                }
            }
            continue;
        }

        tokio::select! {
            biased;
            command = command_rx.recv() => {
                if process_command(command, &mut consumer).await { return; }
            },
            delivery = consumer.next() => match delivery {
                Some(Ok(message)) => pending = Some(raw_delivery(message).map_err(ReceiveError::Fatal)),
                Some(Err(error)) => pending = Some(Err(ReceiveError::Retry(error.into()))),
                None => return,
            }
        }
    }
}

async fn process_command(
    command: Option<ConsumerCommand>,
    consumer: &mut Consumer<Vec<u8>, TokioExecutor>,
) -> bool {
    match command {
        Some(ConsumerCommand::Ack { topic, id, result }) => {
            let ack = consumer
                .ack_with_id(&topic, id)
                .await
                .map_err(anyhow::Error::from);
            let _ = result.send(ack);
            false
        }
        Some(ConsumerCommand::Close(result)) => {
            let close = consumer.close().await.map_err(anyhow::Error::from);
            let _ = result.send(close);
            true
        }
        None => true,
    }
}

fn raw_delivery(message: Message<Vec<u8>>) -> anyhow::Result<RawDelivery> {
    let metadata = PulsarMetadata {
        topic: message.topic.clone(),
        message_id: message.message_id().clone(),
        publish_time: message.metadata().publish_time,
    };
    let properties = message
        .metadata()
        .properties
        .iter()
        .map(|property| (property.key.clone(), property.value.clone()))
        .collect();
    Ok(RawDelivery {
        bytes: message.payload.data.to_vec(),
        key: message.key_bytes()?,
        properties,
        event_time: message.metadata().event_time,
        metadata,
    })
}
