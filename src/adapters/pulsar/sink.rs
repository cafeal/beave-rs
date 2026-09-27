use super::{config::PulsarSinkConfig, record::PulsarPublish};
use crate::{codec::Encoder, sink::Sink};
use pulsar::{
    Pulsar, TokioExecutor,
    producer::{Message as ProducerMessage, Producer},
};
use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::sync::Mutex;

/// Encoded Pulsar output. Clones can be retried without rerunning the codec.
#[derive(Clone, Debug, Default)]
pub struct PulsarPrepared {
    pub payload: Vec<u8>,
    pub properties: HashMap<String, String>,
    pub key: Option<Vec<u8>>,
    pub ordering_key: Option<Vec<u8>>,
    pub event_time: Option<u64>,
}

struct SinkState {
    producer: Option<Producer<TokioExecutor>>,
}

/// Publishes prepared messages and waits for the Pulsar broker receipt.
pub struct PulsarSink<C, T> {
    config: PulsarSinkConfig,
    codec: C,
    state: Mutex<SinkState>,
    closed: AtomicBool,
    marker: PhantomData<fn(T)>,
}

impl<C: Default, T> PulsarSink<C, T> {
    pub fn new(config: PulsarSinkConfig) -> Self {
        Self::with_codec(config, C::default())
    }
}

impl<C, T> PulsarSink<C, T> {
    pub fn with_codec(config: PulsarSinkConfig, codec: C) -> Self {
        Self {
            config,
            codec,
            state: Mutex::new(SinkState { producer: None }),
            closed: AtomicBool::new(false),
            marker: PhantomData,
        }
    }

    async fn connect(&self) -> anyhow::Result<Producer<TokioExecutor>> {
        self.config.validate()?;
        let mut builder = Pulsar::builder(&self.config.service_url, TokioExecutor);
        if let Some(auth) = &self.config.authentication {
            builder = builder.with_auth(auth.provider());
        }
        let client: Pulsar<_> = builder.build().await?;
        let mut producer = client.producer().with_topic(&self.config.topic);
        if let Some(name) = &self.config.producer_name {
            producer = producer.with_name(name);
        }
        Ok(producer.build().await?)
    }
}

impl<C: Encoder<T>, T: Send + Sync + 'static> Sink<PulsarPublish<T>> for PulsarSink<C, T> {
    type Prepared = PulsarPrepared;

    fn prepare(&self, output: PulsarPublish<T>) -> anyhow::Result<Self::Prepared> {
        Ok(PulsarPrepared {
            payload: self.codec.encode(&output.value)?,
            properties: output.properties,
            key: output.key,
            ordering_key: output.ordering_key,
            event_time: output.event_time,
        })
    }

    async fn publish(&self, output: &Self::Prepared) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "Pulsar sink is closed"
        );
        if state.producer.is_none() {
            state.producer = Some(self.connect().await?);
        }
        let message = ProducerMessage {
            payload: output.payload.clone(),
            properties: output.properties.clone(),
            partition_key: output.key.as_ref().map(|key| base64_encode(key)),
            partition_key_b64_encoded: output.key.as_ref().map(|_| true),
            ordering_key: output.ordering_key.clone(),
            event_time: output.event_time,
            ..Default::default()
        };
        let receipt = state
            .producer
            .as_mut()
            .expect("connected producer")
            .send_non_blocking(message)
            .await?;
        receipt.await?;
        Ok(())
    }

    async fn close(&self) -> anyhow::Result<()> {
        self.closed.store(true, Ordering::Release);
        if let Some(mut producer) = self.state.lock().await.producer.take() {
            producer.close().await?;
        }
        Ok(())
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let bits = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        output.push(TABLE[((bits >> 18) & 63) as usize] as char);
        output.push(TABLE[((bits >> 12) & 63) as usize] as char);
        output.push(if chunk.len() > 1 {
            TABLE[((bits >> 6) & 63) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            TABLE[(bits & 63) as usize] as char
        } else {
            '='
        });
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_keys_use_pulsars_base64_representation() {
        assert_eq!(base64_encode(&[0, 1, 2, 253, 254, 255]), "AAEC/f7/");
        assert_eq!(base64_encode(b"a"), "YQ==");
        assert_eq!(base64_encode(b"ab"), "YWI=");
    }
}
