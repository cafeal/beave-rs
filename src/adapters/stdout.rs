use crate::{codec::Encoder, sink::Sink};
use tokio::io::AsyncWriteExt;

pub struct StdoutSink<C> {
    codec: C,
    writer: tokio::sync::Mutex<tokio::io::Stdout>,
}
impl<C: Default> Default for StdoutSink<C> {
    fn default() -> Self {
        Self::new()
    }
}
impl<C: Default> StdoutSink<C> {
    pub fn new() -> Self {
        Self {
            codec: C::default(),
            writer: tokio::sync::Mutex::new(tokio::io::stdout()),
        }
    }
}
impl<T: Sync, C: Encoder<T>> Sink<T> for StdoutSink<C> {
    type Prepared = Vec<u8>;
    fn prepare(&self, value: T) -> anyhow::Result<Vec<u8>> {
        let mut bytes = self.codec.encode(&value)?;
        bytes.push(b'\n');
        Ok(bytes)
    }
    async fn publish(&self, bytes: &Vec<u8>) -> anyhow::Result<()> {
        let mut writer = self.writer.lock().await;
        writer.write_all(bytes).await?;
        writer.flush().await?;
        Ok(())
    }
}
