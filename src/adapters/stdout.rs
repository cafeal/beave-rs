use crate::{codec::Encoder, sink::Sink};
use tokio::io::AsyncWriteExt;

/// Newline-delimited output: each prepared value is written and flushed as one line.
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
    /// Creates a sink that encodes with the codec's default value.
    pub fn new() -> Self {
        Self::with_codec(C::default())
    }
}
impl<C> StdoutSink<C> {
    /// Uses an existing codec instance, such as a configured `Avro` codec.
    pub fn with_codec(codec: C) -> Self {
        Self {
            codec,
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

#[cfg(test)]
mod tests {
    use super::StdoutSink;
    use crate::{Json, Sink, Utf8};

    #[test]
    fn prepared_values_are_one_line_each() {
        let sink = StdoutSink::<Json>::new();
        assert_eq!(
            sink.prepare(serde_json::json!({"id": 1})).unwrap(),
            b"{\"id\":1}\n"
        );
        let sink = StdoutSink::with_codec(Utf8);
        assert_eq!(sink.prepare("done".to_owned()).unwrap(), b"done\n");
    }
}
