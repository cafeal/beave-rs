use crate::message::SourceMessage;
use crate::{
    codec::Decoder,
    source::{Receive, ReceiveError, Source},
};
use std::{
    io::{self, BufRead},
    marker::PhantomData,
    sync::Arc,
    thread,
};
use tokio::sync::mpsc;

/// Newline-delimited input. One detached, bounded reader starts on the first receive.
/// An OS read may remain blocked after close, but never blocks Tokio runtime shutdown.
/// Use only one stdin source per process.
pub struct StdinSource<C, T> {
    codec: Arc<C>,
    receiver: Option<mpsc::Receiver<io::Result<Vec<u8>>>>,
    marker: PhantomData<T>,
}
impl<C: Default, T> Default for StdinSource<C, T> {
    fn default() -> Self {
        Self::new()
    }
}
impl<C: Default, T> StdinSource<C, T> {
    pub fn new() -> Self {
        Self::with_codec(C::default())
    }
}
impl<C, T> StdinSource<C, T> {
    /// Uses an existing codec instance, such as a configured `Avro` codec.
    pub fn with_codec(codec: C) -> Self {
        Self {
            codec: Arc::new(codec),
            receiver: None,
            marker: PhantomData,
        }
    }
}
impl<T: Clone + Send + Sync + 'static, C: Decoder<T>> Source for StdinSource<C, T> {
    type Message = StdinMessage<C, T>;
    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        if self.receiver.is_none() {
            let (sender, receiver) = mpsc::channel(1);
            thread::Builder::new()
                .name("beavers-stdin".into())
                .spawn(move || {
                    let stdin = io::stdin();
                    let mut reader = stdin.lock();
                    while !sender.is_closed() {
                        let mut line = Vec::new();
                        match reader.read_until(b'\n', &mut line) {
                            Ok(0) => break,
                            Ok(_) => {
                                trim_line_ending(&mut line);
                                if sender.blocking_send(Ok(line)).is_err() {
                                    break;
                                }
                            }
                            Err(error) => {
                                let _ = sender.blocking_send(Err(error));
                                break;
                            }
                        }
                    }
                })
                .map_err(|error| ReceiveError::Fatal(error.into()))?;
            self.receiver = Some(receiver);
        }
        match self.receiver.as_mut().unwrap().recv().await {
            Some(Ok(line)) => Ok(Receive::Message(StdinMessage {
                bytes: line,
                codec: self.codec.clone(),
                marker: PhantomData,
            })),
            Some(Err(error)) => Err(ReceiveError::Fatal(error.into())),
            None => Ok(Receive::End),
        }
    }
    async fn close(&mut self) -> anyhow::Result<()> {
        if let Some(receiver) = &mut self.receiver {
            receiver.close();
        }
        Ok(())
    }
}

/// Removes a trailing `\n` or `\r\n`.
fn trim_line_ending(line: &mut Vec<u8>) {
    if line.last() == Some(&b'\n') {
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
    }
}

/// Owns one raw stdin line until decode and processing have completed.
pub struct StdinMessage<C, T> {
    bytes: Vec<u8>,
    codec: Arc<C>,
    marker: PhantomData<T>,
}
impl<C: Decoder<T>, T: Clone + Send + Sync + 'static> SourceMessage for StdinMessage<C, T> {
    type Item = T;
    /// The received line without its line ending.
    type Raw = Vec<u8>;
    fn decode(&self) -> anyhow::Result<T> {
        self.codec.decode(&self.bytes)
    }
    async fn ack(self) -> anyhow::Result<()> {
        Ok(())
    }
    fn raw(&self) -> Vec<u8> {
        self.bytes.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::trim_line_ending;

    #[test]
    fn line_endings_are_removed() {
        for (input, expected) in [
            (&b"a\n"[..], &b"a"[..]),
            (b"a\r\n", b"a"),
            (b"a", b"a"),
            (b"\n", b""),
            (b"a\r", b"a\r"),
        ] {
            let mut line = input.to_vec();
            trim_line_ending(&mut line);
            assert_eq!(line, expected);
        }
    }
}
