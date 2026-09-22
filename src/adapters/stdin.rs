use crate::message::SourceMessage;
use crate::{
    codec::Decoder,
    source::{Receive, ReceiveError, Source},
};
use std::{marker::PhantomData, sync::Arc};

/// Newline-delimited input. One detached, bounded reader starts on the first receive.
/// An OS read may remain blocked after close, but never blocks Tokio runtime shutdown.
/// Use only one stdin source per process.
pub struct StdinSource<C, T> {
    codec: Arc<C>,
    receiver: Option<tokio::sync::mpsc::Receiver<std::io::Result<Vec<u8>>>>,
    marker: PhantomData<T>,
}
impl<C: Default, T> Default for StdinSource<C, T> {
    fn default() -> Self {
        Self::new()
    }
}
impl<C: Default, T> StdinSource<C, T> {
    pub fn new() -> Self {
        Self {
            codec: Arc::new(C::default()),
            receiver: None,
            marker: PhantomData,
        }
    }
}
impl<T: Clone + Send + Sync + 'static, C: Decoder<T>> Source for StdinSource<C, T> {
    type Message = StdinMessage<C, T>;
    async fn receive(&mut self) -> std::result::Result<Receive<Self::Message>, ReceiveError> {
        if self.receiver.is_none() {
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            std::thread::Builder::new()
                .name("beavers-stdin".into())
                .spawn(move || {
                    use std::io::BufRead;
                    let stdin = std::io::stdin();
                    let mut reader = stdin.lock();
                    while !sender.is_closed() {
                        let mut line = Vec::new();
                        match reader.read_until(b'\n', &mut line) {
                            Ok(0) => break,
                            Ok(_) => {
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

/// Owns one raw stdin line until decode and processing have completed.
pub struct StdinMessage<C, T> {
    bytes: Vec<u8>,
    codec: Arc<C>,
    marker: PhantomData<T>,
}
impl<C: Decoder<T>, T: Clone + Send + Sync + 'static> SourceMessage for StdinMessage<C, T> {
    type Item = T;
    fn decode(&self) -> anyhow::Result<T> {
        self.codec.decode(&self.bytes)
    }
    async fn ack(self) -> anyhow::Result<()> {
        Ok(())
    }
}
