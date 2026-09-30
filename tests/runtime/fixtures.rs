use beavers::{
    Delivery, RawPayload, Receive, ReceiveError, RetryPolicy, Sink, Source, SourceMessage,
};
use std::{
    collections::VecDeque,
    future::pending,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

pub(crate) fn fast() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        initial_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
        ..RetryPolicy::default()
    }
}

pub(crate) struct Flaky {
    pub(crate) calls: Arc<AtomicUsize>,
    pub(crate) acks: Arc<AtomicUsize>,
    pub(crate) fail_always: bool,
}

impl Sink<i32> for Flaky {
    type Prepared = i32;

    fn prepare(&self, value: i32) -> anyhow::Result<i32> {
        Ok(value)
    }

    async fn publish(&self, _: &i32) -> anyhow::Result<()> {
        assert_eq!(self.acks.load(Ordering::SeqCst), 0);
        let calls = self.calls.fetch_add(1, Ordering::SeqCst);
        anyhow::ensure!(!self.fail_always && calls >= 2, "offline");
        Ok(())
    }
}

pub(crate) struct Waiting;

impl Source for Waiting {
    type Message = Delivery<i32>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        pending().await
    }
}

pub(crate) struct ScriptedSource {
    pub(crate) events: VecDeque<Result<Option<i32>, bool>>,
    pub(crate) calls: Arc<AtomicUsize>,
}

impl Source for ScriptedSource {
    type Message = Delivery<i32>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.events.pop_front().unwrap_or(Ok(None)) {
            Ok(Some(value)) => Ok(Receive::Message(Delivery::untracked(value))),
            Ok(None) => Ok(Receive::End),
            Err(true) => Err(ReceiveError::Retry(anyhow::anyhow!("retry"))),
            Err(false) => Err(ReceiveError::Fatal(anyhow::anyhow!("fatal"))),
        }
    }
}

/// Yields text payloads that decode as `i32`; anything else is a decode failure.
pub(crate) struct TextSource {
    pub(crate) payloads: VecDeque<&'static str>,
    pub(crate) acks: Arc<AtomicUsize>,
}

pub(crate) struct TextMessage {
    payload: &'static str,
    acks: Arc<AtomicUsize>,
}

impl SourceMessage for TextMessage {
    type Item = i32;

    fn decode(&self) -> anyhow::Result<i32> {
        Ok(self.payload.parse()?)
    }

    async fn ack(self) -> anyhow::Result<()> {
        self.acks.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn raw_payload(&self) -> Option<RawPayload> {
        Some(RawPayload::Bytes(self.payload.as_bytes().to_vec()))
    }
}

impl Source for TextSource {
    type Message = TextMessage;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        Ok(match self.payloads.pop_front() {
            Some(payload) => Receive::Message(TextMessage {
                payload,
                acks: self.acks.clone(),
            }),
            None => Receive::End,
        })
    }
}

/// Publishes values but fails to prepare negative ones.
#[derive(Clone, Default)]
pub(crate) struct RejectNegative(pub(crate) Arc<Mutex<Vec<i32>>>);

impl Sink<i32> for RejectNegative {
    type Prepared = i32;

    fn prepare(&self, value: i32) -> anyhow::Result<i32> {
        anyhow::ensure!(value >= 0, "negative output");
        Ok(value)
    }

    async fn publish(&self, value: &i32) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(*value);
        Ok(())
    }
}
