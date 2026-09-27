use beavers::{Delivery, Receive, ReceiveError, RetryPolicy, Sink, Source};
use std::{
    collections::VecDeque,
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

pub(crate) fn fast() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        initial_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
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
