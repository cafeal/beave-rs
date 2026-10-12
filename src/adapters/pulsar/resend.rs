use crate::error::{BoxError, Error};
use crate::retry::RetryPolicy;
use futures_util::future::BoxFuture;
use magnetar::{
    proto::{pb::ServerError, producer::OutgoingMessage},
    runtime_tokio::{ClientError, Producer},
};
use std::{collections::VecDeque, future::Future, sync::Mutex};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use tracing::warn;

/// Queues a message on a producer and resolves with its broker receipt.
pub(super) trait SendMessage: Send + Sync + 'static {
    fn send(&self, message: OutgoingMessage) -> BoxFuture<'static, Result<(), ClientError>>;
}

impl SendMessage for Producer {
    fn send(&self, message: OutgoingMessage) -> BoxFuture<'static, Result<(), ClientError>> {
        let receipt = Producer::send(self, message);
        Box::pin(async move { receipt.await.map(drop) })
    }
}

/// Sends messages on one producer and sends again those the broker rejects.
///
/// The client replays a send that loses its connection, but a send the broker
/// answers with an error fails. A broker that is shutting down or unloading a
/// topic rejects the sends in flight with a persistence error. A task awaits
/// the receipts in submission order and sends each rejected message again, so
/// the messages of one rejected run are sent again in their original order.
pub(super) struct Resender<P> {
    /// Locked while a message is queued on the producer and handed to the task
    /// so that the task sees the messages in the producer's order.
    sender: Mutex<(P, mpsc::UnboundedSender<Attempt>)>,
}

struct Attempt {
    message: OutgoingMessage,
    receipt: BoxFuture<'static, Result<(), ClientError>>,
    sent_at: Instant,
    /// Rejected sends of this message so far.
    failures: usize,
    result: oneshot::Sender<Result<(), BoxError>>,
}

impl<P: SendMessage + Clone> Resender<P> {
    /// Starts the task that awaits the receipts. It stops once the resender
    /// is dropped and every queued message has its outcome.
    pub(super) fn spawn(producer: P, retry: RetryPolicy) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(await_receipts(producer.clone(), receiver, retry));
        Self {
            sender: Mutex::new((producer, sender)),
        }
    }
}

impl<P: SendMessage> Resender<P> {
    /// Queues `message` on the producer. The returned future resolves with the
    /// receipt of the send that the broker accepted, or with the error that
    /// ended its sends. Dropping the future stops further sends of the message
    /// but not one already queued.
    pub(super) fn send(
        &self,
        message: OutgoingMessage,
    ) -> impl Future<Output = Result<(), BoxError>> + Send + 'static {
        let (result, outcome) = oneshot::channel();
        {
            let sender = self
                .sender
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let (producer, attempts) = &*sender;
            let receipt = producer.send(message.clone());
            // The task stops only after the resender is dropped, so it
            // receives every attempt.
            let _ = attempts.send(Attempt {
                message,
                receipt,
                sent_at: Instant::now(),
                failures: 0,
                result,
            });
        }
        async move {
            outcome
                .await
                .map_err(|_| Error::closed("Pulsar producer task stopped"))?
        }
    }
}

/// Awaits each attempt's receipt in order. A rejected message whose outcome
/// is still awaited is sent again no earlier than the retry delay after its
/// previous send and is then awaited after the attempts queued before it.
async fn await_receipts<P: SendMessage>(
    producer: P,
    mut attempts: mpsc::UnboundedReceiver<Attempt>,
    retry: RetryPolicy,
) {
    let mut pending = VecDeque::new();
    loop {
        while let Ok(attempt) = attempts.try_recv() {
            pending.push_back(attempt);
        }
        let Some(mut attempt) = pending.pop_front() else {
            match attempts.recv().await {
                Some(attempt) => {
                    pending.push_back(attempt);
                    continue;
                }
                None => return,
            }
        };
        let error = match (&mut attempt.receipt).await {
            Ok(()) => {
                let _ = attempt.result.send(Ok(()));
                continue;
            }
            Err(error) => error,
        };
        attempt.failures += 1;
        if !resendable(&error) || attempt.failures >= retry.max_attempts {
            let _ = attempt.result.send(Err(error.into()));
            continue;
        }
        if attempt.result.is_closed() {
            continue;
        }
        let delay = retry.delay(attempt.failures);
        warn!(
            attempt = attempt.failures,
            ?delay,
            error = %error,
            "resending a message the Pulsar broker rejected"
        );
        tokio::time::sleep_until(attempt.sent_at + delay).await;
        attempt.receipt = producer.send(attempt.message.clone());
        attempt.sent_at = Instant::now();
        pending.push_back(attempt);
    }
}

/// Whether a later send of the message can succeed. Like the Java client, the
/// adapter sends again after any rejection except a message the broker does
/// not allow or a terminated topic.
fn resendable(error: &ClientError) -> bool {
    match error {
        ClientError::SendRejected { code, .. } => ![
            ServerError::NotAllowedError as i32,
            ServerError::TopicTerminatedError as i32,
        ]
        .contains(code),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{Arc, Mutex as StdMutex},
        time::Duration,
    };

    fn rejected(code: ServerError) -> ClientError {
        ClientError::SendRejected {
            code: code as i32,
            message: "rejected".into(),
        }
    }

    /// Records each send by payload and answers it with the next scripted
    /// outcome for that payload, accepting it once the script runs out.
    #[derive(Clone, Default)]
    struct Script {
        outcomes: Arc<StdMutex<Vec<(&'static str, ServerError)>>>,
        sent: Arc<StdMutex<Vec<String>>>,
    }

    impl Script {
        fn reject(self, payload: &'static str, code: ServerError, times: usize) -> Self {
            self.outcomes
                .lock()
                .unwrap()
                .extend(std::iter::repeat_n((payload, code), times));
            self
        }

        fn sent(&self) -> Vec<String> {
            self.sent.lock().unwrap().clone()
        }
    }

    impl SendMessage for Script {
        fn send(&self, message: OutgoingMessage) -> BoxFuture<'static, Result<(), ClientError>> {
            let payload = String::from_utf8(message.payload.to_vec()).unwrap();
            self.sent.lock().unwrap().push(payload.clone());
            let mut outcomes = self.outcomes.lock().unwrap();
            let outcome = match outcomes.iter().position(|(p, _)| *p == payload) {
                Some(index) => Err(rejected(outcomes.remove(index).1)),
                None => Ok(()),
            };
            Box::pin(async move { outcome })
        }
    }

    fn message(payload: &'static str) -> OutgoingMessage {
        OutgoingMessage {
            payload: payload.as_bytes().to_vec().into(),
            uncompressed_size: payload.len() as u32,
            metadata: Default::default(),
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        }
    }

    fn retry(max_attempts: usize) -> RetryPolicy {
        RetryPolicy {
            max_attempts,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            ..RetryPolicy::default()
        }
    }

    #[tokio::test]
    async fn rejected_messages_are_sent_again_in_submission_order() {
        let script = Script::default()
            .reject("b", ServerError::PersistenceError, 2)
            .reject("c", ServerError::PersistenceError, 1);
        let resender = Resender::spawn(script.clone(), retry(3));
        let receipts: Vec<_> = ["a", "b", "c", "d"]
            .into_iter()
            .map(|payload| resender.send(message(payload)))
            .collect();
        for receipt in receipts {
            receipt.await.unwrap();
        }
        assert_eq!(script.sent(), ["a", "b", "c", "d", "b", "c", "b"]);
    }

    #[tokio::test]
    async fn a_message_rejected_on_every_attempt_fails() {
        let script = Script::default().reject("a", ServerError::PersistenceError, 3);
        let resender = Resender::spawn(script.clone(), retry(3));
        let error = resender.send(message("a")).await.unwrap_err();
        assert!(format!("{error:#}").contains("code=2"), "{error:#}");
        assert_eq!(script.sent(), ["a", "a", "a"]);
    }

    #[tokio::test]
    async fn a_message_the_broker_does_not_allow_is_not_sent_again() {
        let script = Script::default().reject("a", ServerError::NotAllowedError, 1);
        let resender = Resender::spawn(script.clone(), retry(3));
        assert!(resender.send(message("a")).await.is_err());
        assert!(resender.send(message("b")).await.is_ok());
        assert_eq!(script.sent(), ["a", "b"]);
    }

    #[tokio::test]
    async fn an_abandoned_message_is_not_sent_again() {
        let script = Script::default().reject("a", ServerError::PersistenceError, 1);
        let resender = Resender::spawn(script.clone(), retry(3));
        drop(resender.send(message("a")));
        resender.send(message("b")).await.unwrap();
        assert_eq!(script.sent(), ["a", "b"]);
    }
}
