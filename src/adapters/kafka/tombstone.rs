use super::record::{KafkaPublish, KafkaRecord};
use crate::{
    handler::{Emit, HandlerError, Result},
    middleware::Middleware,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Policy {
    Reject,
    Skip,
    Propagate,
}

/// Kafka middleware that decides how records with a null value (tombstones)
/// are handled before the handler runs.
///
/// Records with a value always reach the handler. A tombstone never reaches
/// the handler:
///
/// - `reject` routes it to the DLQ, or stops without ACK when none is
///   configured;
/// - `skip` acknowledges it without publishing output;
/// - `propagate` publishes a tombstone with the same key, which suits
///   forwarding between compacted topics that share a key space. A tombstone
///   without a key is rejected.
///
/// Propagated tombstones pass through the other middleware's `map`, so
/// `KafkaInherit` can still add inherited headers.
#[derive(Clone, Copy, Debug)]
pub struct KafkaTombstones {
    policy: Policy,
}

impl KafkaTombstones {
    pub fn reject() -> Self {
        Self {
            policy: Policy::Reject,
        }
    }

    pub fn skip() -> Self {
        Self {
            policy: Policy::Skip,
        }
    }

    pub fn propagate() -> Self {
        Self {
            policy: Policy::Propagate,
        }
    }
}

impl<I: 'static, O: 'static> Middleware<KafkaRecord<I>, KafkaPublish<O>> for KafkaTombstones {
    fn intercept(&self, input: &KafkaRecord<I>) -> Result<Option<Emit<KafkaPublish<O>>>> {
        if input.value.is_some() {
            return Ok(None);
        }
        match (self.policy, &input.key) {
            (Policy::Skip, _) => Ok(Some(Emit::None)),
            (Policy::Propagate, Some(key)) => {
                Ok(Some(Emit::One(KafkaPublish::tombstone(key.clone()))))
            }
            (Policy::Propagate, None) => Err(HandlerError::Reject(anyhow::anyhow!(
                "Kafka tombstone has no key to propagate"
            ))),
            (Policy::Reject, _) => Err(HandlerError::Reject(anyhow::anyhow!(
                "Kafka record has a null value"
            ))),
        }
    }
}
