//! Platform-neutral handling of tombstones: received records whose value is
//! null, which compacted topics and change-data-capture streams use to delete
//! a key.
use crate::{
    handler::{Emit, HandlerError, Result},
    middleware::Middleware,
};

/// A received record type that can carry a tombstone.
pub trait TombstoneRecord {
    fn is_tombstone(&self) -> bool;
}

/// A publish type that can express a tombstone derived from a received record.
///
/// Implemented only by platforms whose sink can publish a null value, so
/// `Tombstones::propagate()` does not compile for a sink that cannot delete.
pub trait TombstonePublish<I>: Sized {
    /// Returns `HandlerError::Reject` when the input cannot be propagated,
    /// such as a tombstone without a key.
    fn tombstone(input: &I) -> Result<Self>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Policy {
    Reject,
    Skip,
}

/// Middleware that decides how tombstones are handled before the handler runs.
///
/// Records with a value always reach the handler; a tombstone never does.
/// `reject` routes it to the DLQ, or stops without ACK when none is configured.
/// `skip` acknowledges it without output. `propagate` returns
/// [`PropagateTombstones`], which publishes a tombstone for the same key.
#[derive(Clone, Copy, Debug)]
pub struct Tombstones {
    policy: Policy,
}

impl Tombstones {
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

    pub fn propagate() -> PropagateTombstones {
        PropagateTombstones
    }
}

impl<I: TombstoneRecord + 'static, O: 'static> Middleware<I, O> for Tombstones {
    fn intercept(&self, input: &I) -> Result<Option<Emit<O>>> {
        if !input.is_tombstone() {
            return Ok(None);
        }
        match self.policy {
            Policy::Reject => Err(HandlerError::Reject(anyhow::anyhow!(
                "tombstone rejected before the handler"
            ))),
            Policy::Skip => Ok(Some(Emit::None)),
        }
    }
}

/// Middleware that publishes a tombstone for each received tombstone.
///
/// Suitable when the output shares the input's key space, such as forwarding
/// between compacted topics. The propagated tombstone still passes through the
/// other middleware's `map`, so inheritance middleware can add metadata.
#[derive(Clone, Copy, Debug)]
pub struct PropagateTombstones;

impl<I, O> Middleware<I, O> for PropagateTombstones
where
    I: TombstoneRecord + 'static,
    O: TombstonePublish<I> + 'static,
{
    fn intercept(&self, input: &I) -> Result<Option<Emit<O>>> {
        if !input.is_tombstone() {
            return Ok(None);
        }
        O::tombstone(input).map(|output| Some(Emit::One(output)))
    }
}
