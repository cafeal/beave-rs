//! State kept in the memory of the running process.
use super::{
    key::{MessageKey, StateKey},
    store::{Begin, StateContext, StateStore},
    value::{Final, Returned, State},
};
use crate::{error::BoxError, handler::Result, message::Position, shutdown::CancellationToken};
use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// A [`StateStore`] that keeps one value per key in the memory of the
/// process, for state that may be lost, such as recent IDs for
/// deduplication or counters that only need to be approximate.
///
/// The state is lost when the process stops. When the source revokes a
/// partition, the state stored by its deliveries is discarded on next use,
/// because another consumer may have changed it meanwhile; a key starts
/// again from `T::default()`. Within one process, a redelivery whose
/// [position](crate::SourceMessage::position) the stored state already
/// covers is acknowledged without running the handler.
///
/// Every key ever used keeps an entry. Clones share the stored values.
///
/// ```
/// use beavers::{
///     App, InMemorySink, IterSource, Result,
///     state::{InMemoryState, State, stateful},
/// };
///
/// async fn running_total(amount: u64, mut total: State<u64>) -> Result<u64> {
///     *total += amount;
///     Ok(*total)
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> std::result::Result<(), beavers::Error> {
/// let totals = InMemoryState::<u64>::new().key(|_: &u64| "all");
/// let sink = InMemorySink::default();
/// App::new()
///     .subscribe(
///         "totals",
///         IterSource::new([1, 2, 3]),
///         sink.clone(),
///         stateful(totals, running_total),
///     )
///     .run()
///     .await?;
/// assert_eq!(sink.values(), [1, 3, 6]);
/// # Ok(())
/// # }
/// ```
pub struct InMemoryState<T, K = MessageKey> {
    entries: Arc<Mutex<Entries<T>>>,
    key: K,
}

/// Each key's entry, locked by the delivery that holds the key.
type Entries<T> = HashMap<Vec<u8>, Arc<AsyncMutex<Entry<T>>>>;

struct Entry<T> {
    value: Option<T>,
    /// Position of the delivery whose commit last changed the entry.
    position: Option<Position>,
    /// Revocation token of that delivery; once cancelled, the entry is stale.
    owner: Option<CancellationToken>,
}

impl<T> Default for Entry<T> {
    fn default() -> Self {
        Self {
            value: None,
            position: None,
            owner: None,
        }
    }
}

impl<T> InMemoryState<T> {
    /// An empty store keeping state per
    /// [message key](crate::SourceMessage::message_key).
    pub fn new() -> Self {
        Self {
            entries: Arc::default(),
            key: MessageKey,
        }
    }
}

impl<T> Default for InMemoryState<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, K> InMemoryState<T, K> {
    /// Keeps state under the key `key` chooses instead; see [`StateKey`].
    pub fn key<K2>(self, key: K2) -> InMemoryState<T, K2> {
        InMemoryState {
            entries: self.entries,
            key,
        }
    }
}

impl<T: Clone, K> InMemoryState<T, K> {
    /// The value currently stored under `key`, if any.
    pub async fn get(&self, key: impl AsRef<[u8]>) -> Option<T> {
        let entry = self.entries.lock().unwrap().get(key.as_ref()).cloned()?;
        let entry = entry.lock().await;
        if is_stale(&entry) {
            return None;
        }
        entry.value.clone()
    }
}

impl<T, K: Clone> Clone for InMemoryState<T, K> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
            key: self.key.clone(),
        }
    }
}

impl<T, K: fmt::Debug> fmt::Debug for InMemoryState<T, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryState")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

fn is_stale<T>(entry: &Entry<T>) -> bool {
    entry
        .owner
        .as_ref()
        .is_some_and(CancellationToken::is_cancelled)
}

/// A held key of an [`InMemoryState`] from `begin` until `commit`.
pub struct InMemoryTxn<T> {
    entry: OwnedMutexGuard<Entry<T>>,
    returned: Returned<T>,
    position: Option<Position>,
    owner: Option<CancellationToken>,
}

impl<I, M, T, K> StateStore<I, M> for InMemoryState<T, K>
where
    I: Sync,
    T: Clone + Default + Send + Sync + 'static,
    K: StateKey<I, M>,
{
    type State = State<T>;
    type Txn = InMemoryTxn<T>;

    async fn begin(
        &self,
        input: &I,
        context: &StateContext,
    ) -> Result<Begin<State<T>, InMemoryTxn<T>>> {
        let key = self.key.key(input, context)?;
        let entry = self.entries.lock().unwrap().entry(key).or_default().clone();
        // Deliveries with one key take turns, so each sees the last commit.
        let mut entry = entry.lock_owned().await;
        if is_stale(&entry) {
            *entry = Entry::default();
        }
        if let (Some(position), Some(applied)) = (context.position(), &entry.position)
            && position.is_covered_by(applied)
        {
            return Ok(Begin::Applied);
        }
        let (state, returned) = State::new(entry.value.clone());
        Ok(Begin::Ready(
            state,
            InMemoryTxn {
                entry,
                returned,
                position: context.position().cloned(),
                owner: context.revocation().cloned(),
            },
        ))
    }

    async fn commit(&self, txn: InMemoryTxn<T>) -> std::result::Result<(), BoxError> {
        let InMemoryTxn {
            mut entry,
            returned,
            position,
            owner,
        } = txn;
        let value = match returned
            .take()
            .ok_or("the handler kept its state after returning")?
        {
            Final::Unchanged => return Ok(()),
            Final::Set(value) => Some(value),
            Final::Clear => None,
        };
        *entry = Entry {
            value,
            position,
            owner,
        };
        Ok(())
    }
}
