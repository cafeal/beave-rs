//! One stored value per key, as the handler sees it.
use std::{
    fmt,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};

/// The value stored under one key, passed to a [stateful](super::stateful)
/// handler by value and changed through `DerefMut`.
///
/// A key without a stored value starts as `T::default()`. When the handler
/// returns, the runtime takes back the value, publishes the outputs, and
/// then stores it, unless the handler only read it. Keeping the `State` after
/// the handler returns, for example in a spawned task, fails the commit.
///
/// ```
/// use beavers::{Emit, Result, state::State};
///
/// #[derive(Default)]
/// struct Count {
///     n: u64,
/// }
///
/// async fn count(word: String, mut count: State<Count>) -> Result<Emit<String>> {
///     count.n += 1;
///     Ok(if count.n == 3 { Emit::One(word) } else { Emit::None })
/// }
/// ```
pub struct State<T> {
    value: Option<T>,
    is_new: bool,
    change: Change,
    slot: Arc<Mutex<Option<Final<T>>>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Change {
    None,
    Set,
    Clear,
}

/// What a dropped [`State`] leaves for the store to commit.
pub(crate) enum Final<T> {
    Unchanged,
    Set(T),
    Clear,
}

/// The store's side of a [`State`]: receives the value when the handler
/// drops it.
pub(crate) struct Returned<T>(Arc<Mutex<Option<Final<T>>>>);

impl<T> Returned<T> {
    /// The handler's change, or `None` while the handler still holds the
    /// `State`.
    pub(crate) fn take(&self) -> Option<Final<T>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}

impl<T: Default> State<T> {
    /// A handle starting from `stored`, the value currently stored under the
    /// key, if any.
    pub(crate) fn new(stored: Option<T>) -> (Self, Returned<T>) {
        let slot = Arc::new(Mutex::new(None));
        let is_new = stored.is_none();
        let state = Self {
            value: Some(stored.unwrap_or_default()),
            is_new,
            change: Change::None,
            slot: slot.clone(),
        };
        (state, Returned(slot))
    }

    /// Whether no value was stored under the key before this delivery.
    pub fn is_new(&self) -> bool {
        self.is_new
    }

    /// Removes the key's value when the delivery commits and resets this
    /// handle to `T::default()`. Changing the value afterwards stores it
    /// again.
    pub fn clear(&mut self) {
        self.value = Some(T::default());
        self.change = Change::Clear;
    }
}

impl<T> Deref for State<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.value
            .as_ref()
            .expect("state value is present until drop")
    }
}

impl<T> DerefMut for State<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.change = Change::Set;
        self.value
            .as_mut()
            .expect("state value is present until drop")
    }
}

impl<T> Drop for State<T> {
    fn drop(&mut self) {
        let value = self
            .value
            .take()
            .expect("state value is present until drop");
        let change = match self.change {
            Change::None => Final::Unchanged,
            Change::Set => Final::Set(value),
            Change::Clear => Final::Clear,
        };
        *self.slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(change);
    }
}

impl<T: fmt::Debug> fmt::Debug for State<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("State")
            .field("value", &self.value)
            .field("is_new", &self.is_new)
            .finish_non_exhaustive()
    }
}
