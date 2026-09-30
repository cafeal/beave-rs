//! Bounded exponential-backoff policy shared by independent retry domains.

use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hasher},
    time::Duration,
};

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Includes the initial attempt; must be at least one.
    pub max_attempts: usize,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub jitter: Jitter,
}

/// Randomization applied to the capped exponential delay before each retry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Jitter {
    /// Wait exactly the capped exponential delay.
    #[default]
    None,
    /// Wait a uniformly random duration between zero and the capped exponential delay.
    Full,
    /// Wait half of the capped exponential delay plus a uniformly random share of the other half.
    Equal,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            jitter: Jitter::None,
        }
    }
}

impl RetryPolicy {
    /// Delay before the next attempt after `failures` consecutive failed attempts.
    pub(crate) fn delay(&self, failures: usize) -> Duration {
        let delay = self
            .initial_delay
            .saturating_mul(2u32.saturating_pow(failures.saturating_sub(1).min(31) as u32))
            .min(self.max_delay);
        match self.jitter {
            Jitter::None => delay,
            Jitter::Full => random_up_to(delay),
            Jitter::Equal => delay / 2 + random_up_to(delay - delay / 2),
        }
    }

    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.max_attempts > 0, "max_attempts must be positive");
        Ok(())
    }
}

/// Uniform duration in `[0, max]`. Retry spreading does not need a cryptographic generator,
/// so the randomly keyed standard hasher avoids an extra dependency.
fn random_up_to(max: Duration) -> Duration {
    let nanos = max.as_nanos().min(u64::MAX as u128) as u64;
    if nanos == 0 {
        return Duration::ZERO;
    }
    let random = RandomState::new().build_hasher().finish();
    Duration::from_nanos(((random as u128 * (nanos as u128 + 1)) >> 64) as u64)
}

#[cfg(test)]
mod tests {
    use super::{Jitter, RetryPolicy};
    use std::time::Duration;

    fn policy(jitter: Jitter) -> RetryPolicy {
        RetryPolicy {
            max_attempts: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(250),
            jitter,
        }
    }

    #[test]
    fn exponential_delay_is_capped() {
        let policy = policy(Jitter::None);
        assert_eq!(policy.delay(1), Duration::from_millis(100));
        assert_eq!(policy.delay(2), Duration::from_millis(200));
        assert_eq!(policy.delay(3), Duration::from_millis(250));
        assert_eq!(policy.delay(usize::MAX), Duration::from_millis(250));
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let capped = policy(Jitter::None);
        for (jitter, half) in [(Jitter::Full, false), (Jitter::Equal, true)] {
            let policy = policy(jitter);
            for failures in 1..=4 {
                let max = capped.delay(failures);
                let min = if half { max / 2 } else { Duration::ZERO };
                for _ in 0..100 {
                    let delay = policy.delay(failures);
                    assert!(min <= delay && delay <= max, "{jitter:?}: {delay:?}");
                }
            }
        }
    }

    #[test]
    fn jitter_varies_delays() {
        let policy = policy(Jitter::Full);
        let delays: Vec<_> = (0..32).map(|_| policy.delay(3)).collect();
        assert!(delays.iter().any(|delay| *delay != delays[0]));
    }
}
