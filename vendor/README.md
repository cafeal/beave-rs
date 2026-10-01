# Vendored crates

## magnetar-proto

`magnetar-proto/` is the published `magnetar-proto` 1.7.2 crate, without its
tests and development dependencies, with one change in `src/conn.rs`. The root
`Cargo.toml` replaces the crates.io release with it through `[patch.crates-io]`.

The release acknowledges a message of a producer batch through a bitset that
the consumer keeps for the whole session: each acknowledgement clears its batch
index in that bitset and sends the result. Transactional acknowledgements share
the bitset, so a transaction also acknowledges the indexes earlier
transactions acknowledged. Against a Pulsar 4.0 broker this fails in two ways:

- While an earlier transaction is pending or after it commits, the broker
  rejects the overlapping acknowledgement with `TransactionConflictException`,
  so every message of a batch after the first fails to commit.
- When an earlier transaction aborts, a later transaction acknowledges the
  aborted index as well, and that message is never redelivered.

The patched client sends a transactional acknowledgement of a batched message
with a bitset of its own index only, as Pulsar's Java client does. Remove this
directory and the patch once a `magnetar-proto` release behaves the same way;
the ignored `transactional_pipeline_commits_each_message_of_a_batch` and
`aborted_batch_index_acknowledgement_is_redelivered` tests in
`tests/pulsar.rs` cover the behavior.
