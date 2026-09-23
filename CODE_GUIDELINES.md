# Code Guidelines

These guidelines define the repository's implementation standards. They apply
to contributors and automated coding agents.

## Module design

- Keep `mod.rs` focused on module declarations and public re-exports.
- Put source, sink, configuration, record, and protocol-specific state in
  separate files once an adapter contains more than one of those concerns.
- Prefer small private modules with explicit public exports over a large public
  module containing unrelated implementation details.
- Do not extract abstractions merely because two broker SDKs use similar words.
  Share a component only when its semantics and lifecycle are actually the same.

Broker adapters should normally use this layout:

```text
src/adapters/<broker>/
  mod.rs
  config.rs
  record.rs
  source.rs
  sink.rs
```

Additional private modules such as `progress.rs` or `consumer_task.rs` are
appropriate when they isolate a substantial state machine or lifecycle.

## Public adapter API

- Expose broker adapters below `beavers::adapters::<broker>`.
- Use `Source`, `SourceMessage`, and `Sink` as the common runtime boundary.
- Represent a received broker record separately from a record prepared for
  publication. Received records may contain immutable delivery metadata;
  publish records contain only user-controlled output fields.
- Make broker data models explicit in handler types. Do not silently translate
  keys, headers, properties, partitions, timestamps, or other metadata between
  platforms.
- Use `SourceName<C, T>` and `SinkName<C, T>` consistently when both the codec
  and payload type are part of an adapter's contract.
- Provide `new(config)` when `C: Default` and
  `with_codec(config, codec)` for configured or stateful codecs.
- Keep codec work in `decode` and `prepare`. A publish retry must reuse prepared
  bytes rather than encode the value again.

## Configuration and lifecycle

- Configuration types provide `new` for required values and `validate` for
  local validation that performs no network access.
- Construct broker clients lazily on the first `receive` or `publish`.
- Treat invalid configuration and malformed broker metadata as fatal errors.
  Report transient receive failures as retryable errors.
- A successful sink publication means the broker or local transport has
  accepted the prepared output according to that adapter's documented contract.
- ACK only after all required output publication succeeds. Dropping a delivery
  must not acknowledge it.
- Make `close` safe to call more than once and ensure clones share closure state
  where the sink itself is cloneable.
- Preserve cancellation safety at every async boundary. Document any SDK
  operation whose cancellation behavior affects delivery guarantees.

## Broker semantics

- Keep offset tracking, rebalances, acknowledgements, transactions, routing,
  and reconnect behavior inside the owning adapter.
- Preserve nullable payloads when the broker supports them; do not assign
  sentinel meanings to empty bytes, empty strings, or `None`.
- Do not claim exactly-once processing unless publication and acknowledgement
  are part of one broker transaction implemented by the adapter.
- Avoid implicit metadata inheritance. A middleware or explicit conversion may
  map metadata when the application requests it.

## Quality and documentation

- Add focused unit tests for state machines and contract tests for public API
  behavior. Keep live broker tests ignored by default and runnable explicitly.
- Run formatting, the default test suite, all-feature tests, Clippy with
  warnings denied, and documentation generation before considering a change
  complete.
- Keep public documentation in English and update it in the same change as an
  API or behavior change.
- Documentation and comments describe current contracts, rationale that remains
  relevant, and real limitations. Do not leave review history, temporary notes,
  agent activity, or implementation worklogs in the repository.
- Keep the root `PLAN.md` out of commits. The maintained roadmap is
  `docs/plan.md`.
