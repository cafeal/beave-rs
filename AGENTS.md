# Repository Instructions

Read and follow [CODE_GUIDELINES.md](CODE_GUIDELINES.md) before changing code.

## Required workflow

1. Inspect the existing traits and adjacent adapters before designing a public
   API.
2. Keep `mod.rs` limited to declarations and re-exports. Follow the broker
   adapter layout in `CODE_GUIDELINES.md`.
3. Keep platform-specific delivery semantics inside the adapter while making
   the public source, record, publish, sink, configuration, and codec concepts
   consistent across adapters.
4. Update tests and durable English documentation with the implementation.
5. Run these checks before reporting completion:

   ```text
   cargo fmt --check
   cargo test --offline
   cargo test --offline --all-features
   cargo clippy --offline --all-features --all-targets -- -D warnings
   cargo doc --offline --all-features --no-deps
   git diff --check
   ```

Live Kafka and Pulsar tests remain ignored by default. Run them when their
brokers are available and report separately whether they were executed.

## Repository constraints

- Write repository documentation and code comments in English.
- Do not add review notes, agent notes, progress logs, or temporary commentary
  to source files or documentation.
- Do not create compatibility aliases for unreleased APIs unless explicitly
  requested.
