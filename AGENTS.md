# Repository Instructions

Read and follow [CODE_GUIDELINES.md](CODE_GUIDELINES.md) before changing code.
Use [docs/plan.md](docs/plan.md) as the source of truth for planned work and
unresolved design decisions.

## Required workflow

1. Inspect the existing traits and adjacent adapters before designing a public
   API.
2. Keep `mod.rs` limited to declarations and re-exports. Follow the broker
   adapter layout in `CODE_GUIDELINES.md`.
3. Keep platform-specific delivery semantics inside the adapter while making
   the public source, record, publish, sink, configuration, and codec concepts
   consistent across adapters.
4. Update `docs/plan.md` as planning and implementation progress. Remove
   completed work from the plan once its behavior is covered by the appropriate
   architecture, runtime, adapter, codec, or API documentation.
5. Update tests and durable English documentation with the implementation.
6. Run these checks before reporting completion:

   ```text
   cargo fmt --check
   cargo test --offline
   cargo test --offline --all-features
   cargo clippy --offline --all-features --all-targets -- -D warnings
   cargo doc --offline --all-features --no-deps
   cargo deny check
   typos
   ```

   Install the last two tools with `cargo install --locked cargo-deny typos-cli`.
   `deny.toml` lists the allowed dependency licenses and `_typos.toml` holds the
   spelling exceptions.

Live Kafka and Pulsar tests remain ignored by default. Start the local brokers
with `docker compose up -d --wait` when Docker is available, run
`cargo test-live`, and report separately whether they were executed. See
[docs/development.md](docs/development.md).

## Repository constraints

- Write repository documentation and code comments in English.
- Do not add review notes, agent notes, progress logs, or temporary commentary
  to source files or documentation.
- Do not create compatibility aliases for unreleased APIs unless explicitly
  requested.
