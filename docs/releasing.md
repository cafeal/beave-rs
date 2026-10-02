# Versioning and releases

beave.rs is published on crates.io as the `beavers` crate. Releases follow
[Semantic Versioning](https://semver.org/) as Cargo interprets it.

## Version numbers

While the major version is `0`, the minor version is the breaking-change
number:

| Change | Before 1.0 | From 1.0 |
|---|---|---|
| Breaking change | `0.x.y` → `0.(x+1).0` | `x.y.z` → `(x+1).0.0` |
| Backward-compatible addition | `0.x.y` → `0.x.(y+1)` | `x.y.z` → `x.(y+1).0` |
| Bug fix | `0.x.y` → `0.x.(y+1)` | `x.y.z` → `x.y.(z+1)` |

A dependency requirement of `beavers = "0.1"` therefore never picks up a
breaking change. The first published release is `0.1.0`.

Pre-release versions such as `0.2.0-rc.1` may be published to let applications
try a breaking release before it becomes the default.

## Compatibility contract

Beyond Rust signatures, the following are part of the compatibility contract
and change only in a breaking release:

- Public items reachable from the crate root, including trait methods, public
  fields, enum variants, and the bounds on generic parameters.
- Cargo feature names and the items each feature enables.
- Delivery behavior that applications rely on: when a delivery is acknowledged,
  retried, routed to a dead-letter destination, or stops its subscription.
- Data visible outside the process: header and property names written by the
  adapters, dead-letter metadata, trace-context propagation fields, metric
  names and labels, span names and fields, and the health endpoint paths.
- Types from broker client crates, such as `rdkafka`, that appear in the
  public API. Upgrading such a dependency to a semver-incompatible version is a
  breaking change for `beavers`.

Fixing behavior that contradicts the documented contract is a bug fix, even
when an application could observe the difference.

## Minimum supported Rust version

The minimum supported Rust version (MSRV) is declared as `rust-version` in
`Cargo.toml` and checked in CI with every Cargo feature enabled. Raising it is
allowed in a minor release before 1.0 and is called out in the release notes.
The MSRV is not raised merely to adopt newer language features; it follows the
requirements of the broker client dependencies.

## Changelog

`CHANGELOG.md` follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Every pull request that changes behavior visible to applications adds an entry
under `Unreleased`, in the `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`,
or `Security` section. Breaking changes are marked with **Breaking** and say
what applications must change.

## Release procedure

1. Choose the version from the changes under `Unreleased` and the table above.
2. Rename `Unreleased` in `CHANGELOG.md` to the version and release date, and
   start a new empty `Unreleased` section.
3. Set `version` in `Cargo.toml` and run `cargo check` to update `Cargo.lock`.
4. Merge these changes to `main` and wait for CI, including the package check.
5. From an up-to-date `main`, run `cargo publish`.
6. Tag the published commit as `vX.Y.Z`, push the tag, and create a GitHub
   release whose notes are the version's changelog section.

A published version cannot be replaced. If a release is broken, yank it with
`cargo yank --version X.Y.Z` and publish a fixed patch release.
