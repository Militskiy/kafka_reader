# Kafka Reader — instructions for coding agents

## Purpose and scope

This repository contains the native **Kafka Reader** desktop application and
its headless CLI. It lets a user inspect an exact Kafka topic with broker/topic
access, even without a Kafka web UI or consumer-group permissions. The current
application version is **0.3.0**. This is the Rust implementation; earlier Java
and C# prototypes are separate projects.

Treat the user's current task as the work to perform. Preserve the decisions
below unless that task explicitly changes them. The known limitations in the
project notes are context, not an instruction to implement a backlog.

## Read first

1. Read [docs/PROJECT_CONTEXT.md](docs/PROJECT_CONTEXT.md) for accepted product
   decisions, current capabilities, and verification limits.
2. Read the relevant sections of [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and
   [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) before changing their area.
3. Use [README.md](README.md) and [QUICKSTART.md](QUICKSTART.md) for user-facing
   behavior. [TEST-REPORT.md](TEST-REPORT.md) records executed release checks;
   do not turn those historical results into claims about new builds.

Inspect the actual implementation and current working changes before editing.
Use repository-relative paths; the original development machine and its
downloaded toolchains are not part of this repository.

## Behaviors to preserve

- **Read-only, group-free Kafka access.** Keep `group.id` removed and automatic
  commits, legacy auto commits, automatic offset storage, and topic auto-creation disabled.
  A high-level consumer with manual assignment is not a drop-in replacement:
  it can still discover a coordinator and require group ACLs. Inspect
  `src/reader.rs` and `src/direct_consumer.rs` before changing consumption.
- **Exact bytes and read boundaries.** Preserve null versus empty keys/values,
  ordered duplicate headers, raw Base64 exports, finite exclusive end bounds,
  offset gaps, and timestamp seeks. `read_committed` is the current default;
  an explicit advanced-property override is supported. Read the LSO explanation
  in the architecture notes before changing bounds or completion conditions.
- **Responsive, bounded UI.** Network reads and preview formatting belong off
  the UI thread. One active reader, including queued events, guards topic/server
  switches. Up to eight tabs share a retention budget. Preview results are
  scoped to their originating tab and generation; retain those checks.
- **Native developer-tool UX.** Keep the compact Bruno-inspired server/topic
  workspace, separate native Add/Edit server window, and separate App settings.
  Connection details belong in Connection, Authentication, TLS, and Advanced
  tabs; App settings contains application preferences.
- **Current message UI.** No normal demo server, public `--demo`, auto-scroll,
  or Rust branding in the app name. Time, Partition, and Offset are separate
  sortable numeric columns; newest timestamps are first by default and missing
  times stay last. Sort retained references without changing eviction order or
  selected-record identity. The inspector has Value, Key, Headers, Metadata;
  Hex is an optional Value/Key format, never an automatic duplicate key dump.
- **Compatible persistence.** Startup never connects automatically. Preserve
  real profiles, favorites, and preferences. Session credentials and message
  bodies are not persisted. Keep malformed workspace files intact. Migrate old
  samples by their explicit offline marker, not a server name or ID.
- **Portable Windows delivery.** Preserve the genuine single native EXE, static
  Kafka/TLS/compression linkage, and required GNU runtime flags. Keep
  `.cargo/config.toml` and `build.rs`. The public name is Kafka Reader, while the
  Cargo crate and existing settings directory remain `kafka-reader-rust` for
  compatibility. Do not rename those internal paths as cosmetic cleanup.

## Working and verification

Use the existing modules and locked dependencies. Avoid framework rewrites,
dependency upgrades, unrelated cleanup, or new product features unless needed
for the requested change. Preserve user edits and real workspace data.

Use subagents when independent implementation or review work helps. Give them
disjoint ownership, and keep one owner for builds and release packaging to
avoid overlapping edits and Cargo target-directory contention.

On a configured Linux development host, the normal code-check sequence is:

```bash
cargo fmt --check
cargo test --locked --release
cargo build --locked --release
target/release/kafka-reader-rust --self-test
```

Windows uses the GNU-target workflow in
[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md), rather than assuming the host's
default Rust target matches the release. First builds compile native libraries
and need C/C++ tools, CMake, Perl, and dependency downloads.

Choose verification for the actual change. Documentation-only changes need
link/archive checks, not an application rebuild. UI changes need a native
desktop inspection; reader/FFI/bounds changes need the relevant live-broker
fixture checks. Use isolated test data. Do not contact saved user brokers or
alter production Kafka state as an incidental test. Never disable a failing
test merely to get a green result. Report unavailable checks precisely.

## Delivering changes

Update the relevant project notes when behavior, commands, or decisions change.
Keep `AGENTS.md` concise and put detailed context in the linked documents.
For releases, update versioned packaging paths, the changelog, evidence, and
checksums together. The source archive uses an explicit allowlist in
`packaging/package.py`; ensure new guidance files remain included.

Summarize what changed, why, checks actually run, and remaining limitations.
Do not claim Windows execution from cross-compilation, import inspection, or
Linux GUI results. Do not label new screenshots or binaries with old evidence.
