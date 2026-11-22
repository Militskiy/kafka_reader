# Architecture

This describes the **Kafka Reader 0.3.0** implementation. Check the referenced
source when changing it; [TEST-REPORT.md](../TEST-REPORT.md) records executed
verification and platform limits.

## Product and runtime

Kafka Reader is a native desktop inspector for someone who can reach a Kafka
broker and read a particular topic but may lack a web UI or consumer-group
permissions. It registers exact topic names locally, reads messages, inspects
metadata, and exports retained records. It does not produce records, administer
the cluster, or discover all topics. A headless CLI uses the same reader engine.

[Cargo.toml](../Cargo.toml) specifies Rust edition 2024, minimum Rust 1.95,
egui/eframe 0.37.0 and rust-rdkafka 0.39.0. [Cargo.lock](../Cargo.lock) pins the
complete graph, including librdkafka 2.16.0 through rdkafka-sys. The delivered
release used Rust 1.100.0. Rendering uses eframe's Glow/OpenGL backend; Linux
enables X11, so Wayland sessions require XWayland. There is no embedded browser.

## Source map

| File | Responsibility |
|---|---|
| [src/main.rs](../src/main.rs) | CLI parsing, native application startup, headless event draining, self-test |
| [src/app.rs](../src/app.rs) | Workspace UI, active/cached topic views, selection, reader control, formatting jobs, GUI export |
| [src/app/presentation.rs](../src/app/presentation.rs) | Numeric sorting, timestamp availability, Auto/JSON/Text/Hex payload display |
| [src/app/dialogs.rs](../src/app/dialogs.rs) | Separate native configuration windows, drafts, exact-topic tests, save/cancel |
| [src/workspace.rs](../src/workspace.rs) | Saved profiles/settings/tabs, validation, sample migration, credential filtering, atomic writes |
| [src/reader.rs](../src/reader.rs) | Request execution, configuration policy, bounds, worker controls and bounded events |
| [src/direct_consumer.rs](../src/direct_consumer.rs) | Narrow unsafe adapter for native partition queues and owned message copies |
| [src/model.rs](../src/model.rs) | Raw records, buffer accounting, offset calculations, previews, properties and JSONL |
| [src/app/smoke.rs](../src/app/smoke.rs) | Explicit synthetic GUI/reopen verification hooks |
| [packaging/](../packaging/) and [qa/integration/](../qa/integration/) | Platform packaging and disposable verification fixtures |

## Reading and native ownership

The UI combines a saved server's properties with its session credentials and
builds a `Request`. `Session::start` moves that request to a worker thread. The
worker emits metadata, immutable `Arc<Record>` values, progress and state
events. The GUI drains these into the active `TopicView`; the CLI drains the
same events directly into its summary and optional JSONL output.

`safe_properties` removes `group.id`, `group.instance.id` and unsupported or
executable extension hooks. It forces modern and legacy automatic commits,
automatic offset storage, and automatic topic creation off. These settings
override imported values. The low-level topic configuration also disables
legacy commits and uses broker offset storage to avoid local offset files.

`BaseConsumer` owns the native client and supplies metadata, watermarks,
timestamp lookup and pause/resume. Record delivery uses
`rd_kafka_consume_start_queue` and `rd_kafka_consume_queue` at explicit offsets.
This choice is deliberate: a high-level consumer with a group ID can request a
coordinator even when partitions are manually assigned, breaking topic-only
authorization. Replacing this path with `subscribe` or ordinary grouped
consumption would change a central product guarantee.

`DirectConsumer` borrows its owning client. Native topic/queue handles remain on
that worker; message key/value/header bytes are copied into Rust-owned data
before native messages are destroyed. Drop stops started partitions, then
releases the topic and queue while the owner still exists. Preserve that
lifetime and cleanup order around the unsafe boundary.

## Read boundaries and completion

The worker fetches metadata for the supplied topic only, validates selected
partitions, and captures their beginning and exclusive readable-end offsets.
Those bounds determine earliest, recent-window, explicit-offset, timestamp and
latest starts. A recent window counts offset positions per partition, not
existing records. A timestamp with no matching offset starts at the captured
end; it does not rewind to the beginning.

Finite reads exclude records beyond each captured end. Offsets can contain gaps;
completion follows delivered positions and native partition EOF offsets instead
of assuming that every offset exists. These are per-partition snapshots, not a
single transactionally synchronized snapshot across the whole topic. Follow
mode continues beyond the captured bounds.

The default isolation is `read_committed`, with an explicit advanced-property
override allowed. The pinned librdkafka path expects readable-end queries to
honor isolation and return the last stable offset for committed reads. An open
transaction must not turn a finite read into a wait for the ordinary high
watermark. Dependency or bounds changes need the transaction cases in the
existing broker fixture. Record/time limits can finish earlier; initialization
and paused time are excluded from the active reading-time budget.

## UI state, concurrency and memory

`Workspace` holds saved navigation; `TopicView` holds session data. Up to eight
topic tabs share one active reader. A reader is considered active while its
worker runs **or its event queue still contains events**. Switching context
before the queue drains could place messages in the wrong tab.

Idle views cache records, filters, selected-record references, sort/format
choices and read controls. Editing a server clears its affected view caches so
old data is not relabeled with a changed connection. Add/Edit server and App
settings are real native viewports with drafts; Cancel discards the draft, and
saving a server does not connect it.

Records stay in an arrival-ordered `VecDeque`. Filtering and numeric sorting
produce a separate vector of `Arc` references; they do not mutate retention
order. Selection uses record identity rather than a sorted row index. Time
defaults to descending, missing times stay last, and partition/offset ties are
deterministic. GUI exports use this visible sorted vector. CLI exports retain
received order. No auto-scroll behavior is attached to Follow.

Formatting runs off the UI thread. Each result carries its originating `Tab`
and a generation number. `deliver_preview` accepts it only into the matching
view with the current generation, preventing delayed results from replacing a
new selection. Eviction clears a removed selection and invalidates its pending
generation.

The default combined retention budget is 10,000 records / 64 MiB of raw
key/value/header data. Settings allow 100–50,000 records and 1–512 MiB. Each
buffer enforces its bounds; `ReaderApp::enforce_budget` additionally trims the
combined total, preferring older inactive caches during global eviction. The
worker channel allows 256 events and 16 MiB of queued raw record bytes; larger
individual records are skipped. These are raw-data budgets, not process-memory
limits: UI text, native fetch buffers and export snapshots add overhead.

## Bytes, presentation and export

`Record` stores key/value as `Option<Vec<u8>>`. `None`, an empty byte vector and
invalid UTF-8 are distinct. Headers are an ordered vector, preserving duplicate
names and null/empty values. Do not replace headers with a map or retain only
decoded text.

The inspector has Value, Key, Headers and Metadata tabs. Value and Key each
offer Auto, JSON, Text and optional Hex. Auto formats JSON or shows readable
text; binary data directs the user to Hex. Preview limits are 256 KiB of
text/JSON input, 512 KiB of formatted JSON, 8 KiB of hex and 500 displayed
headers. Formatting never changes raw records.

`Record::json` defines `kafka-reader-jsonl-v1`: metadata, Base64 key/value bytes,
valid UTF-8 projections and ordered headers. Base64 is authoritative: JSON
`null` represents a null array, while `""` represents an empty one. GUI export
snapshots visible retained rows and refuses an existing filename; CLI export
streams the read and overwrites its explicitly supplied output file.

## Persistence and package identity

`Store` loads/saves validated `workspace.json` through temporary-file replacement.
It persists profiles, registered topics, favorites, tab identities and settings;
records, view filters/sorts and session credentials are not serialized.
Usernames, passwords, tokens and inline private-key material stay in memory;
certificate/key paths may be saved. Invalid existing workspaces are reported
and protected from automatic overwrite by the app's persistence-error state.

Normal startup is empty or restores saved real profiles without connecting.
Migration removes profiles marked `offline` and their tabs, never profiles
merely named Demo. Loading cleans the in-memory workspace; the cleaned state is
written on the next normal save. Only explicit GUI QA creates a synthetic
fixture, and Store excludes it when saving.

The displayed name is **Kafka Reader** and the distributed Windows file is
`kafka-reader.exe`. The internal Cargo target and existing app-data directory
remain `kafka-reader-rust` for compatibility. `--data-dir` selects an explicit
workspace directory. Windows portability relies on static native dependencies,
[.cargo/config.toml](../.cargo/config.toml), [build.rs](../build.rs), and the
packaging scripts; preserve their link behavior. Import inspection establishes
dependency packaging, not successful Windows execution; consult the test report
before making platform claims.
