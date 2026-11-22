# Project context and current state

Snapshot: **22 November 2026**, application **0.4.0**. This is the durable
handoff for future development. Read the current code when making changes;
update this document when an accepted decision or verified capability changes.

## The problem this app solves

The user needs to inspect a Kafka topic directly when they have topic access
but cannot use a Kafka web UI. They want a native desktop tool that feels like
a compact developer workspace, with portable Windows delivery and available
source code. This repository is the Rust implementation of that tool. Earlier
Java and C# experiments are separate and are not included here.

The product is **Kafka Reader**. The user explicitly removed the implementation
language from the app's display name. The internal Cargo package and settings
folder remain `kafka-reader-rust` to preserve build references and existing data.
Keep that distinction when editing labels, packaging, commands, and migrations.

## Accepted product decisions

| Area | Current decision |
|---|---|
| Overall UX | Bruno is the preferred inspiration, ahead of Postman. Keep a compact server/topic workspace; use the supplied direction as inspiration, not a pixel-for-pixel copy. |
| Appearance | Restrained dark theme with amber accents, optional light theme, compact rows and adjustable font size. |
| Language | The desktop defaults to the system display language and supports English and Russian. A manual choice in App settings applies immediately and persists in the workspace. |
| Navigation | Expandable saved servers/topics, search, Favorites, and closable topic tabs. Exact topic registration works without cluster-wide discovery. |
| Server configuration | Separate native Add/Edit server window with Connection, Authentication, TLS, Advanced tabs and draft Save/Cancel behavior. |
| App settings | Separate native window for appearance, reading defaults, timestamp display and retention limits. Keep server credentials in server configuration. |
| Startup | No built-in demo server. Restore real saved configuration without connecting or reading automatically. |
| Message ordering | Separate Time, Partition, Offset columns. Click to sort numerically, click again to reverse; newest timestamps first by default and missing timestamps last. |
| Scrolling | No auto-scroll control or forced scroll-to-bottom. Follow still receives future records. |
| Inspector | Value, Key, Headers, Metadata tabs. Auto/JSON/Text/Hex are optional Value/Key formats; no standalone Hex tab or automatic second key dump. |
| Packaging | A genuine portable Windows x64 EXE with statically linked native dependencies. No installer or companion third-party runtime DLLs required by the delivered build. |

These are current decisions to preserve during unrelated changes. A later
explicit user request can change them; this document is not a permanent veto.

## Implemented capabilities

- A native egui/eframe desktop and a headless reader CLI share the same reader
  and raw record model. The app itself requires no Node, Java, .NET or browser
  runtime. The optional local broker test fixture uses a JDK.
- Exact-topic metadata, partition bounds, earliest/recent/offset/timestamp/latest
  starting modes, finite reads, following, pause/resume/stop, and local filters.
- Direct partition consumption without a group ID or offset commits. Topic-only
  access is a core use case, not an accidental side effect.
- Up to eight tabs with in-memory record/filter/selection/sort state. One reader
  runs at a time and all tabs share a configured retention budget.
- Preserved raw key/value bytes and ordered headers, including null, empty,
  duplicate and binary cases. GUI JSONL export follows displayed filtering and
  sorting; CLI export follows arrival order.
- Persistent real server profiles, exact topic names, favorites, open tab
  identities and application settings. Credentials and record bodies stay out
  of the saved workspace. Current workspace schema version is **2**.
- PLAINTEXT, SSL, SASL_PLAINTEXT and SASL_SSL configuration, with PLAIN and
  SCRAM-SHA-256/512 mechanisms and PEM certificate paths. Configuration support
  is separate from the live-test coverage listed below.

Details and boundaries are in [ARCHITECTURE.md](ARCHITECTURE.md) and the
[README](../README.md).

## Important history

Version 0.2.0 introduced the Bruno-inspired workspace, independent configuration
windows, persistence, topic caches and a genuine static Windows EXE.
Version 0.3.0 removed the normal demo, removed Rust branding, replaced
auto-scroll with numeric column sorting and simplified the inspector.
Version 0.4.0 added the Russian interface and system language detection,
protected concurrent workspace saves and fixed several exact-topic read and
export edge cases. Its Windows executable carries Alexey Militskiy's self-signed
Authenticode certificate with a DigiCert timestamp. See the
[changelog](../CHANGELOG.md) and [signature details](../QUICKSTART.md#verify-the-windows-download).

Old sample profiles are removed using their `offline` marker when a saved
workspace is loaded. The cleaned state reaches disk on the next normal save.
Do not remove profiles by the word "demo" or by server ID. A real connection
can legitimately have a sample-like name.

Synthetic record generation remains for explicit `--gui-smoke` verification and
unit/self checks. Populated screenshots in
[screenshots/v0.3.0](screenshots/v0.3.0/) show this temporary QA fixture.
They are layout references, not a request to put a demo server back into normal
startup. The empty/reopened screenshots show ordinary behavior.

## Verification baseline and limits

For version 0.4.0, consult the [release test report](../TEST-REPORT.md).

The delivered 0.3.0 release has recorded results for **33 unit tests, 9
self-checks, 47 GUI assertions, 15 reopen assertions and 12 independent
window/persistence gates**. The Linux GUI was executed on Debian 13 with
X11/Mesa; the delivered Linux binary needs GLIBC 2.38 or newer. See the
[release test report](../TEST-REPORT.md) and
[machine-readable evidence](../qa/results/v0.3.0/).

The original unsigned Windows release was cross-compiled and its imports were
audited. A separate Windows GNU build passed 33 release tests and 9 offline
self-checks on Windows x64. Its headless reader inspected six partitions and
read one record from `fleet.telemetry` over the Sondahub PLAINTEXT bridge.
Its executable hash and checks are in the
[earlier native Windows report](../qa/results/v0.3.0/windows-native-2026-11-22.json).

The Windows review build passed 46 release tests, nine offline self-checks,
native GUI smoke and separate-process reopen checks, and a read through the
Sondahub bridge. Its disposable Kafka 4.1.2 fixture passed 20 checks, including
topic-only SASL/PLAIN access, unchanged consumer-group state and finite reads
during an open transaction. See the
[Windows review-build results](../qa/results/v0.3.0/windows-review-fixes-2026-11-22.json).

The [Windows language check](../qa/results/v0.3.0/windows-language-2026-11-22.json)
records a later build running with the host's English display language, then
switching to Russian in App settings and reopening in Russian.

Other current limits:

- Live TLS, mTLS and SCRAM broker configurations have not been verified.
- No Schema Registry integration, Avro/Protobuf schema decoding, OAuth or
  Kerberos support is implemented.
- No credential vault/keychain persistence, updater or installer.
- No macOS/ARM release, broad accessibility/high-DPI certification, or prolonged
  throughput/memory-soak acceptance test.
- Native GUI checks use application hooks plus independent window, framebuffer
  and disk observations; they are not a full OS-level input/clipboard suite.

These limits are not an automatically authorized feature queue. There is no
pending feature task at handoff; the next user request determines the work.

## Maintaining this handoff

Keep product decisions here, implementation explanations in
[ARCHITECTURE.md](ARCHITECTURE.md), commands in
[DEVELOPMENT.md](DEVELOPMENT.md), and release evidence in versioned reports.
Update the [changelog](../CHANGELOG.md) for user-visible changes. Do not rewrite
an old PASS as proof of a new executable. A documentation-only handoff does not
require changing the application version or rebuilding its binaries.
