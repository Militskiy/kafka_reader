# Kafka Reader 0.3.0 — verification report

Verified on **22 November 2026 (Europe/Moscow)**. This release updates the
portable native application previously named Kafka Reader Rust. It changes the
workspace startup, app branding, message ordering and inspector layout.

This report covers the original release. For the later Windows executables,
see the [headless Windows run](qa/results/v0.3.0/windows-native-2026-11-22.json)
and the [Windows run after the review fixes](qa/results/v0.3.0/windows-review-fixes-2026-11-22.json).

## Executed checks

| Check | Actual result |
|---|---|
| `cargo fmt --check` | **Passed** |
| `cargo test --locked --release` | **33 passed**: 22 library tests and 11 UI/presentation tests |
| Release `--self-test` | **9 assertions passed** |
| Real native desktop smoke | **47 assertions passed** |
| Separate-process reopen | **15 assertions passed** |
| Independent native-window and persistence observer | **12 gates passed** |
| Native screenshot review | **Passed** at 1400 × 900 and 1050 × 720 |
| Windows x64 static release build | **Succeeded** |
| Windows PE imports | **One executable; zero non-system DLL dependencies** |
| Windows runtime execution | **Not performed** |
| New live-broker integration run | **Not performed for this UI-focused release** |

The unit and self-test logs, native GUI reports, executable hashes, dependency
reports and source-change scope are in `qa/results/v0.3.0/`. Screenshots are in
`docs/screenshots/v0.3.0/`. Raw temporary display credentials and test workspaces
are excluded from the distributed archives.

## Requested behavior

### Clean startup and upgrade

A new workspace has no servers or topic tabs. Both the sidebar and central
empty state offer **Add server**. The displayed app name is **Kafka Reader**,
and the header version is taken from the package version. The distributed
Windows executable is `kafka-reader.exe`.

When an older workspace is loaded, servers marked as synthetic/offline and
their tabs are removed from the in-memory workspace. The cleaned workspace is
written on the next normal save. Real profiles, topics, favorites and app
preferences are preserved. Migration uses the explicit offline marker, not a
name such as Demo or Offline workspace. Tests cover both sample-only and mixed
workspaces, preservation of a real connection with a sample-like name, and
valid persistence of an empty workspace.

The previous app-data directory remains in use to preserve existing settings.
The public `--demo` option and demo launch scripts are removed. Synthetic
records exist only for explicit automated `--gui-smoke` verification, and the
synthetic profile is excluded when that harness saves its workspace.

### Message ordering

**Partition**, **Offset**, and **Time** have separate, aligned columns. Clicking
a header selects its numeric order; clicking it again reverses direction.
The default is descending time. A painted triangle identifies the active
order without depending on a font-specific arrow character.

Time comparisons use full epoch timestamps, including the date. Missing times
display a dash and stay last in either direction. Unit tests cover day
boundaries, numeric offsets such as 2 and 10, the largest signed 64-bit offset,
repeatable ties and numeric partition IDs. Partition sorting keeps offsets
ascending inside each partition. Offset sorting across partitions is a display
comparison; Kafka offsets are local to their partition.

Sorting is applied to filtered retained-record references. It preserves the
underlying arrival/retention order and the selected record's identity. Each
cached topic retains its sort during the session. GUI JSONL exports follow the
visible sort order; the headless reader retains its existing received order.
The auto-scroll control and stick-to-bottom behavior are removed. Follow still
receives new records without explicitly moving the scrollbar.

### Inspector

The inspector now contains **Value**, **Key**, **Headers**, and **Metadata**.
Value and Key each offer **Auto**, **JSON**, **Text**, and optional **Hex**.
There is no standalone Hex or Text tab and no automatic second key hex block.

Auto shows formatted JSON or readable UTF-8. Binary data displays a short notice
pointing to the optional Hex format. Hex remains useful for binary values,
non-text keys and invalid text encodings. Null keys, tombstone values and empty
byte arrays remain distinct. Existing preview bounds and byte-preserving JSONL
exports are retained. Copy view copies the displayed format; Copy record copies
the complete record representation.

The desktop checks exercise sorting in both directions, selection stability,
per-tab sort caching, key Auto and explicit Hex, automatic JSON display,
server/settings windows, pause/resume/stop, filters and saved preferences.
A fresh process restores one real saved profile and its topics/favorites without
restoring the synthetic profile, message bodies, session credentials or a reader.
The external observer also enumerates real X11 window IDs, captures framebuffers
and scans saved files for known test-secret sentinels.

## Exact builds

Build toolchain: Rust **1.100.0**, egui/eframe **0.37.0**, rust-rdkafka **0.39.0**,
librdkafka **2.16.0**. Locked dependencies were retained.

| Artifact | Bytes | SHA-256 |
|---|---:|---|
| Linux executable | 17,599,544 | `dc729010c4744f83c04b1c4ee245f6fd858287ab4e0b3a6f54b57dfa1334af1c` |
| Windows single EXE | 17,695,744 | `7d6afd62e700a094a9f9651704945e09a67a09327b9b2b7ad0a3055a56d22e5d` |

The Windows release statically links Kafka, OpenSSL, zlib, zstd and the required
GNU runtime code. Its PE imports contain only Windows operating-system libraries.
It does not unpack companion libraries at startup and does not require a Java,
.NET or browser runtime. The optional ZIP adds documentation and examples.

The Linux release was executed on Debian 13 with glibc 2.41 and X11/Mesa software
rendering. Its highest required GLIBC symbol version is **2.38**. A working
OpenGL-capable X11 or XWayland desktop is required.

## Scope and limitations

The Kafka worker, native group-free consumer adapter and connection-dialog
implementation are byte-for-byte unchanged from the delivered 0.2.0 source
archive. `source-change-scope.json` records that comparison. The earlier 0.2.0
release passed 20 real Kafka checks, including topic-only SASL/PLAIN access,
finite reads, raw bytes, transactions and unchanged consumer-group offsets.
Those are historical results and are not presented as a new broker run for
0.3.0. No public Sondahub or production broker was contacted for this update.

Windows binaries were cross-compiled and statically dependency-inspected; they
were not executed on Windows or under Wine and remain unsigned. Native GUI
checks ran on Linux with application test hooks plus independent window,
framebuffer and persistence observations. This is not a full OS-level
mouse/keyboard/clipboard, screen-reader, high-DPI or performance acceptance suite.
TLS/mTLS/SCRAM remain unverified against corresponding live brokers. OAuth,
Kerberos, Schema Registry and Avro/Protobuf decoding remain outside the prototype.

Populated screenshots use an explicit, temporary synthetic QA fixture. The
ordinary empty-start and reopen captures demonstrate that fixture's absence
from normal startup and saved server lists.

## Reproduce the focused checks

```bash
cargo fmt --check
cargo test --locked --release
cargo build --locked --release
target/release/kafka-reader-rust --self-test
python3 qa/integration/run-gui-v03.py \
  --exe ./target/release/kafka-reader-rust \
  --output /tmp/kafka-reader-gui-v03-new-run \
  --xvfb /usr/bin/Xvfb
```

The Cargo target keeps its internal crate name; packaged executables use the
short Kafka Reader name. See `README.md` for platform build dependencies and
`qa/integration/README.md` for the optional live-broker fixture.
