# Kafka Reader — 0.4.0

A native desktop prototype for inspecting one Kafka topic when you have broker
and topic access but no Kafka web UI. The interface is Rust **egui/eframe** with an
OpenGL renderer. The Kafka engine uses **rust-rdkafka / librdkafka**. There is no
browser view, Java runtime or .NET runtime.

Version 0.4.0 is available as a portable Windows x64 executable and as source
for Windows and Linux. See the [quick start](QUICKSTART.md).

## Continue development in Codex

Open the source folder containing `Cargo.toml` and [AGENTS.md](AGENTS.md) as
your project root. [CODEX_START.md](CODEX_START.md) explains how to open it and
provides a first-session prompt. The handoff references are
[project context](docs/PROJECT_CONTEXT.md),
[architecture](docs/ARCHITECTURE.md), and
[development commands](docs/DEVELOPMENT.md). They capture the accepted product
decisions and implementation constraints for future changes.

## Run the desktop app

**Windows x64:** run `kafka-reader.exe`. It is a single native executable:
librdkafka, OpenSSL, zlib and zstd are statically linked. No companion Kafka,
TLS, compression or GNU runtime DLLs are needed. The ZIP also contains the quick
start, example profiles, verification reports and dependency notices. This is
not a self-extracting wrapper and does not require Java, .NET or a browser.

The standalone EXE and the EXE inside the Windows ZIP carry the same Authenticode
signature from **Alexey Militskiy**, with a DigiCert timestamp. The signing
certificate is self-signed. Windows shows an untrusted publisher on PCs that
have not explicitly trusted it. See [Verify the Windows download](QUICKSTART.md#verify-the-windows-download).

**Linux x64:** build from source, then run `./target/release/kafka-reader-rust`.
The desktop requires an OpenGL-capable X11/XWayland session.
See [Build from source](#build-from-source).

First launch opens an empty workspace with **+ Add server**. Existing real server
profiles, navigation and preferences restore without connecting or loading
records. The built-in demo server and its tabs are removed from old workspaces
when loaded. Real servers are identified by their saved connection type, not by
their name, so a real connection named "Demo" is preserved. There is no demo
launcher or public `--demo` mode.

The [release test report](TEST-REPORT.md) identifies the tested builds and
records the checks run on each platform.

## Workspace and settings

The interface uses a compact developer-tool workspace inspired by the supplied
reference and Bruno's collection workflow: a server/topic tree, closable topic
tabs, a read toolbar, a message list and a resizable inspector. The dark theme
uses a restrained amber accent; a light theme is also available.

1. Choose **+ Add server**. This opens a separate native window.
2. **Connection** contains the server name, bootstrap addresses, protocol and
   exact topic to test/register. **Authentication** contains the SASL mechanism,
   username and password. **TLS** contains PEM certificate/key paths and a key
   password. **Advanced** accepts additional native `key=value` properties and
   imports/exports profiles.
3. **Test exact topic** checks that topic in the background. **Save server**
   commits the draft locally; saving does not start a connection. Cancel discards
   unsaved edits, including imported profiles.
4. Expand a server and use **+ Topic** to register another exact topic name. This
   saves a name in the workspace; it does not create a broker topic. Stars place
   topics in Favorites. Sidebar search filters the saved collection.
5. Open a topic, choose the start mode and limits, then press **Read**. Use
   **Messages**, **Topic info** and **Partitions** to inspect loaded data and
   actual metadata. **Inspect topic** explicitly refreshes the selected topic's
   bounds without starting a message read.

Up to **eight topic tabs** can be open. Idle tabs retain their records, filters,
sort order, selection and read controls in memory. One reader runs at a time; stop it before
switching topics/servers or closing its active tab. This also protects queued
records from being delivered into the wrong tab. Inactive tabs can be closed
without interrupting the active read. Editing a server clears its cached views
so old records are not presented under changed connection settings.

**App settings** at the bottom of the sidebar opens another native window. It
controls dark/light appearance, compact rows, font size, UTC/local time display,
default record/time/recent-window limits and the shared record/byte budget.
Settings are drafts until **Apply settings**. Lowering retention trims existing
caches immediately, starting with older inactive tabs.

The desktop follows the system display language by default. A Russian display
language selects Russian, with English as the fallback. To override it, open
**App settings → Language**, choose **English** or **Русский**, and click
**Apply settings**. The change takes effect immediately and is saved for the
next launch. Choose **System default** (**Как в системе**) to follow the
operating system again. On Windows the app uses the user's display language.
On Linux it uses the locale for messages and its language preferences.

Shortcuts: **Ctrl+Enter** reads the active topic; **Ctrl+F** focuses loaded-message
search; **Ctrl+,** opens App settings.

### Saved workspace and portable data

Server profiles, topic registrations, favorites, open tab identities and app
preferences are written atomically to `workspace.json`. Typical defaults are
`%APPDATA%\kafka-reader-rust` on Windows and
`$XDG_CONFIG_HOME/kafka-reader-rust` or `~/.config/kafka-reader-rust` on Linux.
Choose an explicit directory to keep a portable workspace with the executable:

```powershell
.\kafka-reader.exe --data-dir .\workspace-data
```

The existing `kafka-reader-rust` data directory is retained for compatibility
with earlier releases; the application and distributed executable are named
Kafka Reader. Replace the executable and keep using the same data directory to
retain your connections and preferences.

`--data-dir` does not turn on automatic reading. Usernames, passwords, tokens and
inline private-key material stay in memory and must be entered again after a
restart. Saved certificate/key **paths** are allowed. Message bodies and cached
filter/selection state are not written to the workspace. A malformed workspace
is reported and left intact; repair or move it and restart to resume saving.

## What the prototype does

- Connects to explicitly supplied bootstrap addresses and a single exact topic.
- Inspects that topic's partitions and readable beginning/exclusive-end offsets.
- Reads from the beginning, a recent offset window, an explicit offset, a
  timestamp, or the current readable end; optionally follows future arrivals.
- Provides responsive pause/resume/stop controls and dark/light themes.
- Keeps profiles, favorite topics and application preferences in a local workspace.
- Presents a virtualized message list with separate **Partition**, **Offset**
  and **Time** columns. Click any of those headers to sort numerically; click
  again to reverse direction. The initial order is newest timestamp first.
  Missing timestamps stay last. Sorting uses the full timestamp, including the
  date, even though the compact column displays the time of day.
- Keeps four inspector tabs: **Value**, **Key**, **Headers**, **Metadata**.
  Value and Key each offer **Auto**, **JSON**, **Text**, **Hex** formats. Auto
  shows formatted JSON or valid UTF-8; binary content offers the optional Hex
  format. Hex is not a separate tab or a duplicate block under every key.
  JSON inspection includes bounded syntax highlighting and line numbers.
- Leaves scrolling under your control; the auto-scroll control and behavior
  have been removed. Following a topic still receives new records.
- Preserves null versus empty payloads, binary keys/values, duplicate headers,
  header order and null header values. Formatting never changes raw record bytes.
- Filters the retained records locally and exports visible records as JSONL.
- Imports literal UTF-8 `key=value` profiles and saves profiles with secrets
  omitted. Profile and export operations use editable file paths in this prototype.
- Supports PLAINTEXT, SSL, SASL_PLAINTEXT and SASL_SSL; PLAIN, SCRAM-SHA-256 and
  SCRAM-SHA-512 are available. PEM CA and client certificates use native
  librdkafka configuration keys.

### A truly group-free reader

Manual assignment on a high-level librdkafka consumer with a nonempty `group.id`
can still discover a coordinator. That requires group authorization on some
brokers. This app therefore **removes `group.id` entirely** and uses the native
partition API, `rd_kafka_consume_start_queue` / `rd_kafka_consume_queue`, through a
small Rust RAII adapter. A `BaseConsumer` is used for metadata, offset queries and
the owning native client only. The application never subscribes to a group,
joins a group, commits offsets, produces records, creates topics or changes
broker configuration.

The app enforces `enable.auto.commit=false`, legacy
`auto.commit.enable=false`, `enable.auto.offset.store=false`,
`allow.auto.create.topics=false` and absolute starting offsets. Broker-backed
offset storage is selected with automatic storage disabled, so the legacy
partition reader does not create local offset files. Imported group IDs,
interceptor hooks, native plugin paths and Kerberos command hooks are ignored.

The broker must support Metadata v4 (Apache Kafka 0.11 or newer). Kafka Reader
refuses older Metadata versions because a topic lookup on those versions can
create a mistyped topic on brokers with automatic topic creation enabled.

The native adapter owns the topic handle and queue on one worker thread. It
copies message bytes before releasing native messages, and stops partition
consumption before destroying the topic, queue and finally the owning client.
Its short `unsafe` boundary is in `src/direct_consumer.rs`.

### Reading semantics

Each finite read captures the selected partitions' exclusive readable ends
before delivering messages. Messages appended beyond those ends are excluded.
The captured ends are per-partition bounds, not a transactionally synchronized
snapshot across the entire topic. Offset gaps are normal in compacted topics and
around transaction/control records. Completion uses delivered offsets and the
native partition EOF offset, rather than assuming consecutive messages exist.

The default `isolation.level=read_committed` excludes aborted records. In the
pinned librdkafka 2.16.0, watermark and timestamp queries honor isolation and
read-committed end queries return the **last stable offset (LSO)**. An open
transaction therefore limits the snapshot end instead of leaving the app waiting
for unreadable records. Both aborted and open transaction cases were tested.
You can explicitly set `isolation.level=read_uncommitted` in advanced properties
if that is the behavior you need.

The **recent window** is an offset-position window per partition, not an exact
count of the last N existing records. An explicit offset must lie within every
selected partition's retained bounds, including the exclusive end. A timestamp
with no matching message starts at the captured end and never silently rewinds
to the beginning. The GUI accepts epoch milliseconds or ISO-8601 timestamps; the
CLI accepts epoch milliseconds. **New messages** without following is an empty
snapshot; enable following to wait for arrivals.

Record and active-time limits can end a read before every snapshot end is
reached. Paused time and connection initialization are excluded from the read
time budget. Kafka defines offset order within each partition. Sorting by offset
across multiple partitions compares numbers for display; it does not establish
a global broker order. The GUI sorts the currently retained and filtered rows
by the selected column without changing the retention buffer or Kafka read
position. GUI exports follow that displayed sort order. The headless CLI exports
records in their received order.

### Bounded inspection and exact exports

All open tabs share a configurable retention budget, defaulting to **10,000
records and 64 MiB of raw key/value/header bytes**. App settings allow 100–50,000
records and 1–512 MiB. Older inactive tabs are evicted first, followed by the
oldest active-tab records, whenever either budget is crossed. This is a raw-data budget,
not a process-RSS limit: UI objects, text previews, native Kafka buffers and an
in-progress export require additional memory. The UI channel also has a
256-event / 16 MiB raw-data budget. A record larger than 16 MiB is skipped and the
skipped counter increases. Native fetch buffers are separately bounded, though
Kafka may return a large first batch to make progress.

Text/JSON previews are capped at 256 KiB, formatted JSON at 512 KiB, hex at 8 KiB
and the header inspector at 500 headers. Full bytes of retained, non-skipped
records remain available in exports. Selected previews are formatted off the UI
thread. Unicode text is preserved; glyphs outside the bundled font coverage may
display as missing glyphs. Exported UTF-8 and Base64 are unaffected.

The local filter searches topic, partition, offset, key preview and value
preview; it is not a broker-side query or a full-payload search. **Export JSONL**
takes a snapshot of the currently filtered retained rows, preserves their order,
and refuses an already existing filename in the GUI. The CLI streams all read
records to its export file and overwrites that explicitly named file.

Every JSONL row uses `format: "kafka-reader-jsonl-v1"` with:

```text
topic, partition, offset, timestamp, timestampType
keyBase64, keyUtf8, valueBase64, valueUtf8
headers: [{ name, valueBase64, valueUtf8 }, ...]
```

Base64 `null` means a null byte array; `""` means an empty byte array. UTF-8 fields
are null for invalid UTF-8, so Base64 is the authoritative representation. This
matches the companion Java and C# prototype export contract.

## Connection profiles

See `examples/local.properties`, `examples/sasl-ssl.properties` and
`examples/tls-client-certificate.properties`. Profiles and advanced options use
native librdkafka property names, **not Java `sasl.jaas.config`** or Java keystore
properties. Java JAAS input gets an actionable error. Values are literal UTF-8;
there are no Java backslash escapes or line continuations. Comments begin with
`#` or `!`. Use PEM file paths for TLS material. One property goes on each line.

The protocol/username/password fields in the dialog take precedence over matching
advanced options when populated. Usernames and passwords remain in session
memory; there is no OS keychain storage. Saving a profile omits usernames and
keys containing password, passwd,
secret, token, JAAS or private, plus inline TLS private-key properties. A saved
certificate/key **path** is not removed. Profiles you supply yourself may contain
credentials, and exports contain the actual topic data: keep those files in an
appropriate location.

Both certificate verification and TLS hostname verification default to enabled.
For a private CA, set `ssl.ca.location` to the appropriate PEM file rather than
disabling verification. Native SSL/SCRAM capabilities are included in the builds,
but the integration fixture exercised PLAINTEXT and SASL/PLAIN; it did not supply
a TLS/mTLS or SCRAM broker. GSSAPI/Kerberos, OAuth, Schema Registry, Avro/Protobuf
decoders, cluster administration and full-text broker search are outside this
prototype.

## CLI

Run without flags for the desktop:

```bash
./kafka-reader
./kafka-reader --self-test
./kafka-reader --version
./kafka-reader --read-config examples/local.properties --topic YOUR_TOPIC \
  --mode recent --value 100 --partitions 0,1 --max 2000 --seconds 30 \
  --export records.jsonl
./kafka-reader --read-config examples/local.properties --topic YOUR_TOPIC \
  --mode latest --follow --max 1000 --seconds 60 --export new-records.jsonl
```

`--inspect` with `--read-config` requests metadata and bounds only. Modes are
`earliest`, `recent`, `offset`, `timestamp`, `latest`. `--value` supplies the
recent window, offset or epoch milliseconds. Omit `--partitions` to select all
partitions in the supplied topic. `--max 0` and `--seconds 0` remove the respective
limit in the CLI; the GUI keeps explicit positive limits.

The CLI emits progress states on stderr, a final JSON summary on stdout, and
returns nonzero for failure. Credentials are redacted from application error
messages. The Windows desktop executable attaches to the caller's console for
CLI flags while preserving redirected pipes. In PowerShell, a GUI-subsystem
executable may need `Start-Process -Wait` when waiting for it is important.

GUI QA hooks include `--gui-smoke --screenshot-dir OUTPUT --data-dir DATA` and
`--gui-check-reopen OUTPUT --data-dir DATA` for a second-process restoration
check. With no explicit data directory, `--gui-smoke` creates a temporary
workspace. To keep its output for the reopen check, supply a directory without
an existing `workspace.json`. The smoke fixture's sample profile is excluded
from the saved workspace.
`--light`, `--compact`, `--screenshot FILE` and `--exit-after SECONDS` are also
available. A screenshot requires an actual native desktop/OpenGL display; it
does not start a reader. Use an isolated data directory for QA so test changes
do not affect a normal workspace.

## Build from source

The project pins eframe **0.37.0** and rust-rdkafka **0.39.0**. `Cargo.lock` pins
the entire dependency graph, including rdkafka-sys **4.11.0+2.16.0**. The minimum
Rust version is **1.95**; delivered binaries used **Rust 1.100.0**. Build commands
use `--locked` so dependency upgrades are explicit.

### Linux x64

Install a Rust toolchain and the standard C/C++/CMake/Perl/pkg-config build tools,
plus X11/OpenGL development dependencies. On Debian/Ubuntu, a typical package set
is:

```bash
sudo apt-get install build-essential cmake pkg-config perl \
  libx11-dev libxrandr-dev libxi-dev libxcursor-dev libxinerama-dev \
  libxkbcommon-dev libgl1-mesa-dev
./packaging/build-linux.sh
```

Linux builds librdkafka, OpenSSL, zlib and zstd from the locked crate sources. The
0.3.0 Linux binary dynamically needs glibc/libm/libgcc; X11, xkbcommon and OpenGL
are loaded at runtime by the windowing backend. Rebuild on an older supported
distribution if you need an older glibc baseline. The enabled Linux backend is
X11; Wayland sessions need XWayland.

### Windows x64

The scripts provide a GNU-target static build matching the delivered executable.
Install Rust, Python 3, CMake, Ninja, Make, Perl and pkg-config/pkgconf, plus an
MSYS2 MinGW-w64 x64 C/C++
toolchain and binutils. Put the tools on PATH, install
`rustup target add x86_64-pc-windows-gnu`, then run:

```powershell
powershell -ExecutionPolicy Bypass -File packaging/build-windows.ps1
```

The build compiles librdkafka, OpenSSL, zlib and zstd from the locked Cargo
sources. `.cargo/config.toml` selects static GNU runtimes; `build.rs` supplies the
Windows system libraries after the native static archives. Both files are part
of the source distribution and must be retained. The release has only Windows
system-library imports, including the operating system's graphics/network APIs.
No native DLL extraction or external runtime download occurs when the app starts.

To cross-build from Linux, install the Windows GNU Rust target, MinGW C/C++ and
binutils, CMake, Ninja, Make, Perl and pkg-config, then use
`packaging/build-windows-cross.sh`.
The script isolates host pkg-config lookup for the cross build. Source downloads
are needed on the first build unless the pinned Cargo sources are cached.
Cross-linking and dependency inspection do not replace running the app on
Windows. A separate MSVC build recipe is not supplied.

### Source layout and checks

| File | Responsibility |
|---|---|
| `src/app.rs` | Native workspace UI, topic caches, previews and export |
| `src/app/presentation.rs` | Numeric sorting and bounded payload display formats |
| `src/app/dialogs.rs` | Separate native server, settings, topic and export windows |
| `src/app/smoke.rs` | Instrumented desktop and restart verification |
| `src/workspace.rs` | Profiles, settings, tab state, validation and atomic persistence |
| `src/reader.rs` | Worker, controls, bounds, snapshot/follow loop and read-only policy |
| `src/direct_consumer.rs` | Small native group-free partition/queue adapter with RAII |
| `src/model.rs` | Raw records, previews, offsets, budgets, profiles, JSONL, QA records |
| `src/main.rs` | GUI/CLI entry points and built-in self-test |
| `qa/integration/` | Actual broker integration fixture and test runner |
| `packaging/` | Reproducible builds, dependency notices and ZIP assembly |

```bash
cargo fmt --check
cargo test --locked --release
cargo build --locked --release
target/release/kafka-reader-rust --self-test
```

App code is MIT licensed. Every distribution ZIP includes the dependency license
texts and native redistribution notices under `THIRD-PARTY/`. The checkout
contains the inventory and instructions for collecting the original texts.
See [Third-party notices](THIRD-PARTY-NOTICES.md).

## Primary project references

- rust-rdkafka: <https://docs.rs/rdkafka/0.39.0/rdkafka/>
- eframe native framework: <https://docs.rs/eframe/0.37.0/eframe/>
- librdkafka C API: <https://github.com/confluentinc/librdkafka/blob/v2.16.0/src/rdkafka.h>
- librdkafka configuration: <https://github.com/confluentinc/librdkafka/blob/v2.16.0/CONFIGURATION.md>
- librdkafka offset/isolation change history: <https://github.com/confluentinc/librdkafka/blob/v2.16.0/CHANGELOG.md>
- OpenSSL source/build documentation: <https://github.com/openssl/openssl>
