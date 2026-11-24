# Development, verification, and packaging

Use these commands from the project root unless a section changes directory.
They describe the **0.7.0** source layout. Check the current files when updating
versions or build targets. [TEST-REPORT.md](../TEST-REPORT.md) is the record of
what actually ran for the delivered release.

## Toolchains and outputs

| Item | Current requirement or convention |
|---|---|
| Rust | Edition 2024; minimum 1.95 in Cargo.toml; the delivered release used 1.100.0 |
| Dependency graph | Cargo.lock is included; use `--locked` for builds and tests |
| Native compilation | C/C++ compiler, Make, CMake, Perl, pkg-config/pkgconf; Ninja for the Windows build scripts |
| Packaging | Python 3 |
| Linux desktop | X11/XWayland and OpenGL; X11/xkbcommon/graphics development packages for builds |
| Windows release | Rust target `x86_64-pc-windows-gnu`, MinGW-w64 x64 C/C++ compiler and binutils |
| Local Kafka fixture | Optional JDK 21+, Maven 3.9+, Python 3 |
| GUI observer | Linux, Xvfb, xauth, xwininfo, and Pillow with X11/XCB support |

No downloaded SDK, dependency cache, or original developer-machine path is
required in the repository. First builds download locked Rust dependencies and
compile librdkafka, OpenSSL, zlib and zstd; they are substantially slower than
incremental app builds. Do not interpret a native-library build as a UI hang.

The internal executable is `kafka-reader-rust` or `kafka-reader-rust.exe`.
Packaging renames it to `kafka-reader` or `kafka-reader.exe`. Commands below
assume the default `target/` directory. The Windows scripts support
`CARGO_TARGET_DIR`; the current Linux wrapper uses literal `target/` paths for
its self-test and packaging, so leave that override unset when using it.

## Linux build and local run

A typical Debian/Ubuntu setup includes `build-essential`, `cmake`,
`pkg-config`, `perl`, `libx11-dev`, `libxrandr-dev`, `libxi-dev`,
`libxcursor-dev`, `libxinerama-dev`, `libxkbcommon-dev`, and
`libgl1-mesa-dev`. Python 3 is needed for packaging. Install prerequisites using
the environment's normal package-management process.

```bash
cargo fmt --check
cargo test --locked --release
cargo build --locked --release
target/release/kafka-reader-rust --self-test
target/release/kafka-reader-rust --data-dir /tmp/kafka-reader-dev-workspace
```

The last command opens the native desktop and uses an isolated local workspace.
It does not connect until a user explicitly tests or reads a topic. A display
and OpenGL are needed for the desktop; the self-test does not need a display or
Kafka broker. Release-mode tests reuse the release native-library cache.

For the existing combined build/test/package workflow:

```bash
./packaging/build-linux.sh
```

That wrapper also creates source and Linux release ZIPs. Use the individual
Cargo commands for development when you do not need new archives.

## Windows single-EXE build

The release target is GNU. Do not assume a Windows host's default MSVC target
matches the supplied build recipe. Install the Rust GNU target and make
MinGW-w64 x64 GCC/G++, binutils, CMake, Ninja, Make, Perl, Python and pkgconf
available on PATH. On Windows the script supports an MSYS2 MinGW-w64 toolchain.

For the default MSYS2 installation, add the tools to the current PowerShell
session. Adjust `C:\msys64` if MSYS2 is installed elsewhere.

```powershell
$env:Path = "$env:USERPROFILE\.cargo\bin;C:\msys64\mingw64\bin;C:\msys64\usr\bin;$env:Path"
```

Run these commands from the directory containing `Cargo.toml`.

```powershell
rustup target add x86_64-pc-windows-gnu
cargo fmt --check
.\packaging\build-windows.ps1
cargo test --locked --release --target x86_64-pc-windows-gnu

$check = Start-Process -Wait -PassThru -NoNewWindow `
  -FilePath ".\target\x86_64-pc-windows-gnu\release\kafka-reader-rust.exe" `
  -ArgumentList '--self-test'
if ($check.ExitCode -ne 0) { throw 'Kafka Reader self-test failed' }

.\dist\v0.7.0\kafka-reader.exe --data-dir "$env:TEMP\kafka-reader-dev-workspace"
```

The build script sets the required native-library environment before invoking
Cargo and packages the result. The original 0.3.0 release used the Linux cross-build
route below. The [earlier native Windows report](../qa/results/v0.3.0/windows-native-2026-11-22.json)
and the [review-build results](../qa/results/v0.3.0/windows-review-fixes-2026-11-22.json)
identify the executable used for each Windows verification run.

For a Linux cross-build, install the MinGW-w64 compiler/binutils and the other
native build tools, including Ninja, then run:

```bash
rustup target add x86_64-pc-windows-gnu
./packaging/build-windows-cross.sh
```

The internal output is
`target/x86_64-pc-windows-gnu/release/kafka-reader-rust.exe`; the portable output
is `dist/v0.7.0/kafka-reader.exe`.

Keep [.cargo/config.toml](../.cargo/config.toml), [build.rs](../build.rs), and
the static features in [Cargo.toml](../Cargo.toml). They supply static GNU
runtime flags and the Windows system-library link ordering needed after the
native Kafka/OpenSSL archives. Cross-build scripts isolate host pkg-config
lookup. Replacing this setup with a dynamic librdkafka DLL changes the agreed
distribution model.

For import inspection, a MinGW binutils environment can use:

```bash
x86_64-w64-mingw32-objdump -p dist/v0.7.0/kafka-reader.exe
```

Review the imported DLL names for unexpected third-party runtimes. An OS-only
import list supports the static-package claim; it does not execute Windows
graphics, input, clipboard, networking or authentication paths.

## Choose checks for the change

| Change | Relevant verification |
|---|---|
| Markdown only | Check local links, commands against scripts, archive inclusion and unchanged application source; no runtime rebuild required |
| UI layout or interaction | Format/unit checks, release build, native GUI observer and visual inspection at normal/compact sizes |
| Sorting/formatting/export | Numeric, missing-time, null/empty/binary and byte-preservation tests; inspect affected GUI and export behavior |
| Profiles, settings or migration | Persistence validation and malformed-file checks; fresh-start/reopen checks with isolated data |
| Kafka loop, bounds, client properties or native FFI | Unit/self checks plus the live local Kafka fixture, especially topic-only ACLs, group state and transaction boundaries |
| Dependencies or packaging | Relevant code checks, rebuild intended targets, inspect native dependencies, verify archive contents and licenses |

Keep tests proportional to a concrete risk. Do not add assertions that merely
repeat the implementation, and do not rerun a full broker suite for a wording
change. If the current environment cannot perform a required check, state that
limit and preserve the unverified status instead of manufacturing PASS evidence.

## Native GUI verification

On a configured Linux host, run the observer with a new output directory:

```bash
python3 qa/integration/run-gui-v03.py \
  --exe ./target/release/kafka-reader-rust \
  --output /tmp/kafka-reader-gui-check-new-run \
  --xvfb /usr/bin/Xvfb
```

Choose a different directory for another run; the runner intentionally refuses
to reuse an existing output directory. `--xvfb` is optional if Xvfb is on PATH.
Install the observer dependencies listed in
[qa/integration/README.md](../qa/integration/README.md).

The observer launches a normal empty startup, then explicit `--gui-smoke`, then
`--gui-check-reopen` in a fresh process. It observes real native windows and
saved state and records the executable hash. Its synthetic sample is test-only
and does not persist as a normal server. Inspect the captured images as well
as checking the reports. Run observers sequentially; they use display 99 and
TCP port 6099 with a temporary Xauthority cookie.

Keep raw observer workspaces, temporary credentials and Xauthority files in
the test output area. Curate final report JSON and selected screenshots into
the versioned evidence folders. A screenshot of a populated QA sample should
be identified as synthetic and must not be mistaken for normal startup.

## Real Kafka verification

The Rust fixture accepts `--oauth` to test a local HTTPS refresh grant and
Kafka OAUTHBEARER over TLS. Put OpenSSL on `PATH` for this check. The
[fixture guide](../qa/integration/README.md) lists prerequisites and commands.

The optional local fixture starts its own disposable Kafka broker and tests the
actual headless executable. From the project root:

```bash
cd qa/integration
mvn dependency:build-classpath -Dmdep.outputFile=broker-classpath.txt
python3 run-integration.py --app rust --exe ../../target/release/kafka-reader-rust
```

Each run allocates three loopback ports and a fresh directory under
`qa/integration/work`. Before seeding topics, ACLs, records, or group offsets,
the fixture checks that both client listeners report the cluster ID it formatted
for this run. Its broker storage and credentials are temporary. The
[fixture guide](../qa/integration/README.md) also describes the separate
read-only Sondahub bridge check.

The checks cover snapshots, offsets, timestamp seeks, follow, exact limits,
raw records/headers, authentication failures, restricted topic access,
unchanged consumer groups/offsets, aborted transactions and open-transaction
last stable offsets. They do not establish live TLS/mTLS/SCRAM coverage.

## Package the source or release

All ZIPs include the Cargo dependency notices for both supported targets.
Put Cargo on `PATH` when running the Python packager. It resolves both graphs
with `cargo metadata --locked` and writes the files listed in
[`dependency-inventory.json`](../THIRD-PARTY/dependency-inventory.json) directly
from the crate sources into each ZIP. The first run can download crates.
To prepare an offline packaging host, populate its cache while online:

```bash
cargo fetch --locked --target x86_64-unknown-linux-gnu --target x86_64-pc-windows-gnu
```

Set `CARGO_NET_OFFLINE=true` to package from that cache. Packaging needs
the sources for both targets even when building a source-only ZIP. See
[Third-party notices](../THIRD-PARTY-NOTICES.md) when changing dependencies.

For source-only packaging, including the Codex handoff documents:

```bash
python3 packaging/package.py --out dist/codex-handoff
```

For an existing pair of binaries built in the default target directories:

```bash
python3 packaging/package.py \
  --linux-exe target/release/kafka-reader-rust \
  --windows-exe target/x86_64-pc-windows-gnu/release/kafka-reader-rust.exe \
  --out dist/v0.7.0
```

The packager lists the shipped integration fixture files in `FIXTURE_FILES`.
Add new fixture sources there when needed. The checksum manifest covers all
recognized release artifacts present in the selected output directory, including
platform builds produced by earlier invocations. Run packaging commands
sequentially for a given output directory. Verify ZIP CRCs and compare packaged
binaries with the hashes in the corresponding test reports.

## Release versioning

Write the GitHub release description in Russian. Include only user-facing
features and behavior changes. Keep checks, hashes and signature details in the
release evidence.

Assign a new `MAJOR.MINOR.PATCH` version to each application release. Use a
patch bump for fixes and a minor bump for new features. After `0.7.0`, the
next fix-only release is `0.7.1` and the next feature release is `0.8.0`.
For incompatible changes during `0.x`, increment the
minor version and explain the migration in the changelog. From `1.0.0`
onward, use a major bump for incompatible changes.

1. Set the package version in [Cargo.toml](../Cargo.toml) and update its entry
   in [Cargo.lock](../Cargo.lock). Move the entries being shipped from
   **Unreleased** into a dated section in the [changelog](../CHANGELOG.md).
2. Build and test the release. Record new evidence under `qa/results/vX.Y.Z/`
   and include it in the archive allowlist. For Windows, sign a copy of the
   final executable before packaging and pass that copy to
   `packaging/package.py --windows-exe`. Record the signer, timestamp and
   Windows trust result in the release evidence. Verify that the executable
   inside the Windows ZIP has the same SHA-256 as the standalone download.
   Check that `--version`, the `dist/vX.Y.Z/` directory and the test report
   agree, and verify the asset hashes against `SHA256SUMS.txt`.
3. Commit the release as `Release vX.Y.Z` and tag that commit with an annotated
   `vX.Y.Z` tag. When publishing to GitHub, push the commit and tag, then
   create the GitHub Release from that tag. Attach the verified executable,
   available platform ZIPs, source ZIP and checksum file. Confirm the published
   tag points to the release commit and the uploaded assets match the manifest.

Keep a published tag and its artifacts tied to that build. Ship subsequent
application changes under the next version. Documentation-only work can keep
the current application version.

Do not overwrite old evidence with new claims. Retain the exact binary hashes
and distinguish compilation, unit checks, GUI execution, broker execution and
platform import inspection. The release baseline and known gaps are in
[PROJECT_CONTEXT.md](PROJECT_CONTEXT.md).
