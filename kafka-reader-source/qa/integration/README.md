# Shared Kafka integration fixture

This fixture verifies the C# and Rust prototypes against the same real Apache Kafka 4.1.2 broker. It uses a disposable single-node KRaft broker with an administrative localhost listener and a SASL/PLAIN localhost listener for a restricted topic-only reader.

The mutation operations in `FixtureHost.java` create test topics, ACLs, synthetic messages and fixture group offsets. They are test setup only and are not compiled into either desktop application.

## Requirements

- Java/JDK 21 or later, Maven 3.9 or later and Python 3.
- The prototype executable built for the machine on which you run these checks.
- Three available loopback ports. The runner selects them for each run.
- Network access for the initial Maven dependency download.

From this directory:

```bash
mvn dependency:build-classpath -Dmdep.outputFile=broker-classpath.txt
python3 run-integration.py --app csharp --exe /absolute/path/to/KafkaReader
python3 run-integration.py --app rust --exe /absolute/path/to/kafka-reader-rust
```

On Windows, set `JAVA_HOME` to a JDK 21+ installation. From this directory,
run `mvn.cmd dependency:build-classpath "-Dmdep.outputFile=broker-classpath.txt"`,
then `python run-integration.py --app rust --exe ..\..\target\x86_64-pc-windows-gnu\release\kafka-reader-rust.exe`.

Add `--oauth` to exercise refresh-token and password grants. This requires
`openssl` on `PATH`. The fixture creates a temporary CA, a loopback HTTPS
token endpoint and a SASL_SSL Kafka listener. For each grant it checks TLS
verification, exact-topic inspection, renewal during a read and record
delivery after renewal. The issuer rotates refresh tokens after each use and
accepts the same synthetic login and password for each password grant. A
rejected password must produce HTTP 401 and keep both fields out of reader
output even when the issuer echoes them in the error body.

Each invocation creates a new directory under `work/run-*` and prints its
path. It formats temporary broker storage with a random cluster ID and
generates fresh credentials for the administrative and restricted accounts.
The helper checks the cluster ID through both client listeners before it seeds
data. The runner stops its child processes and removes its client profiles
and temporary broker storage when it exits. Logs, JSONL exports, and the per-language
`integration-report.json` stay in that run's directory.

The generated credentials belong to that disposable broker. The Rust reader
uses native librdkafka properties such as `sasl.username` and `sasl.password`.

## Read-only check through the Sondahub bridge

From the project root, supply the built executable, the path to your copy of
`sondahub-bridge.mjs`, and an exact topic containing at least one readable record.

```powershell
python qa/integration/run-bridge-check.py --exe .\target\x86_64-pc-windows-gnu\release\kafka-reader-rust.exe --bridge-script "$env:USERPROFILE\sondahub-bridge.mjs" --topic fleet.telemetry
```

The check uses `npx.cmd --yes --package=node@22` to locate Node 22, starts the
bridge, and waits for that process to confirm it has bound port 9192. It stops
its bridge when the check succeeds, fails, or is interrupted with Ctrl+C.
If you already run the bridge yourself, omit `--bridge-script` and stop it in
its own window with Ctrl+C after the check.

The check uses PLAINTEXT on `127.0.0.1:9192` by default. For a bridge that
needs additional client settings, pass `--config PATH` with a librdkafka profile
whose bootstrap address is `127.0.0.1:9192` or `localhost:9192`. The check
validates partition bounds and uses a finite read capped at one record. Only
counts and status are reported.

## Coverage

Twenty checks exercise exact-topic metadata, finite snapshots, recent windows, partition selection, explicit offsets, timestamps, exact record caps, late arrivals, following, null/empty/binary data, duplicate headers, Unicode export, restricted SASL access, denied group commits, authentication/authorization failures, unchanged existing group offsets, no newly created groups, aborted transactions, offset gaps and an open transaction's last stable offset.

The reader must emit `READING` on stderr after capturing end offsets so the fixture can coordinate a later arrival. Results go to stdout as a JSON summary; records go to the explicitly supplied export file.

This fixture does not measure throughput or prove Windows execution when it is run on Linux. The separate GUI checks exercise the actual native desktop windows under a real display or Xvfb. See the prototype's test report for results that were actually executed.

## Independent native GUI observer (v0.2.0)

`run-gui-v02.py` runs the application's synthetic GUI checks and then launches a separate process against the same isolated data directory to verify restart restoration. It enumerates real X11 top-level windows, captures their framebuffer, checks that server configuration and App Settings use separate native windows, and scans saved files for the known fake-secret sentinels. It does not need a Kafka broker.

Dependencies on the Linux test host:

- Python 3 and Pillow with X11/XCB screenshot support.
- Xvfb, `xauth`, and `xwininfo` available on `PATH`, or an explicit Xvfb path.
- The application's normal Linux desktop dependencies; the Rust build also needs an OpenGL-capable Mesa/X11 runtime for software rendering.

On Debian/Ubuntu, the observer-specific packages are `xvfb`, `xauth`, `x11-utils`, and `python3-pil`. Pillow can alternatively be installed in a Python virtual environment. No SDK or toolchain environment script is required to run an already-built application.

From the project root, supply a newly named output directory that does not already exist:

```bash
python3 qa/integration/run-gui-v02.py \
  --kind rust \
  --exe ./target/release/kafka-reader-rust \
  --output /tmp/kafka-reader-rust-gui-v02-new-run \
  --xvfb /usr/bin/Xvfb
```

`--exe` also accepts an absolute path. `--xvfb` is optional when Xvfb is discoverable on `PATH`; `KAFKA_READER_XVFB` is another optional override. Omit `--output` to create a unique timestamped directory under the observer's `work-v0.2.0` folder. `--timeout` changes the default 180-second limit per application process.

The observer launches a temporary TCP X11 display on `127.0.0.1:99`, protected by a random xauth cookie, and stops it when finished. Run observers sequentially on a normal host and keep display 99 / TCP port 6099 free. The app receives `--data-dir` pointing inside the fresh output directory, so the checks do not use the user's saved workspace.

Results include `observer-report.json`, separate smoke/reopen logs, actual native screenshots, and the sanitized persisted workspace. A successful exit means the automated assertions passed; screenshots still need visual review for layout and readability. These Linux checks and the separate PE import reports do not claim Windows runtime execution.


## Current native GUI observer

`run-gui-v03.py` additionally starts a clean normal application and captures its
empty server list. The synthetic server exists only during explicit GUI QA; the
saved workspace and fresh-process reopen must contain only the manually-created
real server profile. The in-app checks exercise numeric Time, Partition and
Offset sorting in both directions, retained-record selection, per-tab sort
state, and explicit Key Hex formatting without a redundant Hex tab.

```bash
python3 qa/integration/run-gui-v03.py \
  --exe ./target/release/kafka-reader-rust \
  --output /tmp/kafka-reader-gui-v03-new-run \
  --xvfb /usr/bin/Xvfb
```

The dependencies and isolated X11 setup are the same as the v0.2 observer above.
No production server is contacted. The QA data contains a localhost profile with
synthetic credentials; it is saved without those credentials and is never read.
The observer records the version from the tested executable's `--version`
output. By default it creates a new run under `work-vX.Y.Z/` for that version.
Keep release evidence in the matching `qa/results/vX.Y.Z/` directory.
