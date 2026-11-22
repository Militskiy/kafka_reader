# Kafka Reader 0.3.0 — quick start

## Launch

**Windows x64:** double-click `kafka-reader.exe`. This is one native
executable with Kafka, TLS and compression code linked into it. No companion
library DLLs, Java, .NET, browser or installer are required. The ZIP contains
optional documentation, example profiles and dependency notices alongside it.
The original Windows release is unsigned and was cross-compiled. A later
Windows build passed its headless checks on Windows x64. Native desktop
interaction there remains unverified. See the
[Windows host results](qa/results/v0.3.0/windows-native-2026-11-22.json).

**Linux x64:** extract the Linux ZIP and run `./kafka-reader`. If your
extractor dropped executable permissions, use `chmod +x kafka-reader`.
An X11/XWayland desktop and working OpenGL driver are required. The supplied
release was executed on Debian 13 with Mesa.

First launch starts with an empty workspace. Use **+ Add server** to add your
connection. Existing real connections and preferences restore automatically;
the built-in demo server is removed when an older workspace is loaded.

## Add your Kafka server

1. Click **+ Add server** in the left sidebar.
2. In **Connection**, enter a profile name, bootstrap addresses such as
   `broker-1:9092,broker-2:9092`, a security protocol and an exact topic name.
3. In **Authentication**, choose PLAIN or SCRAM and enter your username/password
   if required. In **TLS**, provide PEM CA/client-certificate/key paths where
   required. **Advanced** accepts additional librdkafka properties or imports a
   literal `.properties` profile.
4. Click **Test exact topic** to check access in the background, then **Save
   server**. Save commits the local draft without starting a read. Cancel
   discards it.
5. Expand the server, select the saved topic, or use **+ Topic** for another exact
   name. Star topics to add Favorites. Adding a topic here does not create it on
   the broker. No cluster-wide topic listing is requested.

All brokers advertised by Kafka must be reachable from your desktop, not just
the initial bootstrap address. The app uses direct partition reads without a
consumer group or offset commits. It was tested with topic READ/DESCRIBE access
and no consumer-group permission. Kafka web UI access is not required.

## Read and inspect

Choose **Recent window** and `100` to begin up to 100 offset positions before
each partition's readable end. Leave **Read partitions** empty for all partitions
or enter a list such as `0,1`. Set **Max** and the time budget, then click **Read**.
To watch future arrivals, choose **New messages**, enable **Follow**, and read.

Click **Time**, **Partition**, or **Offset** to sort the loaded messages; click
again to reverse direction. Each field has its own column. The initial order is
newest timestamp first. Missing timestamps stay last. The selected order stays
with its topic tab during the session. Auto-scroll has been removed; **Follow**
still receives new records without forcing the view to the bottom.

Select a row to inspect **Value**, **Key**, **Headers**, or **Metadata**. Value
and Key each have a format selector: **Auto**, **JSON**, **Text**, **Hex**. Auto
shows readable text or formatted JSON. Choose Hex only when you want to inspect
binary bytes. Null and empty keys/values remain distinct. Drag the divider to
resize the inspector. Search and key/partition filters
operate on retained messages. **Topic info**, **Partitions** and **Inspect topic**
show exact-topic metadata and readable bounds. Pause/Resume/Stop control the
active reader. Stop it before switching topics or servers; idle tabs keep their
loaded records and filters in memory. Up to eight tabs can be open.

**Export** writes visible retained rows in the selected sort order as JSONL,
preserving complete raw bytes
in Base64, valid UTF-8 projections, nulls and ordered duplicate headers. Choose a
new output filename; the GUI does not overwrite existing exports.

## App preferences and portable workspace

**App settings** at the bottom left opens a separate window for theme, compact
rows, font size, UTC/local timestamps, reading defaults and retained-message
limits. **Apply settings** commits the changes. All tabs share the retention
budget, initially 10,000 records / 64 MiB of raw payload. Lowering it trims older
inactive tabs first.

Profiles, topic names, Favorites, open tab identities and app settings restore on
startup. Startup does not connect or read. Usernames/passwords/tokens/private-key
content stay in memory, so enter them again after restarting. Certificate paths
can be saved. Message bodies are not saved in the workspace file.

To store workspace data in an explicit portable directory:

```powershell
.\kafka-reader.exe --data-dir .\workspace-data
```

Without this flag, the app uses the platform's normal app-data/config directory.
A single executable does not mean preferences are written inside the executable.
When upgrading, replace the EXE and keep the same data directory. The existing
`kafka-reader-rust` settings directory is still used for compatibility with your
saved profiles; the app is displayed as **Kafka Reader**.

Shortcuts: **Ctrl+Enter** Read, **Ctrl+F** search loaded messages, **Ctrl+,** App
settings. See `README.md` for source builds, CLI usage and precise read semantics.
`TEST-REPORT.md` records executed tests and platform/authentication limitations.
