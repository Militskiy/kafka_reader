# Changelog

## 0.5.0 — 23 November 2026

- Added system clipboard support for keyboard shortcuts and inspector copy
  actions. Paste buttons in the server editor insert text at the cursor or
  replace the selected text, including in masked credential fields.
- Added optional SASL/OAUTHBEARER authentication with an HTTPS refresh-token
  exchange. Kafka Reader renews the access token and keeps rotated refresh
  tokens in memory for the session. PLAIN and SCRAM continue to accept a
  username and password.

## 0.4.0 — 22 November 2026

- Signed the Windows executable with Alexey Militskiy's self-signed Authenticode
  certificate and a DigiCert timestamp. The Windows ZIP includes the same signed EXE.
- Added a Russian desktop interface. App settings can follow the system display
  language or save an explicit English or Russian preference.
- Isolated GUI smoke workspaces and broker integration fixtures from existing user data.
- Required broker Metadata v4 to protect exact-topic lookups from automatic topic creation.
- Preserved terminal read outcomes and skipped counts under backpressure, and
  surfaced individual partition errors during timestamp seeks.
- Bound exports to the originating view and made GUI export file creation exclusive.
- Protected newer workspace saves and preserved per-tab read controls when changing appearance.
- Rejected a new server draft before saving if its first tab exceeds the tab limit.
- Applied the default SASL/PLAIN mechanism to the native client.
- Corrected checkout instructions, source archive contents, and release checksum coverage.
- Moved collection of Cargo license texts into packaging. Each distribution ZIP
  includes the original notices from the locked crate sources.

## 0.3.0 — 22 November 2026

- Renamed the displayed application to **Kafka Reader** and the portable
  executable to `kafka-reader.exe`.
- Removed the built-in demo server from normal startup, the public `--demo`
  mode, and demo launch scripts. Older workspaces drop synthetic profiles and
  their tabs while retaining real servers, favorites and app settings.
- Replaced auto-scroll with numeric sorting through separate **Time**,
  **Partition** and **Offset** headers. A second click reverses direction;
  newest timestamps are first by default and missing timestamps stay last.
- Retained per-topic sort state during the session. Sorting preserves the
  selected message and the original retention order; GUI exports follow the
  displayed ordering.
- Simplified the inspector to **Value**, **Key**, **Headers**, **Metadata**.
  Value and Key offer Auto, JSON, Text and optional Hex formats. Removed the
  standalone Hex/Text tabs and the automatic duplicate key hex block.
- Added focused migration, numeric sorting, selection and binary/null preview
  checks, plus native GUI verification and clean-start/reopen observations.
- Kept the same saved-data directory for seamless upgrades and the same static
  single-EXE Windows build. Windows runtime execution remains unverified.

## 0.2.0 — 22 November 2026

- Reorganized the desktop into a compact server/topic workspace with a dark
  amber theme, Favorites, topic search and up to eight closable topic tabs.
- Added independent native Add/Edit server windows with Connection,
  Authentication, TLS and Advanced tabs, draft validation and exact-topic tests.
- Moved application preferences into a separate native App settings window.
- Added atomic local workspace persistence, explicit `--data-dir`, session-only
  credentials, restart restoration without network requests, and preservation of
  malformed saved files.
- Preserved loaded records, filters and read controls when switching idle tabs,
  with one global retained-record/raw-byte budget and inactive-cache eviction.
- Added real Topic info/Partitions views, bounded JSON highlighting and line
  numbers, and read/search/settings keyboard shortcuts.
- Guarded active and queued reads against context switches; routed late preview
  results to their originating tab and invalidated evicted selections.
- Built the Windows x64 application as a single statically linked native EXE
  with no companion Kafka/TLS/compression/runtime DLLs or extraction wrapper.
- Expanded unit, native GUI, separate-process restoration and independent native
  window/secret-storage verification. Retained the live Kafka broker suite.
- Windows runtime execution remains unverified. See TEST-REPORT.md for evidence.

## 0.1.0 — initial prototype

- Native egui desktop, offline demo, direct group-free Kafka reader, headless
  CLI, raw message inspection, JSONL exports and Linux/Windows distributions.
