#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cargo build --locked --release
cargo test --locked --release
target/release/kafka-reader-rust --self-test
python3 packaging/package.py --linux-exe target/release/kafka-reader-rust
