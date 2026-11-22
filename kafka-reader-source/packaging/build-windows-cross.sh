#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Rust target, MinGW-w64 GCC/binutils, CMake, Ninja, Perl and make are required.
# Native transport/compression/TLS archives come from the pinned Cargo sources.
export CMAKE_GENERATOR=Ninja
export OPENSSL_STATIC=1
export RUSTFLAGS='-C target-feature=+crt-static -C link-arg=-static-libgcc -C link-arg=-static-libstdc++'
export PKG_CONFIG_PATH=
export PKG_CONFIG_PATH_x86_64_pc_windows_gnu=
mkdir -p target/empty-pkgconfig
export PKG_CONFIG_LIBDIR="$PWD/target/empty-pkgconfig"
export PKG_CONFIG_LIBDIR_x86_64_pc_windows_gnu="$PKG_CONFIG_LIBDIR"
export CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER="${CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER:-x86_64-w64-mingw32-gcc}"
cargo build --locked --release --target x86_64-pc-windows-gnu
rust_build_dir="${CARGO_TARGET_DIR:-target}"
python3 packaging/package.py --windows-exe "$rust_build_dir/x86_64-pc-windows-gnu/release/kafka-reader-rust.exe"
