# Third-party notices

The application's source code is covered by the MIT license in `LICENSE`.
Third-party code retains its own copyright and license terms. The included
dependency inventory records declared SPDX license expressions; alternatives
such as `MIT OR Apache-2.0` do not require choosing every alternative.

## Rust dependencies

`THIRD-PARTY/DEPENDENCIES.md` and `THIRD-PARTY/dependency-inventory.json` record the
227 packages in the union of the locked Linux/Windows metadata graphs, including
build dependencies and optional entries that need not all be linked into the
delivered executable. `Cargo.lock` is the authoritative version lockfile.

Every distribution ZIP contains the original crate license texts under
`THIRD-PARTY/cargo/`. Packaging reads those files from Cargo's locked crate
sources using the reviewed inventory. Git tracks the inventory and the shared
upstream, Rust and MinGW notices. Some upstream workspaces do not include a license file in
each published member crate. Their original shared license texts are retained
under `THIRD-PARTY/upstream/`, with the exact upstream commit URLs in
`THIRD-PARTY/upstream/ORIGINS.json`. These cover the egui workspace, AccessKit,
gl-rs, profiling and clipboard-win. The egui default font licenses, including OFL and Ubuntu
Font License texts, are preserved in the `epaint_default_fonts` entry.

Major components:

| Component | Version | License / location |
|---|---|---|
| eframe / egui family | 0.37.0 | MIT OR Apache-2.0; `upstream/egui/` |
| rust-rdkafka | 0.39.0 | MIT; `cargo/rdkafka-0.39.0/` |
| rdkafka-sys Rust binding | 4.11.0+2.16.0 | MIT; wrapper shares rust-rdkafka's license |
| librdkafka native library | 2.16.0 | BSD-2-Clause plus bundled notices; rdkafka-sys `librdkafka/LICENSE*` |
| OpenSSL source in Linux and Windows builds | 3.6.3 | Apache-2.0; `openssl-src` entry includes native OpenSSL license |
| zlib/zstd native compression | Locked in Cargo.lock | Native notices included within `libz-sys` / `zstd-sys` |
| Rust standard library | 1.100.0 toolchain | `rust-standard-library/COPYRIGHT-library.html` and referenced license texts |

The zstd source archive includes alternative GPL terms as well as its BSD
licenses. This distribution uses the available permissive BSD terms. Merely
retaining an upstream alternative license text does not select that alternative.

## Native libraries and the single Windows executable

Version 0.2 builds librdkafka, OpenSSL, zlib and zstd from the pinned Cargo source
archives on both platforms. Their original licenses and bundled attributions
are retained within the corresponding `THIRD-PARTY/cargo/` directories.
The Windows x64 GNU build statically links these native dependencies into the
application. It contains no bundled DLL extraction loader and does not require
companion librdkafka, TLS, compression or MinGW runtime DLLs. Its import table
contains only Windows system libraries.

Original MinGW-w64/GCC copyright and license notices from the build distribution
are retained in `THIRD-PARTY/mingw-runtime/`. These broad notices also cover
toolchain/header components that need not contribute code to this executable.
The GNU runtime exception text is additionally retained with the Rust
standard-library license attribution. The compiler, build tools and complete
toolchain are not redistributed in the application ZIP.

Windows system libraries remain supplied by Windows. On Linux, system
glibc/libgcc/X11/OpenGL libraries remain supplied by the operating system and
are not copied into the ZIP. The direct standalone EXE is also provided inside
the Windows ZIP together with this documentation and the complete license set.

## Regenerating the inventory

When dependencies change, run `cargo metadata --locked --format-version 1`
with `--filter-platform x86_64-unknown-linux-gnu` and again with
`--filter-platform x86_64-pc-windows-gnu`. Save each UTF-8 JSON result, then
run `python packaging/generate-notices.py metadata-linux.json metadata-windows.json`
from the source root. Review the updated inventory and any shared upstream
licenses. Local generated copies under `THIRD-PARTY/cargo/` are ignored.
Original upstream workspace notices are already retained in this source bundle.
The optional `--rust-doc` argument accepts the Rust distribution's documentation
directory to copy its standard-library attribution.
