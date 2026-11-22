$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)

# Rust GNU target + Python 3 + MinGW-w64 GCC/binutils + CMake/Ninja/Perl/make.
# The Linux cross-build route is the route used for the delivered executable.
$env:CMAKE_GENERATOR = 'Ninja'
$env:OPENSSL_STATIC = '1'
$env:RUSTFLAGS = '-C target-feature=+crt-static -C link-arg=-static-libgcc -C link-arg=-static-libstdc++'
$env:PKG_CONFIG_PATH = ''
$env:PKG_CONFIG_PATH_x86_64_pc_windows_gnu = ''
New-Item -ItemType Directory -Force -Path target/empty-pkgconfig | Out-Null
$env:PKG_CONFIG_LIBDIR = (Join-Path (Get-Location) 'target/empty-pkgconfig')
$env:PKG_CONFIG_LIBDIR_x86_64_pc_windows_gnu = $env:PKG_CONFIG_LIBDIR
if (Get-Command pkgconf.exe -ErrorAction SilentlyContinue) { $env:PKG_CONFIG = 'pkgconf.exe' }
if (Get-Command x86_64-w64-mingw32-gcc.exe -ErrorAction SilentlyContinue) {
    $env:CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER = 'x86_64-w64-mingw32-gcc.exe'
} elseif (Get-Command gcc.exe -ErrorAction SilentlyContinue) {
    $env:CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER = 'gcc.exe'
} else { throw 'Add MSYS2 mingw64/bin to PATH (x64 GCC, binutils, and pkgconf).' }
cargo build --locked --release --target x86_64-pc-windows-gnu
if ($LASTEXITCODE -ne 0) { throw 'Rust Windows build failed.' }
$rustBuildDirectory = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { 'target' }
python packaging/package.py --windows-exe (Join-Path $rustBuildDirectory 'x86_64-pc-windows-gnu/release/kafka-reader-rust.exe')
if ($LASTEXITCODE -ne 0) { throw 'Portable ZIP packaging failed.' }
Write-Host 'A genuine single EXE, Windows ZIP and source ZIP are in dist/v0.3.0/. No native companion DLLs are required. Windows execution still needs validation.'
