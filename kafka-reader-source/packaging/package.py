#!/usr/bin/env python3
"""Create source and native portable ZIPs using an explicit deliverable allowlist."""
import argparse
import hashlib
import json
import stat
import shutil
import tempfile
import zipfile
from pathlib import Path

from cargo_notices import notice_files

ROOT = Path(__file__).resolve().parent.parent
COMMON = ["README.md", "QUICKSTART.md", "CHANGELOG.md", "TEST-REPORT.md", "LICENSE",
          "THIRD-PARTY-NOTICES.md", "THIRD-PARTY", "examples", "docs/screenshots/v0.3.0", "qa/results/v0.3.0"]
FIXTURE_FILES = [
    ".gitignore", "FixtureHost.java", "README.md", "pom.xml", "run-bridge-check.py",
    "run-gui-v02.py", "run-gui-v03.py", "run-integration.py", "server.properties",
    "test_fixture_isolation.py",
]
SOURCE = COMMON + ["Cargo.toml", "Cargo.lock", "build.rs", ".cargo/config.toml", "src", "packaging", ".gitignore", ".gitattributes",
                   "AGENTS.md", "CODEX_START.md", "docs/PROJECT_CONTEXT.md", "docs/ARCHITECTURE.md", "docs/DEVELOPMENT.md"]
SOURCE += [f"qa/integration/{name}" for name in FIXTURE_FILES]
RELEASE_FILES = ("kafka-reader-source.zip", "kafka-reader-linux-x64.zip",
                 "kafka-reader-windows-x64.zip", "kafka-reader.exe")


def add_tree(archive, relative, prefix):
    path = ROOT / relative
    if not path.exists():
        raise SystemExit(f"Required deliverable is missing: {relative}")
    if relative == "THIRD-PARTY":
        # An extracted source ZIP or older checkout can already contain generated copies.
        for child in sorted(path.iterdir()):
            if child.name != "cargo":
                add_tree(archive, child.relative_to(ROOT).as_posix(), prefix)
        return
    entries = sorted(path.rglob("*")) if path.is_dir() else [path]
    for file in entries:
        relative_parts = file.relative_to(ROOT).parts
        is_qa_work = any(part == "work" or part.startswith("work-v") for part in relative_parts)
        if file.is_file() and "__pycache__" not in relative_parts and not is_qa_work:
            member = f"{prefix}/{file.relative_to(ROOT).as_posix()}"
            if file.suffix == ".sh" and relative_parts[0] == "packaging":
                executable(archive, file, member)
            else:
                archive.write(file, member)


def executable(archive, file, path):
    info = zipfile.ZipInfo(path)
    info.create_system = 3
    info.external_attr = (stat.S_IFREG | 0o755) << 16
    info.compress_type = zipfile.ZIP_DEFLATED
    archive.writestr(info, file.read_bytes() if isinstance(file, Path) else file)


def create_archives(directory, linux_exe, windows_exe):
    notices = notice_files(ROOT)
    editions = [("source", SOURCE, None)]
    if linux_exe is not None:
        editions.append(("linux-x64", COMMON, linux_exe))
    if windows_exe is not None:
        editions.append(("windows-x64", COMMON, windows_exe))
    for platform, entries, binary in editions:
        destination = directory / f"kafka-reader-{platform}.zip"
        prefix = f"kafka-reader-{platform}"
        # Cargo registry files can have normalized timestamps earlier than ZIP's 1980 minimum.
        with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED,
                             compresslevel=9, strict_timestamps=False) as archive:
            for entry in entries:
                add_tree(archive, entry, prefix)
            for member, original in sorted(notices.items()):
                archive.write(original, f"{prefix}/THIRD-PARTY/{member}")
            if binary is not None:
                executable(archive, binary, f"{prefix}/{'kafka-reader.exe' if platform == 'windows-x64' else 'kafka-reader'}")
    if windows_exe is not None:
        shutil.copyfile(windows_exe, directory / "kafka-reader.exe")


def package(out, linux_exe=None, windows_exe=None):
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=out) as temp:
        staging = Path(temp)
        create_archives(staging, linux_exe, windows_exe)
        manifest = []
        for name in RELEASE_FILES:
            path = staging / name
            if not path.is_file():
                path = out / name
                if not path.is_file():
                    continue
            manifest.append({"file": name, "bytes": path.stat().st_size,
                             "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        sums = staging / "SHA256SUMS.txt"
        sums.write_text("".join(f"{m['sha256']}  {m['file']}\n" for m in manifest), encoding="ascii")
        for name in RELEASE_FILES:
            path = staging / name
            if path.is_file():
                path.replace(out / name)
        sums.replace(out / sums.name)
    return manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--linux-exe", type=Path)
    parser.add_argument("--windows-exe", type=Path)
    parser.add_argument("--out", type=Path, default=ROOT / "dist/v0.3.0")
    args = parser.parse_args()
    manifest = package(args.out, args.linux_exe, args.windows_exe)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
