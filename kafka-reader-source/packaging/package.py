#!/usr/bin/env python3
"""Create source and native portable ZIPs using an explicit deliverable allowlist."""
import argparse
import hashlib
import json
import stat
import shutil
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMMON = ["README.md", "QUICKSTART.md", "CHANGELOG.md", "TEST-REPORT.md", "LICENSE",
          "THIRD-PARTY-NOTICES.md", "THIRD-PARTY", "examples", "docs/screenshots/v0.3.0", "qa/results/v0.3.0"]
SOURCE = COMMON + ["Cargo.toml", "Cargo.lock", "build.rs", ".cargo/config.toml", "src", "packaging", "qa/integration", ".gitignore",
                   "AGENTS.md", "CODEX_START.md", "docs/PROJECT_CONTEXT.md", "docs/ARCHITECTURE.md", "docs/DEVELOPMENT.md"]


def add_tree(archive, relative, prefix):
    path = ROOT / relative
    if not path.exists():
        raise SystemExit(f"Required deliverable is missing: {relative}")
    entries = sorted(path.rglob("*")) if path.is_dir() else [path]
    for file in entries:
        relative_parts = file.relative_to(ROOT).parts
        is_qa_work = any(part == "work" or part.startswith("work-v") for part in relative_parts)
        if file.is_file() and "__pycache__" not in relative_parts and not is_qa_work:
            archive.write(file, f"{prefix}/{file.relative_to(ROOT).as_posix()}")


def executable(archive, file, path):
    info = zipfile.ZipInfo(path)
    info.create_system = 3
    info.external_attr = (stat.S_IFREG | 0o755) << 16
    info.compress_type = zipfile.ZIP_DEFLATED
    archive.writestr(info, file.read_bytes() if isinstance(file, Path) else file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--linux-exe", type=Path)
    parser.add_argument("--windows-exe", type=Path)
    parser.add_argument("--out", type=Path, default=ROOT / "dist/v0.3.0")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    outputs = []
    source = args.out / "kafka-reader-source.zip"
    with zipfile.ZipFile(source, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for entry in SOURCE:
            add_tree(archive, entry, "kafka-reader-source")
    outputs.append(source)
    for platform, binary in [("linux-x64", args.linux_exe), ("windows-x64", args.windows_exe)]:
        if binary is None:
            continue
        destination = args.out / f"kafka-reader-{platform}.zip"
        prefix = f"kafka-reader-{platform}"
        with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
            for entry in COMMON:
                add_tree(archive, entry, prefix)
            executable(archive, binary, f"{prefix}/{'kafka-reader.exe' if platform == 'windows-x64' else 'kafka-reader'}")
        outputs.append(destination)
    if args.windows_exe is not None:
        standalone = args.out / "kafka-reader.exe"
        if args.windows_exe.resolve() != standalone.resolve():
            shutil.copyfile(args.windows_exe, standalone)
        outputs.append(standalone)
    manifest = [{"file": p.name, "bytes": p.stat().st_size,
                 "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in outputs]
    (args.out / "SHA256SUMS.txt").write_text("".join(f"{m['sha256']}  {m['file']}\n" for m in manifest))
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
