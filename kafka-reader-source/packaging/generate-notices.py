#!/usr/bin/env python3
"""Collect original dependency license texts from locked Cargo metadata.

Run cargo metadata for each shipped target first, then pass both JSON paths.
No source cache paths are written to the resulting redistributable inventory.
"""
import argparse
import json
import re
import shutil
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("metadata", nargs="+")
    parser.add_argument("--out", default="THIRD-PARTY")
    parser.add_argument("--rust-doc", type=Path)
    args = parser.parse_args()
    output = Path(args.out)
    output.mkdir(parents=True, exist_ok=True)
    packages = {}
    for metadata in args.metadata:
        for package in json.loads(Path(metadata).read_text(encoding="utf-8-sig"))["packages"]:
            if package["name"] != "kafka-reader-rust":
                packages[package["id"]] = package
    entries = []
    missing = []
    name_pattern = re.compile(r"(^|[-_.])(licen[cs]e|copying|copyright|notice)([-_.]|$)", re.I)
    for package in sorted(packages.values(), key=lambda p: (p["name"], p["version"])):
        root = Path(package["manifest_path"]).parent
        key = f"{package['name']}-{package['version']}"
        candidates = []
        for path in root.rglob("*"):
            if path.is_file() and path.suffix not in {".rs", ".py", ".pm", ".c", ".h", ".sh"}:
                if name_pattern.search(path.name) or path.name in {"OFL.txt", "UFL.txt"}:
                    candidates.append(path)
        if package.get("license_file"):
            candidate = root / package["license_file"]
            if candidate.is_file() and candidate not in candidates:
                candidates.append(candidate)
        files = []
        for source in candidates:
            relative = source.relative_to(root)
            destination = output / "cargo" / key / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            files.append(destination.relative_to(output).as_posix())
        shared = {
            "clipboard-win": "clipboard-win",
            "accesskit": "accesskit", "gl_generator": "gl-rs", "khronos_api": "gl-rs",
            "profiling": "profiling", "ecolor": "egui", "eframe": "egui", "egui": "egui",
            "egui-wgpu": "egui", "egui-winit": "egui", "egui_glow": "egui",
            "emath": "egui", "epaint": "egui",
        }.get(package["name"])
        if not files and shared:
            files = [p.relative_to(output).as_posix() for p in (output / "upstream" / shared).glob("LICENSE*")]
        entries.append({
            "name": package["name"], "version": package["version"],
            "license": package.get("license"), "repository": package.get("repository"),
            "source": package.get("source"), "licenseFiles": sorted(files),
        })
        if not files:
            missing.append(key)
    (output / "dependency-inventory.json").write_text(json.dumps(entries, indent=2) + "\n")
    lines = ["# Locked Rust dependency inventory", "", "The union of the Linux x64 and Windows x64 build graphs is listed below.",
             "Original license texts are in the `cargo/` directory. The app license is separate.", "",
             "| Package | Version | Declared license |", "|---|---|---|"]
    lines += [f"| {p['name']} | {p['version']} | {p['license']} |" for p in entries]
    if missing:
        lines += ["", "Packages without separate license files in their published crate archive:", ""]
        lines += [f"- {name}" for name in missing]
    (output / "DEPENDENCIES.md").write_text("\n".join(lines) + "\n")
    if args.rust_doc:
        rust = output / "rust-standard-library"
        rust.mkdir(exist_ok=True)
        shutil.copyfile(args.rust_doc / "COPYRIGHT-library.html", rust / "COPYRIGHT-library.html")
        shutil.copytree(args.rust_doc / "licenses", rust / "licenses", dirs_exist_ok=True)
    print(json.dumps({"dependencies": len(entries), "missingSeparateLicense": missing}))


if __name__ == "__main__":
    main()
