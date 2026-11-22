"""Resolve the reviewed notice inventory to original files in Cargo's locked sources."""
import json
import subprocess
from pathlib import Path, PurePosixPath


def locked_packages(root):
    packages = {}
    for target in ("x86_64-unknown-linux-gnu", "x86_64-pc-windows-gnu"):
        metadata = json.loads(subprocess.check_output(
            ["cargo", "metadata", "--locked", "--format-version", "1", "--filter-platform", target],
            cwd=root,
        ))
        for package in metadata["packages"]:
            if package["id"] == metadata["resolve"]["root"]:
                continue
            key = (package["name"], package["version"], package["source"])
            packages[key] = Path(package["manifest_path"]).parent
    return packages


def notice_files(root):
    third_party = root / "THIRD-PARTY"
    inventory = json.loads((third_party / "dependency-inventory.json").read_text(encoding="utf-8"))
    sources = locked_packages(root)
    expected = {(entry["name"], entry["version"], entry["source"]) for entry in inventory}
    if sources.keys() != expected or len(inventory) != len(expected):
        raise ValueError("Dependency notice inventory differs from locked Cargo metadata. "
                         "Refresh THIRD-PARTY/dependency-inventory.json before packaging.")

    files = {}
    for entry in inventory:
        name, version, source = entry["name"], entry["version"], entry["source"]
        if not entry["licenseFiles"]:
            raise ValueError(f"No notice files listed for {name} {version}")
        for member in entry["licenseFiles"]:
            parts = PurePosixPath(member).parts
            if len(parts) < 3 or ".." in parts or "\\" in member or ":" in member:
                raise ValueError(f"Invalid notice path: {member}")
            if parts[0] == "upstream":
                path = third_party / member  # Included with the maintained notices.
            elif parts[:2] == ("cargo", f"{name}-{version}"):
                path = sources[name, version, source].joinpath(*parts[2:])
                files[member] = path
            else:
                raise ValueError(f"Unexpected notice path for {name} {version}: {member}")
            if not path.is_file():
                raise FileNotFoundError(f"Missing notice {member} at {path}")
    return files
