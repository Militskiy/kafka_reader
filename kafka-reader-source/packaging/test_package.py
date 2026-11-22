import hashlib
import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import package


class PackageTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name) / "source"
        self.out = Path(temp.name) / "dist"
        for name in package.SOURCE:
            path = self.root / name
            if name in {"src", "packaging", "THIRD-PARTY", "examples"} or name.startswith(("docs/screenshots/", "qa/results/")):
                path.mkdir(parents=True)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(name, encoding="utf-8")
        (self.root / "packaging/build-linux.sh").write_bytes(b"#!/usr/bin/env bash\n")
        self.windows = Path(temp.name) / "windows.exe"
        self.linux = Path(temp.name) / "linux"
        self.windows.write_bytes(b"windows binary")
        self.linux.write_bytes(b"linux binary")

        third_party = self.root / "THIRD-PARTY"
        self.native_notice = "cargo/codec-1.2.0/native/COPYING"
        self.shared_notice = "upstream/shared/LICENSE"
        self.notice_bytes = b"Copyright \xa9 Example\r\nNative codec notice\r\n"
        shared = third_party / self.shared_notice
        shared.parent.mkdir(parents=True)
        shared.write_bytes(b"Shared upstream notice\n")

        registry = "registry+https://github.com/rust-lang/crates.io-index"
        self.catalog = [
            {"name": "codec", "version": "1.2.0", "license": "MIT", "source": registry,
             "repository": None, "licenseFiles": [self.native_notice]},
            {"name": "shared", "version": "2.0.0", "license": "MIT", "source": registry,
             "repository": None, "licenseFiles": [self.shared_notice]},
        ]
        self.inventory = third_party / "dependency-inventory.json"
        self.inventory.write_text(json.dumps(self.catalog), encoding="utf-8")
        self.packages = {}
        for entry in self.catalog:
            crate = Path(temp.name) / "registry" / (entry["name"] + "-" + entry["version"])
            crate.mkdir(parents=True)
            self.packages[(entry["name"], entry["version"], entry["source"])] = crate
        native = self.packages[("codec", "1.2.0", registry)] / "native"
        native.mkdir()
        (native / "COPYING").write_bytes(self.notice_bytes)
        # Cargo may unpack crate members with timestamps below the ZIP epoch.
        os.utime(native / "COPYING", (1, 1))

        # A source ZIP from a previous release can still contain old copies.
        stale = third_party / self.native_notice
        stale.parent.mkdir(parents=True)
        stale.write_bytes(b"stale notice")
        (stale.parent / "unlisted").write_bytes(b"unlisted stale file")

        project = patch.object(package, "ROOT", self.root)
        project.start()
        self.addCleanup(project.stop)
        metadata = patch("cargo_notices.locked_packages", return_value=self.packages)
        metadata.start()
        self.addCleanup(metadata.stop)

    def test_sequential_platform_builds_and_fixture_allowlist(self):
        fixture = self.root / "qa/integration"
        (fixture / "broker-classpath.txt").write_text("local dependencies")
        (fixture / "tools-private").mkdir()
        (fixture / "tools-private/local.properties").write_text("local configuration")

        package.package(self.out, windows_exe=self.windows)
        package.package(self.out, linux_exe=self.linux)
        package.package(self.out)

        sums = {}
        for line in (self.out / "SHA256SUMS.txt").read_text().splitlines():
            digest, name = line.split("  ")
            sums[name] = digest
        self.assertEqual(set(sums), set(package.RELEASE_FILES))
        for name, digest in sums.items():
            self.assertEqual(digest, hashlib.sha256((self.out / name).read_bytes()).hexdigest())
        with zipfile.ZipFile(self.out / "kafka-reader-source.zip") as archive:
            prefix = "kafka-reader-source/qa/integration/"
            packaged = {name.removeprefix(prefix) for name in archive.namelist() if name.startswith(prefix)}
            self.assertEqual(packaged, set(package.FIXTURE_FILES))
            mode = archive.getinfo("kafka-reader-source/packaging/build-linux.sh").external_attr >> 16
            self.assertEqual(mode & 0o111, 0o111)

        for kind in ("source", "windows-x64", "linux-x64"):
            base = "kafka-reader-" + kind
            with self.subTest(kind=kind), zipfile.ZipFile(self.out / (base + ".zip")) as archive:
                prefix = base + "/THIRD-PARTY/"
                cargo_files = [p[len(prefix):] for p in archive.namelist() if p.startswith(prefix + "cargo/")]
                self.assertEqual(cargo_files, [self.native_notice])
                self.assertEqual(archive.read(prefix + self.native_notice), self.notice_bytes)
                self.assertEqual(archive.namelist().count(prefix + self.shared_notice), 1)
                self.assertEqual(archive.read(prefix + self.shared_notice), b"Shared upstream notice\n")

    def test_missing_notice_preserves_prior_outputs(self):
        package.package(self.out, linux_exe=self.linux, windows_exe=self.windows)
        before = {path.name: path.read_bytes() for path in self.out.iterdir()}
        self.catalog[0]["licenseFiles"].append("cargo/codec-1.2.0/native/LICENSE-missing")
        self.inventory.write_text(json.dumps(self.catalog), encoding="utf-8")
        self.windows.write_bytes(b"new executable")
        with self.assertRaisesRegex(FileNotFoundError, "LICENSE-missing"):
            package.package(self.out, windows_exe=self.windows)
        self.assertEqual({path.name: path.read_bytes() for path in self.out.iterdir()}, before)

    def test_stale_inventory_fails_packaging(self):
        self.inventory.write_text(json.dumps(self.catalog[1:]), encoding="utf-8")
        with self.assertRaises(ValueError):
            package.package(self.out)


if __name__ == "__main__":
    unittest.main()
