import hashlib
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import package


class PackageTests(unittest.TestCase):
    def test_sequential_platform_builds_and_fixture_allowlist(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "source"
            out = Path(temp) / "dist"
            for name in package.SOURCE:
                path = root / name
                if name in {"src", "packaging", "THIRD-PARTY", "examples"} or name.startswith(("docs/screenshots/", "qa/results/")):
                    path.mkdir(parents=True)
                else:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(name, encoding="utf-8")
            wrapper = root / "packaging/build-linux.sh"
            wrapper.write_bytes(b"#!/usr/bin/env bash\n")
            fixture = root / "qa/integration"
            (fixture / "broker-classpath.txt").write_text("local dependencies")
            (fixture / "tools-private").mkdir()
            (fixture / "tools-private/local.properties").write_text("local configuration")
            windows = Path(temp) / "windows.exe"
            linux = Path(temp) / "linux"
            windows.write_bytes(b"windows binary")
            linux.write_bytes(b"linux binary")

            with patch.object(package, "ROOT", root):
                package.package(out, windows_exe=windows)
                package.package(out, linux_exe=linux)
                package.package(out)

            sums = {}
            for line in (out / "SHA256SUMS.txt").read_text().splitlines():
                digest, name = line.split("  ")
                sums[name] = digest
            self.assertEqual(set(sums), set(package.RELEASE_FILES))
            for name, digest in sums.items():
                self.assertEqual(digest, hashlib.sha256((out / name).read_bytes()).hexdigest())
            with zipfile.ZipFile(out / "kafka-reader-source.zip") as archive:
                prefix = "kafka-reader-source/qa/integration/"
                packaged = {name.removeprefix(prefix) for name in archive.namelist() if name.startswith(prefix)}
                self.assertEqual(packaged, set(package.FIXTURE_FILES))
                mode = archive.getinfo("kafka-reader-source/packaging/build-linux.sh").external_attr >> 16
                self.assertEqual(mode & 0o111, 0o111)


if __name__ == "__main__":
    unittest.main()
