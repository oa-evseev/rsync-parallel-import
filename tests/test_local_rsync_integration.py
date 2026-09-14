from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


@unittest.skipUnless(shutil.which("rsync"), "local rsync is not installed")
class LocalRsyncIntegrationTests(unittest.TestCase):
    def test_nul_file_list_transfers_newline_and_quote_names(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            destination = root / "destination"
            source.mkdir()
            destination.mkdir()
            names = [b"space and 'quotes'", b"embedded\nnewline", b"unicode-\xe2\x98\x83"]
            source_bytes = os.fsencode(source)
            for index, name in enumerate(names):
                with open(os.path.join(source_bytes, name), "wb") as handle:
                    handle.write(bytes([index + 1]) * (index + 2))
            result = subprocess.run(
                [
                    "rsync",
                    "-a",
                    "--from0",
                    "--files-from=-",
                    "--",
                    os.fspath(source) + "/",
                    os.fspath(destination) + "/",
                ],
                input=b"\0".join(names) + b"\0",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr.decode("utf-8", "replace"))
            for index, name in enumerate(names):
                with open(os.path.join(os.fsencode(destination), name), "rb") as handle:
                    self.assertEqual(handle.read(), bytes([index + 1]) * (index + 2))

    def test_serial_archive_hardlink_pass_repairs_partitioned_links(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            destination = root / "destination"
            source.mkdir()
            destination.mkdir()
            (source / "first").write_bytes(b"shared payload")
            os.link(source / "first", source / "second")
            # Model independent parallel workers: ordinary archive copies do
            # not know that these two separate invocations share an inode.
            for name in ("first", "second"):
                result = subprocess.run(
                    ["rsync", "-a", "--", str(source / name), str(destination) + "/"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                )
                self.assertEqual(result.returncode, 0)
            self.assertNotEqual((destination / "first").stat().st_ino, (destination / "second").stat().st_ino)
            result = subprocess.run(
                ["rsync", "-aH", "--", str(source) + "/", str(destination) + "/"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr.decode("utf-8", "replace"))
            self.assertEqual((destination / "first").stat().st_ino, (destination / "second").stat().st_ino)


if __name__ == "__main__":
    unittest.main()
