from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from rsync_parallel_import.config import load_config
from rsync_parallel_import.errors import ConfigurationError, ManifestError, SourceChangedError
from rsync_parallel_import.manifest import (
    Manifest,
    ManifestEntry,
    RemoteManifestScanner,
    assert_same_source,
    load_manifest,
    parse_scan_output,
    save_manifest,
)

from .helpers import QueueRunner, make_config


class ConfigTests(unittest.TestCase):
    def test_minimal_config_and_defaults(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text(
                '[source]\nhost="host"\nuser="user"\npath="/source"\n'
                '[destination]\npath="/destination"\n[transfer]\nworkers=16\n',
                encoding="utf-8",
            )
            config = load_config(path)
            self.assertEqual(config.source.target, "user@host")
            self.assertEqual(config.transfer.workers, 16)
            self.assertEqual(config.state_dir, Path("/var/lib/rsync-parallel-import"))
            self.assertEqual(config.transfer.max_attempts, 5)

    def test_strict_validation(self):
        cases = [
            '[source]\nhost="h"\nuser="u"\npath="relative"\n[destination]\npath="/d"\n[transfer]\nworkers=1',
            '[source]\nhost="h"\nuser="u"\npath="/s"\nextra=1\n[destination]\npath="/d"\n[transfer]\nworkers=1',
            '[source]\nhost="h"\nuser="u"\npath="/s"\n[destination]\npath="d"\n[transfer]\nworkers=0',
            '[source]\nhost="h"\nuser="u"\npath="/s"\n[destination]\npath="/d"\n[transfer]\nworkers=true',
            '[source]\nhost="h"\nuser="u"\npath="/s"\n[destination]\npath="/d"\n[transfer]\nworkers=1\npartial_dir_name="a/b"',
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            for text in cases:
                path.write_text(text, encoding="utf-8")
                with self.subTest(text=text), self.assertRaises(ConfigurationError):
                    load_config(path)


class ManifestTests(unittest.TestCase):
    def test_unusual_filenames_are_nul_parsed_and_round_trip(self):
        paths = [
            b"space name",
            b"quote'\"name",
            "unicod\N{SNOWMAN}".encode(),
            b"line\nbreak",
            b"tab\tname",
            b"nonutf8-\xff",
        ]
        payload = b"".join(path + b"\0" + b"12\0" + b"123.000000001\0" for path in paths)
        entries = parse_scan_output(payload)
        self.assertEqual([entry.path for entry in entries], sorted(paths))
        self.assertTrue(all(entry.mtime_ns == 123_000_000_001 for entry in entries))
        with tempfile.TemporaryDirectory() as directory:
            manifest = Manifest("h", "u", "/s", entries)
            path = Path(directory) / "manifest.json"
            save_manifest(path, manifest)
            first = path.read_bytes()
            save_manifest(path, manifest)
            self.assertEqual(first, path.read_bytes())
            self.assertEqual(load_manifest(path), manifest)

    def test_scanner_uses_static_script_and_safe_base64_argument(self):
        output = b"hello world\0" + b"7\0" + b"5.5\0"
        runner = QueueRunner([subprocess.CompletedProcess([], 0, output, b"")])
        config = make_config(Path("/tmp/example"))
        manifest = RemoteManifestScanner(config.source, runner).scan()
        command, script, _ = runner.calls[0]
        self.assertEqual(command[:4], ["ssh", "-o", "BatchMode=yes", "importer@source.example.net"])
        self.assertEqual(command[4:8], ["sh", "-s", "--", "L3Nydi9zb3VyY2U="])
        self.assertIn(b"find . -type f -printf", script)
        self.assertEqual(manifest.entries[0].path, b"hello world")

    def test_manifest_corruption_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            manifest = Manifest("h", "u", "/s", (ManifestEntry(b"x", 1, 2),))
            save_manifest(path, manifest)
            text = path.read_text("ascii").replace('"size": 1', '"size": 2')
            path.write_text(text, "ascii")
            with self.assertRaises(ManifestError):
                load_manifest(path)

    def test_source_change_added_removed_and_metadata(self):
        original = Manifest(
            "h", "u", "/s", (ManifestEntry(b"a", 1, 1), ManifestEntry(b"b", 2, 2))
        )
        changed = Manifest(
            "h", "u", "/s", (ManifestEntry(b"a", 9, 1), ManifestEntry(b"c", 2, 2))
        )
        with self.assertRaisesRegex(SourceChangedError, "missing=1.*added=1.*changed=1"):
            assert_same_source(original, changed)

    def test_unsafe_manifest_path_rejected(self):
        for path in (b"", b"/absolute", b"../escape", b"a/../b", b"a//b"):
            with self.subTest(path=path), self.assertRaises(ManifestError):
                ManifestEntry(path, 1, 1)


if __name__ == "__main__":
    unittest.main()
