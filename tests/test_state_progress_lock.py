from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rsync_parallel_import.errors import LockError
from rsync_parallel_import.locking import InstanceLock, lock_is_held
from rsync_parallel_import.manifest import Manifest, ManifestEntry
from rsync_parallel_import.progress import (
    RollingRate,
    make_snapshot,
    newly_crossed_thresholds,
    transferred_bytes,
)
from rsync_parallel_import.state import STATE_VERSION, StateStore, initialize_state


class StateTests(unittest.TestCase):
    def test_version_1_state_migrates_without_losing_import_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "manifest_digest": "digest",
                        "phase": "transfer",
                        "completed": ["done-id"],
                        "failed": {},
                        "attempts": {"retry-id": 2},
                        "retry_count": 7,
                        "thresholds_emitted": [10, 20, 30],
                        "progress": {
                            "transferred_bytes": 40,
                            "total_bytes": 100,
                            "rate_bytes_per_second": 5.0,
                            "eta_seconds": 12.0,
                            "active_workers": 1,
                            "total_workers": 16,
                        },
                        "last_error": "old transient reset",
                    }
                ),
                encoding="utf-8",
            )
            state = StateStore(path).load()
            self.assertEqual(state.version, STATE_VERSION)
            self.assertEqual(state.completed, {"done-id"})
            self.assertEqual(state.attempts, {"retry-id": 2})
            self.assertEqual(state.retry_count, 7)
            self.assertEqual(state.thresholds_emitted, {10, 20, 30})
            self.assertEqual((state.transferred_bytes, state.total_bytes), (40, 100))
            self.assertIsNone(state.last_error)
            self.assertEqual(state.last_transient_error, "old transient reset")

    def test_version_1_terminal_error_remains_current(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "manifest_digest": "digest",
                        "phase": "failed",
                        "failed": {"failed-id": "permission denied"},
                        "last_error": "permission denied",
                    }
                ),
                encoding="utf-8",
            )
            state = StateStore(path).load()
            self.assertEqual(state.failed, {"failed-id": "permission denied"})
            self.assertEqual(state.last_error, "permission denied")
            self.assertIsNone(state.last_transient_error)

    def test_atomic_state_preserves_old_file_when_replace_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            store = StateStore(path)
            state = initialize_state("digest", 100)
            store.save(state)
            original = path.read_bytes()
            state.phase = "transfer"
            with patch("rsync_parallel_import.util.os.replace", side_effect=OSError("boom")):
                with self.assertRaises(OSError):
                    store.save(state)
            self.assertEqual(path.read_bytes(), original)
            self.assertFalse(any(item.name.startswith(".state.json.") for item in path.parent.iterdir()))

    def test_lock_excludes_second_controller(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lock"
            first = InstanceLock(path)
            second = InstanceLock(path)
            first.acquire()
            try:
                self.assertTrue(lock_is_held(path))
                with self.assertRaises(LockError):
                    second.acquire()
            finally:
                first.release()
            self.assertFalse(lock_is_held(path))
            second.acquire()
            second.release()


class ProgressTests(unittest.TestCase):
    def test_rolling_rate_window_and_eta(self):
        rate = RollingRate(60)
        self.assertEqual(rate.add(0, 0), 0)
        self.assertEqual(rate.add(100, 10), 10)
        self.assertAlmostEqual(rate.add(700, 70), 10)
        snapshot = make_snapshot(700, 1000, 10)
        self.assertEqual(snapshot.percentage, 70)
        self.assertEqual(snapshot.eta_seconds, 30)
        self.assertIsNone(make_snapshot(1, 10, 0).eta_seconds)

    def test_thresholds_persist_without_duplicate_emission(self):
        emitted = {10, 20}
        crossed = newly_crossed_thresholds(35, 100, emitted)
        self.assertEqual(crossed, [30])
        emitted.update(crossed)
        self.assertEqual(newly_crossed_thresholds(35, 100, emitted), [])
        self.assertEqual(newly_crossed_thresholds(100, 100, emitted), [40, 50, 60, 70, 80, 90, 100])

    def test_destination_and_partial_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a").write_bytes(b"12345")
            (root / "sub" / ".partial").mkdir(parents=True)
            (root / "sub" / ".partial" / "b").write_bytes(b"123")
            manifest = Manifest(
                "h",
                "u",
                "/s",
                (
                    ManifestEntry(b"a", 5, (root / "a").stat().st_mtime_ns),
                    ManifestEntry(b"sub/b", 10, 1),
                ),
            )
            self.assertEqual(transferred_bytes(manifest, root, ".partial"), 8)

    def test_active_rsync_temp_file_contributes_when_identifiable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".large.bin.A1b2C3").write_bytes(b"1234567")
            manifest = Manifest("h", "u", "/s", (ManifestEntry(b"large.bin", 100, 1),))
            self.assertEqual(transferred_bytes(manifest, root, ".partial"), 7)


if __name__ == "__main__":
    unittest.main()
