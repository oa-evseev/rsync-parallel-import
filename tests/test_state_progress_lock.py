from __future__ import annotations

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
from rsync_parallel_import.state import StateStore, initialize_state


class StateTests(unittest.TestCase):
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
