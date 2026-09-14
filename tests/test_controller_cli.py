from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rsync_parallel_import.cli import human_status, main, reset_state, status_record
from rsync_parallel_import.controller import Controller, verify_destination
from rsync_parallel_import.errors import ImporterError, PrerequisiteError
from rsync_parallel_import.locking import InstanceLock
from rsync_parallel_import.manifest import Manifest, ManifestEntry, save_manifest
from rsync_parallel_import.rsync import RsyncResult
from rsync_parallel_import.state import StateStore, initialize_state

from .helpers import QueueRunner, make_config


def write_config(path: Path, root: Path) -> None:
    path.write_text(
        f'[source]\nhost="h"\nuser="u"\npath="/s"\n'
        f'[destination]\npath="{root / "destination"}"\n'
        f'[state]\npath="{root / "state"}"\n'
        '[transfer]\nworkers=2\n',
        encoding="utf-8",
    )


class ControllerTests(unittest.TestCase):
    def test_completed_entries_are_not_scheduled_on_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = make_config(root, workers=2)
            config.state_dir.mkdir()
            config.destination.mkdir()
            entries = (ManifestEntry(b"done", 4, 1_000_000_000), ManifestEntry(b"todo", 3, 2_000_000_000))
            manifest = Manifest(config.source.host, config.source.user, config.source.path, entries)
            save_manifest(config.state_dir / "manifest.json", manifest)
            state = initialize_state(manifest.digest, manifest.total_bytes)
            state.completed.add(entries[0].id)
            StateStore(config.state_dir / "state.json").save(state)
            target = config.destination / "done"
            target.write_bytes(b"done")
            os.utime(target, ns=(entries[0].mtime_ns, entries[0].mtime_ns))

            controller = Controller(config, QueueRunner())
            controller.scanner.scan = lambda: manifest
            controller.rsync.reconcile = lambda: RsyncResult(True, 0)
            scheduled = []

            def fake_run(scheduler_self, pending):
                scheduled.extend(pending)
                target = config.destination / "todo"
                target.write_bytes(b"new")
                os.utime(target, ns=(entries[1].mtime_ns, entries[1].mtime_ns))
                return True

            with patch("rsync_parallel_import.controller.verify_prerequisites"), patch(
                "rsync_parallel_import.controller.Scheduler.run", new=fake_run
            ):
                self.assertTrue(controller.run())
            self.assertEqual([entry.path for entry in scheduled], [b"todo"])
            self.assertEqual(StateStore(config.state_dir / "state.json").load().phase, "completed")

    def test_progress_threshold_is_persisted_and_not_logged_twice(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = make_config(root)
            config.state_dir.mkdir()
            config.destination.mkdir()
            target = config.destination / "file"
            target.write_bytes(b"x" * 35)
            mtime = target.stat().st_mtime_ns
            manifest = Manifest(config.source.host, config.source.user, config.source.path, (ManifestEntry(b"file", 100, mtime),))
            StateStore(config.state_dir / "state.json").save(initialize_state(manifest.digest, 100))
            controller = Controller(config, QueueRunner())
            with self.assertLogs("rsync_parallel_import.controller", level="INFO") as logs:
                controller._record_progress(manifest, 1)
            self.assertIn("progress threshold reached: 30%", "\n".join(logs.output))
            with self.assertNoLogs("rsync_parallel_import.controller", level="INFO"):
                controller._record_progress(manifest, 1)
            self.assertEqual(
                StateStore(config.state_dir / "state.json").load().thresholds_emitted,
                {10, 20, 30},
            )

    def test_prerequisite_failure_does_not_make_existing_transfer_terminal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = make_config(root)
            config.state_dir.mkdir()
            state = initialize_state("digest", 1)
            state.phase = "transfer"
            StateStore(config.state_dir / "state.json").save(state)
            controller = Controller(config, QueueRunner())
            with patch(
                "rsync_parallel_import.controller.verify_prerequisites",
                side_effect=PrerequisiteError("network unavailable"),
            ):
                with self.assertRaises(PrerequisiteError):
                    controller.run()
            saved = StateStore(config.state_dir / "state.json").load()
            self.assertEqual(saved.phase, "transfer")
            self.assertIn("network unavailable", saved.last_error)

    def test_verify_destination_detects_missing_size_and_mtime(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "wrong-size").write_bytes(b"x")
            (root / "wrong-time").write_bytes(b"xx")
            manifest = Manifest(
                "h", "u", "/s",
                (
                    ManifestEntry(b"missing", 1, 1),
                    ManifestEntry(b"wrong-size", 2, 1),
                    ManifestEntry(b"wrong-time", 2, 1),
                ),
            )
            issues = verify_destination(manifest, root)
            self.assertEqual(len(issues), 3)
            self.assertTrue(any(item.startswith("missing:") for item in issues))
            self.assertTrue(any(item.startswith("size mismatch:") for item in issues))
            self.assertTrue(any(item.startswith("mtime mismatch:") for item in issues))


class CliTests(unittest.TestCase):
    def test_status_human_and_json_cli(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = root / "config.toml"
            write_config(config_path, root)
            config = make_config(root, workers=2)
            config.state_dir.mkdir()
            manifest = Manifest("source.example.net", "importer", "/srv/source", (ManifestEntry(b"a", 100, 1),))
            save_manifest(config.state_dir / "manifest.json", manifest)
            state = initialize_state(manifest.digest, 100)
            state.phase = "transfer"
            state.transferred_bytes = 50
            state.rate_bytes_per_second = 10
            state.eta_seconds = 5
            StateStore(config.state_dir / "state.json").save(state)
            with InstanceLock(config.state_dir / "controller.lock"):
                record = status_record(config)
                rendered = human_status(record)
            self.assertIn("progress=50.0%", rendered)
            self.assertIn("rate=10 B/s", rendered)
            # The CLI's config identifies a different manifest source only for run/verify;
            # status intentionally reads persistent state without contacting the source.
            with patch("sys.stdout") as stdout:
                self.assertEqual(main(["--config", str(config_path), "status", "--json"]), 0)
                self.assertTrue(stdout.write.called)

    def test_reset_requires_confirmation_and_never_touches_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = make_config(root)
            config.state_dir.mkdir()
            config.destination.mkdir()
            (config.state_dir / "manifest.json").write_text("state")
            (config.destination / "valuable").write_text("keep")
            with self.assertRaises(ImporterError):
                reset_state(config, False)
            reset_state(config, True)
            self.assertFalse((config.state_dir / "manifest.json").exists())
            self.assertEqual((config.destination / "valuable").read_text(), "keep")

    def test_relevant_cli_errors_have_nonzero_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = root / "config.toml"
            write_config(config_path, root)
            self.assertEqual(main(["--config", str(config_path), "reset"]), 2)
            self.assertEqual(main(["--config", str(config_path), "status"]), 0)

    def test_verify_cli_reports_success_and_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = root / "config.toml"
            write_config(config_path, root)
            with patch("rsync_parallel_import.cli.Controller.verify", return_value=[]):
                self.assertEqual(main(["--config", str(config_path), "verify"]), 0)
            with patch("rsync_parallel_import.cli.Controller.verify", return_value=["missing: x"]):
                self.assertEqual(main(["--config", str(config_path), "verify"]), 1)


if __name__ == "__main__":
    unittest.main()
