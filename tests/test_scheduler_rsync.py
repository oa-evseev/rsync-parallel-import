from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from rsync_parallel_import.manifest import ManifestEntry
from rsync_parallel_import.rsync import (
    RsyncExecutor,
    RsyncResult,
    build_reconcile_command,
    build_transfer_command,
    file_list,
)
from rsync_parallel_import.scheduler import Scheduler, backoff_seconds, balance_entries
from rsync_parallel_import.state import StateStore, initialize_state

from .helpers import QueueRunner, make_config


class CommandTests(unittest.TestCase):
    def test_transfer_command_and_file_list_do_not_use_shell_quoting(self):
        config = make_config(Path("/tmp/root with spaces"))
        command = build_transfer_command(config)
        self.assertIsInstance(command, list)
        self.assertIn("--protect-args", command)
        self.assertIn("--from0", command)
        self.assertNotIn("--delete", command)
        self.assertEqual(command[-2], "importer@source.example.net:/srv/source/")
        paths = (ManifestEntry(b"a b\n'\"", 1, 1), ManifestEntry(b"x; touch PWNED", 1, 1))
        self.assertEqual(file_list(paths), b"a b\n'\"\0x; touch PWNED\0")

    def test_reconciliation_is_serial_archive_hardlinks_and_non_destructive(self):
        config = make_config(Path("/tmp/root"))
        command = build_reconcile_command(config)
        self.assertIn("-aH", command)
        self.assertNotIn("--files-from=-", command)
        self.assertNotIn("--delete", command)
        runner = QueueRunner()
        self.assertTrue(RsyncExecutor(config, runner).reconcile().success)
        self.assertEqual(runner.calls[0][0], command)


class BalancingTests(unittest.TestCase):
    def test_deterministic_byte_balancing(self):
        entries = tuple(ManifestEntry(str(index).encode(), size, 1) for index, size in enumerate([9, 8, 7, 6, 5, 4]))
        first = balance_entries(entries, 3)
        second = balance_entries(tuple(reversed(entries)), 3)
        self.assertEqual(first, second)
        totals = [item.total_bytes for item in first]
        self.assertEqual(totals, [13, 13, 13])

    def test_backoff_is_bounded_exponential(self):
        self.assertEqual([backoff_seconds(i, 5, 30) for i in range(1, 6)], [5, 10, 20, 30, 30])


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class FakeExecutor:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def transfer(self, entries):
        self.calls.append(tuple(entries))
        return self.outcomes.pop(0)


class SchedulerTests(unittest.TestCase):
    def make_store(self, root, entries):
        store = StateStore(Path(root) / "state.json")
        store.save(initialize_state("digest", sum(entry.size for entry in entries)))
        return store

    def test_retry_then_success_updates_persistent_state(self):
        entries = (ManifestEntry(b"large file", 100, 1),)
        with tempfile.TemporaryDirectory() as directory:
            store = self.make_store(directory, entries)
            executor = FakeExecutor([RsyncResult(False, 12, "network"), RsyncResult(True, 0)])
            clock = FakeClock()
            config = make_config(
                Path(directory),
                workers=1,
                max_attempts=3,
                backoff_initial_seconds=2,
                backoff_max_seconds=10,
                poll_interval_seconds=0.01,
            ).transfer
            result = Scheduler(
                config, executor, store, sleep=clock.sleep, clock=clock.clock
            ).run(entries)
            state = store.load()
            self.assertTrue(result)
            self.assertEqual(len(executor.calls), 2)
            self.assertEqual(state.retry_count, 1)
            self.assertEqual(state.attempts[entries[0].id], 1)
            self.assertIn(entries[0].id, state.completed)
            self.assertFalse(state.failed)

    def test_failed_worker_does_not_stop_healthy_worker_and_stays_visible(self):
        entries = (ManifestEntry(b"a", 100, 1), ManifestEntry(b"b", 90, 1))
        with tempfile.TemporaryDirectory() as directory:
            store = self.make_store(directory, entries)

            class PerPathExecutor:
                def transfer(self, assigned):
                    return RsyncResult(assigned[0].path == b"b", 12, "persistent failure")

            clock = FakeClock()
            config = make_config(
                Path(directory),
                workers=2,
                max_attempts=2,
                backoff_initial_seconds=0.01,
                backoff_max_seconds=0.01,
                poll_interval_seconds=0.01,
            ).transfer
            result = Scheduler(
                config, PerPathExecutor(), store, sleep=clock.sleep, clock=clock.clock
            ).run(entries)
            state = store.load()
            self.assertFalse(result)
            self.assertIn(entries[1].id, state.completed)
            self.assertIn(entries[0].id, state.failed)
            self.assertEqual(state.attempts[entries[0].id], 2)

    def test_failed_batch_preserves_files_that_are_already_complete(self):
        entries = (ManifestEntry(b"done", 10, 1), ManifestEntry(b"unfinished", 9, 1))
        with tempfile.TemporaryDirectory() as directory:
            store = self.make_store(directory, entries)
            executor = FakeExecutor(
                [RsyncResult(False, 12, "link dropped"), RsyncResult(True, 0)]
            )
            clock = FakeClock()
            config = make_config(
                Path(directory),
                workers=1,
                max_attempts=3,
                backoff_initial_seconds=0.01,
                backoff_max_seconds=0.01,
                poll_interval_seconds=0.01,
            ).transfer
            result = Scheduler(
                config,
                executor,
                store,
                completion_probe=lambda entry: entry.path == b"done",
                sleep=clock.sleep,
                clock=clock.clock,
            ).run(entries)
            state = store.load()
            self.assertTrue(result)
            self.assertEqual([entry.path for entry in executor.calls[1]], [b"unfinished"])
            self.assertNotIn(entries[0].id, state.attempts)
            self.assertIn(entries[0].id, state.completed)


if __name__ == "__main__":
    unittest.main()
