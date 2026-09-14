from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import Future
from pathlib import Path
from unittest.mock import patch

from rsync_parallel_import.manifest import ManifestEntry
from rsync_parallel_import.rsync import (
    RsyncExecutor,
    RsyncResult,
    build_reconcile_command,
    build_transfer_command,
    file_list,
)
from rsync_parallel_import.scheduler import (
    Scheduler,
    backoff_seconds,
    balance_entries,
    retry_delay_seconds,
)
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

    def test_retry_jitter_is_deterministic_and_respects_cap(self):
        self.assertEqual(retry_delay_seconds(1, 10, 30, 0, 0), 10)
        self.assertEqual(retry_delay_seconds(1, 10, 30, 0.2, 0), 8)
        self.assertEqual(retry_delay_seconds(1, 10, 30, 0.2, 1), 12)
        self.assertEqual(retry_delay_seconds(3, 10, 30, 0.2, 0), 24)
        self.assertEqual(retry_delay_seconds(3, 10, 30, 0.2, 1), 30)


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class FakeExecutor:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def transfer(self, entries):
        self.calls.append(tuple(entries))
        return self.outcomes.pop(0)


class InlinePool:
    def __init__(self, max_workers, thread_name_prefix):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def submit(self, function, *args):
        future = Future()
        try:
            future.set_result(function(*args))
        except Exception as exc:
            future.set_exception(exc)
        return future


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

    def test_configured_worker_start_stagger_spaces_launches(self):
        entries = tuple(ManifestEntry(str(index).encode(), 1, 1) for index in range(3))
        with tempfile.TemporaryDirectory() as directory:
            store = self.make_store(directory, entries)
            clock = FakeClock()

            class TimedExecutor:
                def __init__(self):
                    self.starts = []

                def transfer(self, assigned):
                    self.starts.append(clock.clock())
                    return RsyncResult(True, 0)

            executor = TimedExecutor()
            config = make_config(
                Path(directory),
                workers=3,
                worker_start_stagger_seconds=0.2,
                poll_interval_seconds=2,
            ).transfer
            with patch("rsync_parallel_import.scheduler.ThreadPoolExecutor", InlinePool):
                self.assertTrue(
                    Scheduler(
                        config, executor, store, sleep=clock.sleep, clock=clock.clock
                    ).run(entries)
                )
            self.assertEqual(executor.starts, [0.0, 0.2, 0.4])

    def test_single_worker_starts_immediately_without_stagger_sleep(self):
        entries = (ManifestEntry(b"only", 1, 1),)
        with tempfile.TemporaryDirectory() as directory:
            store = self.make_store(directory, entries)
            clock = FakeClock()

            class TimedExecutor:
                def __init__(self):
                    self.starts = []

                def transfer(self, assigned):
                    self.starts.append(clock.clock())
                    return RsyncResult(True, 0)

            executor = TimedExecutor()
            config = make_config(
                Path(directory), workers=1, worker_start_stagger_seconds=10
            ).transfer
            with patch("rsync_parallel_import.scheduler.ThreadPoolExecutor", InlinePool):
                self.assertTrue(
                    Scheduler(
                        config, executor, store, sleep=clock.sleep, clock=clock.clock
                    ).run(entries)
                )
            self.assertEqual(executor.starts, [0.0])
            self.assertEqual(clock.sleeps, [])

    def test_simultaneous_due_retries_are_staggered(self):
        entries = (ManifestEntry(b"a", 2, 1), ManifestEntry(b"b", 1, 1))
        with tempfile.TemporaryDirectory() as directory:
            store = self.make_store(directory, entries)
            clock = FakeClock()
            starts = []

            class BatchFailurePool(InlinePool):
                def __init__(self, max_workers, thread_name_prefix):
                    self.initial = []
                    self.submissions = 0

                def submit(self, function, *args):
                    self.submissions += 1
                    starts.append(clock.clock())
                    future = Future()
                    if self.submissions <= 2:
                        self.initial.append(future)
                        if self.submissions == 2:
                            for initial in self.initial:
                                initial.set_result(RsyncResult(False, 12, "shared outage"))
                    else:
                        future.set_result(RsyncResult(True, 0))
                    return future

            config = make_config(
                Path(directory),
                workers=2,
                worker_start_stagger_seconds=0.2,
                max_attempts=3,
                backoff_initial_seconds=1,
                backoff_max_seconds=1,
                retry_jitter_fraction=0,
                poll_interval_seconds=0.25,
            ).transfer
            with patch(
                "rsync_parallel_import.scheduler.ThreadPoolExecutor", BatchFailurePool
            ):
                self.assertTrue(
                    Scheduler(
                        config, FakeExecutor([]), store, sleep=clock.sleep, clock=clock.clock
                    ).run(entries)
                )
            self.assertEqual(starts[:2], [0.0, 0.2])
            self.assertEqual(starts[2:], [1.2, 1.4])

    def test_disabled_jitter_does_not_consume_random_source(self):
        entries = (ManifestEntry(b"one", 1, 1),)
        with tempfile.TemporaryDirectory() as directory:
            store = self.make_store(directory, entries)
            clock = FakeClock()
            executor = FakeExecutor(
                [RsyncResult(False, 12, "network"), RsyncResult(True, 0)]
            )
            config = make_config(
                Path(directory),
                workers=1,
                max_attempts=2,
                backoff_initial_seconds=1,
                backoff_max_seconds=1,
                retry_jitter_fraction=0,
                poll_interval_seconds=0.25,
            ).transfer

            def unexpected_random():
                self.fail("disabled jitter consumed randomness")

            with patch("rsync_parallel_import.scheduler.ThreadPoolExecutor", InlinePool):
                self.assertTrue(
                    Scheduler(
                        config,
                        executor,
                        store,
                        sleep=clock.sleep,
                        clock=clock.clock,
                        jitter_source=unexpected_random,
                    ).run(entries)
                )

    def test_enabled_jitter_uses_injected_random_source(self):
        entries = (ManifestEntry(b"one", 1, 1),)
        with tempfile.TemporaryDirectory() as directory:
            store = self.make_store(directory, entries)
            clock = FakeClock()

            class TimedExecutor(FakeExecutor):
                def __init__(self):
                    super().__init__(
                        [RsyncResult(False, 12, "network"), RsyncResult(True, 0)]
                    )
                    self.starts = []

                def transfer(self, assigned):
                    self.starts.append(clock.clock())
                    return super().transfer(assigned)

            executor = TimedExecutor()
            samples = iter([0.0])
            config = make_config(
                Path(directory),
                workers=1,
                max_attempts=2,
                backoff_initial_seconds=10,
                backoff_max_seconds=30,
                retry_jitter_fraction=0.2,
                poll_interval_seconds=2,
            ).transfer
            with patch("rsync_parallel_import.scheduler.ThreadPoolExecutor", InlinePool):
                self.assertTrue(
                    Scheduler(
                        config,
                        executor,
                        store,
                        sleep=clock.sleep,
                        clock=clock.clock,
                        jitter_source=lambda: next(samples),
                    ).run(entries)
                )
            self.assertEqual(executor.starts, [0.0, 8.0])

    def test_retry_launch_moves_error_from_current_to_transient_history(self):
        entries = (ManifestEntry(b"one", 1, 1),)
        with tempfile.TemporaryDirectory() as directory:
            store = self.make_store(directory, entries)
            clock = FakeClock()
            executor = FakeExecutor(
                [RsyncResult(False, 12, "temporary reset"), RsyncResult(True, 0)]
            )
            observed = []
            config = make_config(
                Path(directory),
                workers=1,
                max_attempts=2,
                backoff_initial_seconds=1,
                backoff_max_seconds=1,
                retry_jitter_fraction=0,
                poll_interval_seconds=0.25,
            ).transfer

            def monitor(active):
                state = store.load()
                if len(executor.calls) == 2:
                    observed.append((state.last_error, state.last_transient_error))

            with patch("rsync_parallel_import.scheduler.ThreadPoolExecutor", InlinePool):
                self.assertTrue(
                    Scheduler(
                        config,
                        executor,
                        store,
                        monitor=monitor,
                        sleep=clock.sleep,
                        clock=clock.clock,
                    ).run(entries)
                )
            self.assertIn((None, "temporary reset"), observed)
            state = store.load()
            self.assertIsNone(state.last_error)
            self.assertEqual(state.last_transient_error, "temporary reset")

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
            self.assertEqual(state.failed[entries[0].id], "persistent failure")
            self.assertEqual(state.last_error, "persistent failure")
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
