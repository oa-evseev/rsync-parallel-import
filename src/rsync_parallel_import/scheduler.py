from __future__ import annotations

import logging
import random
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable, Sequence

from .config import TransferConfig
from .manifest import ManifestEntry
from .rsync import RsyncExecutor, RsyncResult
from .state import StateStore, TransferState

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkItem:
    worker_id: int
    entries: tuple[ManifestEntry, ...]

    @property
    def total_bytes(self) -> int:
        return sum(entry.size for entry in self.entries)


def balance_entries(entries: Sequence[ManifestEntry], workers: int) -> list[WorkItem]:
    """Deterministic largest-first greedy bin packing by logical bytes."""
    if workers < 1:
        raise ValueError("workers must be positive")
    bins: list[list[ManifestEntry]] = [[] for _ in range(min(workers, max(1, len(entries))))]
    sizes = [0] * len(bins)
    for entry in sorted(entries, key=lambda item: (-item.size, item.path)):
        index = min(range(len(bins)), key=lambda candidate: (sizes[candidate], candidate))
        bins[index].append(entry)
        sizes[index] += entry.size
    return [WorkItem(index + 1, tuple(items)) for index, items in enumerate(bins) if items]


def backoff_seconds(attempt: int, initial: float, maximum: float) -> float:
    return min(maximum, initial * (2 ** max(0, attempt - 1)))


def retry_delay_seconds(
    attempt: int,
    initial: float,
    maximum: float,
    jitter_fraction: float,
    random_value: float,
) -> float:
    """Return capped exponential backoff with symmetric proportional jitter."""
    nominal = backoff_seconds(attempt, initial, maximum)
    if jitter_fraction == 0:
        return nominal
    if not 0 <= random_value <= 1:
        raise ValueError("random_value must be between 0 and 1")
    multiplier = 1 + jitter_fraction * (2 * random_value - 1)
    return max(0.0, min(maximum, nominal * multiplier))


class Scheduler:
    def __init__(
        self,
        transfer_config: TransferConfig,
        executor: RsyncExecutor,
        state_store: StateStore,
        *,
        monitor: Callable[[int], None] | None = None,
        completion_probe: Callable[[ManifestEntry], bool] | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        jitter_source: Callable[[], float] | None = None,
    ):
        self.config = transfer_config
        self.executor = executor
        self.state_store = state_store
        self.monitor = monitor or (lambda active: None)
        self.completion_probe = completion_probe or (lambda entry: False)
        self.sleep = sleep
        self.clock = clock
        self.jitter_source = (
            random.Random().random if jitter_source is None else jitter_source
        )

    def run(self, entries: Sequence[ManifestEntry]) -> bool:
        work = balance_entries(entries, self.config.workers)
        if not work:
            self.monitor(0)
            return True
        due: dict[int, float] = {item.worker_id: self.clock() for item in work}
        remaining = {item.worker_id: item for item in work}
        running: dict[Future[RsyncResult], WorkItem] = {}
        retry_errors: dict[int, str] = {}
        next_start_at = self.clock()
        all_successful = True
        interrupted = False
        with ThreadPoolExecutor(max_workers=self.config.workers, thread_name_prefix="rsync-worker") as pool:
            while remaining or running:
                if getattr(getattr(self.executor, "runner", None), "stopping", False):
                    interrupted = True
                    all_successful = False
                    remaining.clear()
                now = self.clock()
                ready = [
                    (worker_id, item)
                    for worker_id, item in remaining.items()
                    if due[worker_id] <= now
                ]
                available = self.config.workers - len(running)
                if ready and available > 0 and now >= next_start_at:
                    launch_count = (
                        min(available, len(ready))
                        if self.config.worker_start_stagger_seconds == 0
                        else 1
                    )
                    for worker_id, item in ready[:launch_count]:
                        was_retry = worker_id in retry_errors
                        LOG.info(
                            "worker %d starting: files=%d bytes=%d",
                            worker_id,
                            len(item.entries),
                            item.total_bytes,
                        )
                        future = pool.submit(self.executor.transfer, item.entries)
                        running[future] = item
                        del remaining[worker_id]
                        if was_retry:
                            del retry_errors[worker_id]

                            def recovery_started(state: TransferState) -> None:
                                if state.failed:
                                    state.last_error = next(
                                        reversed(state.failed.values()), None
                                    )
                                else:
                                    state.last_error = next(
                                        reversed(retry_errors.values()), None
                                    )

                            self.state_store.mutate(recovery_started)
                    if self.config.worker_start_stagger_seconds > 0:
                        next_start_at = now + self.config.worker_start_stagger_seconds

                self.monitor(len(running))
                completed_futures = [future for future in running if future.done()]
                for future in completed_futures:
                    item = running.pop(future)
                    try:
                        result = future.result()
                    except Exception as exc:  # defensive boundary around worker threads
                        result = RsyncResult(False, 1, f"worker exception: {exc}")
                    if result.success:
                        LOG.info("worker %d completed successfully", item.worker_id)
                        ids = {entry.id for entry in item.entries}

                        def complete(state: TransferState) -> None:
                            state.completed.update(ids)
                            for entry_id in ids:
                                state.failed.pop(entry_id, None)
                            if state.failed:
                                state.last_error = next(
                                    reversed(state.failed.values()), None
                                )
                            else:
                                state.last_error = next(
                                    reversed(retry_errors.values()), None
                                )

                        self.state_store.mutate(complete)
                        continue
                    if interrupted or getattr(getattr(self.executor, "runner", None), "stopping", False):
                        LOG.info("worker %d stopped during controller shutdown", item.worker_id)
                        continue

                    LOG.warning(
                        "worker %d failed with rsync status %d: %s",
                        item.worker_id,
                        result.returncode,
                        result.error or "no diagnostic output",
                    )
                    finished = tuple(entry for entry in item.entries if self.completion_probe(entry))
                    unfinished = tuple(entry for entry in item.entries if entry not in finished)
                    if finished:
                        finished_ids = {entry.id for entry in finished}

                        def preserve_finished(state: TransferState) -> None:
                            state.completed.update(finished_ids)
                            for entry_id in finished_ids:
                                state.failed.pop(entry_id, None)

                        self.state_store.mutate(preserve_finished)
                        LOG.info(
                            "worker %d preserved %d completed file(s) from its failed batch",
                            item.worker_id,
                            len(finished),
                        )
                    if not unfinished:
                        continue
                    entry_ids = {entry.id for entry in unfinished}

                    message = result.error or f"rsync exited {result.returncode}"

                    def failed_attempt(state: TransferState) -> None:
                        for entry_id in entry_ids:
                            state.attempts[entry_id] = state.attempts.get(entry_id, 0) + 1
                            if state.attempts[entry_id] >= self.config.max_attempts:
                                state.failed[entry_id] = message
                        state.last_error = message
                        if any(
                            state.attempts[entry_id] < self.config.max_attempts
                            for entry_id in entry_ids
                        ):
                            state.last_transient_error = message

                    state = self.state_store.mutate(failed_attempt)
                    retry_entries = tuple(
                        entry
                        for entry in unfinished
                        if state.attempts.get(entry.id, 0) < self.config.max_attempts
                    )
                    exhausted = tuple(entry for entry in unfinished if entry not in retry_entries)
                    if exhausted:
                        all_successful = False
                        LOG.error(
                            "worker %d exhausted retries for %d file(s)",
                            item.worker_id,
                            len(exhausted),
                        )
                    if retry_entries:
                        retry_errors[item.worker_id] = message
                        attempt = max(state.attempts[entry.id] for entry in retry_entries)
                        random_value = (
                            0.5
                            if self.config.retry_jitter_fraction == 0
                            else self.jitter_source()
                        )
                        delay = retry_delay_seconds(
                            attempt,
                            self.config.backoff_initial_seconds,
                            self.config.backoff_max_seconds,
                            self.config.retry_jitter_fraction,
                            random_value,
                        )

                        def record_retry(current: TransferState) -> None:
                            current.retry_count += 1

                        self.state_store.mutate(record_retry)
                        retry_item = WorkItem(item.worker_id, retry_entries)
                        remaining[item.worker_id] = retry_item
                        due[item.worker_id] = self.clock() + delay
                        LOG.warning(
                            "worker %d retry scheduled in %.1f seconds (attempt %d/%d)",
                            item.worker_id,
                            delay,
                            attempt + 1,
                            self.config.max_attempts,
                        )
                if remaining or running:
                    sleep_for = min(
                        self.config.poll_interval_seconds,
                        0.25 if completed_futures else self.config.poll_interval_seconds,
                    )
                    sleep_now = self.clock()
                    if remaining:
                        next_due = min(due[worker_id] for worker_id in remaining)
                        if next_due > sleep_now:
                            sleep_for = min(sleep_for, next_due - sleep_now)
                        due_waiting = any(
                            due[worker_id] <= sleep_now for worker_id in remaining
                        )
                        if (
                            due_waiting
                            and len(running) < self.config.workers
                            and next_start_at > sleep_now
                        ):
                            sleep_for = min(sleep_for, next_start_at - sleep_now)
                    self.sleep(sleep_for)
        self.monitor(0)
        return all_successful
