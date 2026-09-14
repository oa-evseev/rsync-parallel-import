from __future__ import annotations

import logging
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
    ):
        self.config = transfer_config
        self.executor = executor
        self.state_store = state_store
        self.monitor = monitor or (lambda active: None)
        self.completion_probe = completion_probe or (lambda entry: False)
        self.sleep = sleep
        self.clock = clock

    def run(self, entries: Sequence[ManifestEntry]) -> bool:
        work = balance_entries(entries, self.config.workers)
        if not work:
            self.monitor(0)
            return True
        due: dict[int, float] = {item.worker_id: self.clock() for item in work}
        remaining = {item.worker_id: item for item in work}
        running: dict[Future[RsyncResult], WorkItem] = {}
        all_successful = True
        interrupted = False
        with ThreadPoolExecutor(max_workers=self.config.workers, thread_name_prefix="rsync-worker") as pool:
            while remaining or running:
                if getattr(getattr(self.executor, "runner", None), "stopping", False):
                    interrupted = True
                    all_successful = False
                    remaining.clear()
                now = self.clock()
                for worker_id, item in list(remaining.items()):
                    if due[worker_id] <= now and len(running) < self.config.workers:
                        LOG.info(
                            "worker %d starting: files=%d bytes=%d",
                            worker_id,
                            len(item.entries),
                            item.total_bytes,
                        )
                        future = pool.submit(self.executor.transfer, item.entries)
                        running[future] = item
                        del remaining[worker_id]

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
                            state.last_error = None

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
                        attempt = max(state.attempts[entry.id] for entry in retry_entries)
                        delay = backoff_seconds(
                            attempt,
                            self.config.backoff_initial_seconds,
                            self.config.backoff_max_seconds,
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
                    self.sleep(min(self.config.poll_interval_seconds, 0.25 if completed_futures else self.config.poll_interval_seconds))
        self.monitor(0)
        return all_successful
