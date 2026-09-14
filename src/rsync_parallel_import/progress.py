from __future__ import annotations

import os
import stat
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path

from .manifest import Manifest, ManifestEntry


@dataclass(frozen=True)
class ProgressSnapshot:
    transferred_bytes: int
    total_bytes: int
    percentage: float
    rate_bytes_per_second: float
    eta_seconds: float | None


class RollingRate:
    """Byte rate using the oldest/newest samples in a bounded time window."""

    def __init__(self, window_seconds: float = 60.0):
        self.window_seconds = window_seconds
        self._samples: deque[tuple[float, int]] = deque()

    def add(self, byte_count: int, now: float | None = None) -> float:
        now = time.monotonic() if now is None else now
        if self._samples and byte_count < self._samples[-1][1]:
            self._samples.clear()
        self._samples.append((now, byte_count))
        cutoff = now - self.window_seconds
        while len(self._samples) > 2 and self._samples[1][0] <= cutoff:
            self._samples.popleft()
        if len(self._samples) < 2:
            return 0.0
        elapsed = self._samples[-1][0] - self._samples[0][0]
        if elapsed <= 0:
            return 0.0
        return max(0.0, (self._samples[-1][1] - self._samples[0][1]) / elapsed)


def _file_info(path: bytes) -> os.stat_result | None:
    try:
        info = os.stat(path, follow_symlinks=False)
    except (FileNotFoundError, NotADirectoryError, PermissionError):
        return None
    if not stat.S_ISREG(info.st_mode):
        return None
    return info


def entry_is_complete(entry: ManifestEntry, destination: Path) -> bool:
    info = _file_info(os.path.join(os.fsencode(destination), entry.path))
    return bool(
        info is not None
        and info.st_size == entry.size
        and info.st_mtime_ns == entry.mtime_ns
    )


def _active_temp_size(target: bytes) -> int | None:
    parent, name = os.path.split(target)
    prefix = b"." + name + b"."
    largest: int | None = None
    try:
        with os.scandir(parent) as items:
            for item in items:
                candidate = os.fsencode(item.name)
                # rsync's receiver-side temporary name has a six-character
                # mkstemp suffix. Matching this shape avoids counting ordinary
                # dot files with a similar prefix.
                if not candidate.startswith(prefix) or len(candidate) != len(prefix) + 6:
                    continue
                try:
                    info = item.stat(follow_symlinks=False)
                except OSError:
                    continue
                if stat.S_ISREG(info.st_mode):
                    largest = info.st_size if largest is None else max(largest, info.st_size)
    except (FileNotFoundError, NotADirectoryError, PermissionError):
        return None
    return largest


def transferred_bytes(manifest: Manifest, destination: Path, partial_dir_name: str) -> int:
    root = os.fsencode(destination)
    partial_component = os.fsencode(partial_dir_name)
    total = 0
    for entry in manifest.entries:
        target = os.path.join(root, entry.path)
        target_info = _file_info(target)
        candidates: list[int] = []
        if target_info is not None:
            if target_info.st_size == entry.size and target_info.st_mtime_ns == entry.mtime_ns:
                candidates.append(entry.size)
            elif target_info.st_size < entry.size:
                candidates.append(target_info.st_size)
        parent, name = os.path.split(target)
        partial = os.path.join(parent, partial_component, name)
        partial_info = _file_info(partial)
        if partial_info is not None:
            candidates.append(partial_info.st_size)
        active_size = _active_temp_size(target)
        if active_size is not None:
            candidates.append(active_size)
        if candidates:
            total += min(max(candidates), entry.size)
    return total


def make_snapshot(transferred: int, total: int, rate: float) -> ProgressSnapshot:
    percentage = 100.0 if total == 0 else min(100.0, transferred * 100.0 / total)
    remaining = max(0, total - transferred)
    eta = (remaining / rate) if rate > 0 and remaining else (0.0 if not remaining else None)
    return ProgressSnapshot(transferred, total, percentage, rate, eta)


def newly_crossed_thresholds(
    transferred: int, total: int, emitted: set[int]
) -> list[int]:
    percentage = 100.0 if total == 0 else transferred * 100.0 / total
    return [value for value in range(10, 101, 10) if value <= percentage and value not in emitted]
