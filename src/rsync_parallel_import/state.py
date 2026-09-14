from __future__ import annotations

import copy
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .errors import StateError
from .util import atomic_write_json, load_json

STATE_VERSION = 2
LEGACY_STATE_VERSION = 1
PHASES = {"initialized", "transfer", "reconciliation", "completed", "failed"}


@dataclass
class TransferState:
    manifest_digest: str
    phase: str = "initialized"
    completed: set[str] = field(default_factory=set)
    failed: dict[str, str] = field(default_factory=dict)
    attempts: dict[str, int] = field(default_factory=dict)
    retry_count: int = 0
    thresholds_emitted: set[int] = field(default_factory=set)
    transferred_bytes: int = 0
    total_bytes: int = 0
    rate_bytes_per_second: float = 0.0
    eta_seconds: float | None = None
    active_workers: int = 0
    total_workers: int = 0
    last_error: str | None = None
    last_transient_error: str | None = None
    version: int = STATE_VERSION

    def validate(self) -> None:
        if self.version != STATE_VERSION:
            raise StateError(f"unsupported state version: {self.version!r}")
        if self.phase not in PHASES:
            raise StateError(f"invalid state phase: {self.phase!r}")
        if not self.manifest_digest:
            raise StateError("state has no manifest digest")
        if any(value < 0 for value in self.attempts.values()):
            raise StateError("state contains a negative attempt count")
        if any(not isinstance(value, str) for value in self.completed):
            raise StateError("state contains a non-string completed path id")
        if any(not isinstance(key, str) or not isinstance(value, str) for key, value in self.failed.items()):
            raise StateError("state contains an invalid failed-work record")
        if any(not isinstance(key, str) or isinstance(value, bool) or not isinstance(value, int) for key, value in self.attempts.items()):
            raise StateError("state contains an invalid attempt record")
        if not self.thresholds_emitted <= set(range(10, 101, 10)):
            raise StateError("state contains an invalid progress threshold")
        if min(self.transferred_bytes, self.total_bytes, self.retry_count) < 0:
            raise StateError("state contains a negative counter")
        if self.last_error is not None and not isinstance(self.last_error, str):
            raise StateError("state contains an invalid current error")
        if self.last_transient_error is not None and not isinstance(
            self.last_transient_error, str
        ):
            raise StateError("state contains an invalid transient error")

    def record(self) -> dict[str, Any]:
        self.validate()
        return {
            "version": self.version,
            "manifest_digest": self.manifest_digest,
            "phase": self.phase,
            "completed": sorted(self.completed),
            "failed": dict(sorted(self.failed.items())),
            "attempts": dict(sorted(self.attempts.items())),
            "retry_count": self.retry_count,
            "thresholds_emitted": sorted(self.thresholds_emitted),
            "progress": {
                "transferred_bytes": self.transferred_bytes,
                "total_bytes": self.total_bytes,
                "rate_bytes_per_second": self.rate_bytes_per_second,
                "eta_seconds": self.eta_seconds,
                "active_workers": self.active_workers,
                "total_workers": self.total_workers,
            },
            "last_error": self.last_error,
            "last_transient_error": self.last_transient_error,
        }

    @classmethod
    def from_record(cls, data: Any) -> "TransferState":
        try:
            version = int(data["version"])
            if version not in {LEGACY_STATE_VERSION, STATE_VERSION}:
                raise StateError(f"unsupported state version: {version!r}")
            progress = data.get("progress", {})
            failed = dict(data.get("failed", {}))
            phase = str(data["phase"])
            legacy_error = data.get("last_error")
            if version == LEGACY_STATE_VERSION:
                # Version 1 used last_error for both active failures and old,
                # retryable worker failures. Preserve terminal errors as current
                # and migrate nonterminal ones into explicit retry history.
                last_error = legacy_error if failed or phase == "failed" else None
                last_transient_error = (
                    None if failed or phase == "failed" else legacy_error
                )
            else:
                last_error = legacy_error
                last_transient_error = data.get("last_transient_error")
            state = cls(
                version=STATE_VERSION,
                manifest_digest=str(data["manifest_digest"]),
                phase=phase,
                completed=set(data.get("completed", [])),
                failed=failed,
                attempts={key: int(value) for key, value in data.get("attempts", {}).items()},
                retry_count=int(data.get("retry_count", 0)),
                thresholds_emitted={int(value) for value in data.get("thresholds_emitted", [])},
                transferred_bytes=int(progress.get("transferred_bytes", 0)),
                total_bytes=int(progress.get("total_bytes", 0)),
                rate_bytes_per_second=float(progress.get("rate_bytes_per_second", 0.0)),
                eta_seconds=(
                    None if progress.get("eta_seconds") is None else float(progress["eta_seconds"])
                ),
                active_workers=int(progress.get("active_workers", 0)),
                total_workers=int(progress.get("total_workers", 0)),
                last_error=last_error,
                last_transient_error=last_transient_error,
            )
            state.validate()
            return state
        except StateError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise StateError(f"invalid transfer state: {exc}") from exc


class StateStore:
    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.RLock()

    def exists(self) -> bool:
        return self.path.exists()

    def load(self) -> TransferState:
        with self._lock:
            try:
                return TransferState.from_record(load_json(self.path))
            except FileNotFoundError as exc:
                raise StateError(f"state file does not exist: {self.path}") from exc

    def save(self, state: TransferState) -> None:
        with self._lock:
            atomic_write_json(self.path, state.record())

    def mutate(self, change: Callable[[TransferState], None]) -> TransferState:
        with self._lock:
            state = self.load()
            change(state)
            self.save(state)
            return copy.deepcopy(state)


def initialize_state(manifest_digest: str, total_bytes: int) -> TransferState:
    return TransferState(manifest_digest=manifest_digest, total_bytes=total_bytes)
