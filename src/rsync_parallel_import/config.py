from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import ConfigurationError


@dataclass(frozen=True)
class SourceConfig:
    host: str
    user: str
    path: str
    ssh_options: tuple[str, ...] = ()

    @property
    def target(self) -> str:
        return f"{self.user}@{self.host}"


@dataclass(frozen=True)
class TransferConfig:
    workers: int = 16
    max_attempts: int = 5
    backoff_initial_seconds: float = 5.0
    backoff_max_seconds: float = 300.0
    poll_interval_seconds: float = 2.0
    rate_window_seconds: float = 60.0
    ssh_check_timeout_seconds: float = 15.0
    partial_dir_name: str = ".rsync-parallel-import-partial"


@dataclass(frozen=True)
class Config:
    source: SourceConfig
    destination: Path
    state_dir: Path
    transfer: TransferConfig


_TOP_KEYS = {"source", "destination", "state", "transfer"}
_SOURCE_KEYS = {"host", "user", "path", "ssh_options"}
_DEST_KEYS = {"path"}
_STATE_KEYS = {"path"}
_TRANSFER_KEYS = {
    "workers",
    "max_attempts",
    "backoff_initial_seconds",
    "backoff_max_seconds",
    "poll_interval_seconds",
    "rate_window_seconds",
    "ssh_check_timeout_seconds",
    "partial_dir_name",
}


def _table(data: dict[str, Any], name: str, required: bool = True) -> dict[str, Any]:
    value = data.get(name)
    if value is None and not required:
        return {}
    if not isinstance(value, dict):
        raise ConfigurationError(f"[{name}] must be a TOML table")
    return value


def _unknown(table: dict[str, Any], allowed: set[str], label: str) -> None:
    extra = sorted(set(table) - allowed)
    if extra:
        raise ConfigurationError(f"unknown key(s) in [{label}]: {', '.join(extra)}")


def _nonempty_string(table: dict[str, Any], key: str, label: str) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value or "\x00" in value or "\n" in value:
        raise ConfigurationError(f"[{label}].{key} must be a non-empty single-line string")
    return value


def _number(table: dict[str, Any], key: str, default: float) -> float:
    value = table.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ConfigurationError(f"[transfer].{key} must be a positive number")
    return float(value)


def load_config(path: str | os.PathLike[str]) -> Config:
    config_path = Path(path)
    try:
        with config_path.open("rb") as handle:
            data = tomllib.load(handle)
    except FileNotFoundError as exc:
        raise ConfigurationError(f"configuration file does not exist: {config_path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigurationError(f"invalid TOML in {config_path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigurationError("configuration root must be a TOML table")
    _unknown(data, _TOP_KEYS, "root")
    source = _table(data, "source")
    destination = _table(data, "destination")
    state = _table(data, "state", required=False)
    transfer = _table(data, "transfer")
    _unknown(source, _SOURCE_KEYS, "source")
    _unknown(destination, _DEST_KEYS, "destination")
    _unknown(state, _STATE_KEYS, "state")
    _unknown(transfer, _TRANSFER_KEYS, "transfer")

    host = _nonempty_string(source, "host", "source")
    user = _nonempty_string(source, "user", "source")
    if host.startswith("-") or user.startswith("-") or any(char.isspace() for char in host + user):
        raise ConfigurationError("[source].host and [source].user cannot contain whitespace or begin with '-'")
    source_path = _nonempty_string(source, "path", "source")
    if not source_path.startswith("/"):
        raise ConfigurationError("[source].path must be absolute")
    ssh_options_value = source.get("ssh_options", [])
    if not isinstance(ssh_options_value, list) or any(
        not isinstance(item, str) or not item or "\x00" in item or "\n" in item
        for item in ssh_options_value
    ):
        raise ConfigurationError("[source].ssh_options must be an array of non-empty strings")

    destination_path = Path(_nonempty_string(destination, "path", "destination"))
    if not destination_path.is_absolute():
        raise ConfigurationError("[destination].path must be absolute")
    state_value = state.get("path", "/var/lib/rsync-parallel-import")
    if not isinstance(state_value, str) or not state_value or "\x00" in state_value:
        raise ConfigurationError("[state].path must be a non-empty string")
    state_path = Path(state_value)
    if not state_path.is_absolute():
        raise ConfigurationError("[state].path must be absolute")
    destination_resolved = destination_path.resolve(strict=False)
    state_resolved = state_path.resolve(strict=False)
    if destination_resolved == state_resolved or destination_resolved in state_resolved.parents or state_resolved in destination_resolved.parents:
        raise ConfigurationError("[destination].path and [state].path must not overlap")

    workers = transfer.get("workers", 16)
    max_attempts = transfer.get("max_attempts", 5)
    if isinstance(workers, bool) or not isinstance(workers, int) or not 1 <= workers <= 256:
        raise ConfigurationError("[transfer].workers must be an integer from 1 to 256")
    if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or not 1 <= max_attempts <= 100:
        raise ConfigurationError("[transfer].max_attempts must be an integer from 1 to 100")
    partial = transfer.get("partial_dir_name", ".rsync-parallel-import-partial")
    if (
        not isinstance(partial, str)
        or not partial
        or partial in {".", ".."}
        or "/" in partial
        or "\x00" in partial
    ):
        raise ConfigurationError("[transfer].partial_dir_name must be one safe path component")

    initial = _number(transfer, "backoff_initial_seconds", 5.0)
    maximum = _number(transfer, "backoff_max_seconds", 300.0)
    if maximum < initial:
        raise ConfigurationError("backoff_max_seconds must be >= backoff_initial_seconds")
    return Config(
        source=SourceConfig(host, user, source_path, tuple(ssh_options_value)),
        destination=destination_path,
        state_dir=state_path,
        transfer=TransferConfig(
            workers=workers,
            max_attempts=max_attempts,
            backoff_initial_seconds=initial,
            backoff_max_seconds=maximum,
            poll_interval_seconds=_number(transfer, "poll_interval_seconds", 2.0),
            rate_window_seconds=_number(transfer, "rate_window_seconds", 60.0),
            ssh_check_timeout_seconds=_number(transfer, "ssh_check_timeout_seconds", 15.0),
            partial_dir_name=partial,
        ),
    )
