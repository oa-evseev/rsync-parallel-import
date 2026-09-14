from __future__ import annotations

import os
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Sequence

from .config import Config
from .manifest import ManifestEntry
from .process import safe_stderr


class Runner(Protocol):
    @property
    def stopping(self) -> bool: ...

    def run(self, args: Sequence[str], *, input: bytes | None = None, timeout: float | None = None): ...


@dataclass(frozen=True)
class RsyncResult:
    success: bool
    returncode: int
    error: str = ""


def ssh_transport(config: Config) -> str:
    # rsync parses -e itself. shlex.join preserves each administrator-supplied
    # SSH argv token without invoking a local shell.
    return shlex.join(["ssh", *config.source.ssh_options])


def remote_source(config: Config) -> str:
    return f"{config.source.target}:{config.source.path.rstrip('/')}/"


def build_transfer_command(config: Config) -> list[str]:
    return [
        "rsync",
        "-a",
        "--partial",
        f"--partial-dir={config.transfer.partial_dir_name}",
        "--protect-args",
        "--from0",
        "--files-from=-",
        "--no-compress",
        "-e",
        ssh_transport(config),
        "--",
        remote_source(config),
        os.fspath(config.destination) + "/",
    ]


def build_reconcile_command(config: Config) -> list[str]:
    return [
        "rsync",
        "-aH",
        "--partial",
        f"--partial-dir={config.transfer.partial_dir_name}",
        "--protect-args",
        "--no-compress",
        "-e",
        ssh_transport(config),
        "--",
        remote_source(config),
        os.fspath(config.destination) + "/",
    ]


def file_list(entries: Sequence[ManifestEntry]) -> bytes:
    return b"".join(entry.path + b"\x00" for entry in entries)


class RsyncExecutor:
    def __init__(self, config: Config, runner: Runner):
        self.config = config
        self.runner = runner

    def transfer(self, entries: Sequence[ManifestEntry]) -> RsyncResult:
        if not entries:
            return RsyncResult(True, 0)
        result = self.runner.run(build_transfer_command(self.config), input=file_list(entries))
        return RsyncResult(
            result.returncode == 0,
            result.returncode,
            safe_stderr(result),
        )

    def reconcile(self) -> RsyncResult:
        result = self.runner.run(build_reconcile_command(self.config))
        return RsyncResult(
            result.returncode == 0,
            result.returncode,
            safe_stderr(result),
        )

