from __future__ import annotations

import subprocess
from pathlib import Path

from rsync_parallel_import.config import Config, SourceConfig, TransferConfig


def make_config(root: Path, **transfer_overrides) -> Config:
    transfer = TransferConfig(**transfer_overrides)
    return Config(
        source=SourceConfig(
            "source.example.net",
            "importer",
            "/srv/source",
            ("-o", "BatchMode=yes"),
        ),
        destination=root / "destination",
        state_dir=root / "state",
        transfer=transfer,
    )


class QueueRunner:
    def __init__(self, results=None):
        self.results = list(results or [])
        self.calls = []
        self.stopping = False

    def run(self, args, *, input=None, timeout=None):
        self.calls.append((list(args), input, timeout))
        if self.results:
            return self.results.pop(0)
        return subprocess.CompletedProcess(list(args), 0, b"", b"")

    def terminate_all(self):
        self.stopping = True

