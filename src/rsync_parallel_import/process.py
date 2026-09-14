from __future__ import annotations

import subprocess
import threading
from collections.abc import Sequence


class SubprocessRunner:
    """Run argv-only subprocesses and retain handles for graceful cancellation."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._processes: set[subprocess.Popen[bytes]] = set()
        self._stopping = threading.Event()

    @property
    def stopping(self) -> bool:
        return self._stopping.is_set()

    def run(
        self,
        args: Sequence[str],
        *,
        input: bytes | None = None,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        if self.stopping:
            return subprocess.CompletedProcess(list(args), 143, b"", b"controller is stopping")
        process = subprocess.Popen(
            list(args),
            stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
        )
        with self._lock:
            self._processes.add(process)
        try:
            stdout, stderr = process.communicate(input=input, timeout=timeout)
            return subprocess.CompletedProcess(list(args), process.returncode, stdout, stderr)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                stdout, stderr = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
            return subprocess.CompletedProcess(list(args), 124, stdout, stderr + b"\ncommand timed out")
        finally:
            with self._lock:
                self._processes.discard(process)

    def terminate_all(self) -> None:
        self._stopping.set()
        with self._lock:
            processes = list(self._processes)
        for process in processes:
            if process.poll() is None:
                process.terminate()


def safe_stderr(result: subprocess.CompletedProcess[bytes], limit: int = 2000) -> str:
    data = result.stderr[-limit:]
    return data.decode("utf-8", "backslashreplace").strip()

