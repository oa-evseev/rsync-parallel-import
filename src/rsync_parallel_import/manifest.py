from __future__ import annotations

import base64
import hashlib
import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

from .config import SourceConfig
from .errors import ManifestError, SourceChangedError
from .process import safe_stderr
from .util import atomic_write_json, canonical_json_bytes, decode_path, display_path, encode_path, load_json

LOG = logging.getLogger(__name__)
MANIFEST_VERSION = 1

REMOTE_SCAN_SCRIPT = b"""set -eu
source_path=$(printf '%s' "$1" | base64 -d)
cd -- "$source_path"
LC_ALL=C find . -type f -printf '%P\\0%s\\0%T@\\0'
"""


class Runner(Protocol):
    def run(self, args: list[str], *, input: bytes | None = None, timeout: float | None = None): ...


@dataclass(frozen=True, order=True)
class ManifestEntry:
    path: bytes
    size: int
    mtime_ns: int

    def __post_init__(self) -> None:
        validate_relative_path(self.path)
        if self.size < 0:
            raise ManifestError("manifest sizes cannot be negative")

    @property
    def id(self) -> str:
        return encode_path(self.path)

    def record(self) -> dict[str, Any]:
        return {"path_b64": self.id, "size": self.size, "mtime_ns": self.mtime_ns}


@dataclass(frozen=True)
class Manifest:
    source_host: str
    source_user: str
    source_path: str
    entries: tuple[ManifestEntry, ...]
    version: int = MANIFEST_VERSION

    @property
    def total_bytes(self) -> int:
        return sum(entry.size for entry in self.entries)

    @property
    def digest(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.core_record())).hexdigest()

    def core_record(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "source": {
                "host": self.source_host,
                "user": self.source_user,
                "path": self.source_path,
            },
            "entries": [entry.record() for entry in self.entries],
        }

    def record(self) -> dict[str, Any]:
        value = self.core_record()
        value["digest"] = self.digest
        value["total_bytes"] = self.total_bytes
        value["file_count"] = len(self.entries)
        return value


def validate_relative_path(path: bytes) -> None:
    if not path or path.startswith(b"/") or b"\x00" in path:
        raise ManifestError("manifest path must be non-empty, relative, and NUL-free")
    parts = path.split(b"/")
    if any(part in {b"", b".", b".."} for part in parts):
        raise ManifestError(f"unsafe relative manifest path: {display_path(path)}")


def save_manifest(path: Path, manifest: Manifest) -> None:
    atomic_write_json(path, manifest.record())


def load_manifest(path: Path) -> Manifest:
    try:
        data = load_json(path)
        if data.get("version") != MANIFEST_VERSION:
            raise ManifestError(f"unsupported manifest version: {data.get('version')!r}")
        source = data["source"]
        entries = tuple(
            sorted(
                (
                    ManifestEntry(
                        path=decode_path(record["path_b64"]),
                        size=int(record["size"]),
                        mtime_ns=int(record["mtime_ns"]),
                    )
                    for record in data["entries"]
                ),
                key=lambda entry: entry.path,
            )
        )
        if len({entry.path for entry in entries}) != len(entries):
            raise ManifestError("manifest contains duplicate paths")
        manifest = Manifest(source["host"], source["user"], source["path"], entries)
        if data.get("digest") != manifest.digest:
            raise ManifestError("manifest digest mismatch; state may be corrupt or modified")
        if data.get("total_bytes") != manifest.total_bytes or data.get("file_count") != len(entries):
            raise ManifestError("manifest summary does not match its entries")
        return manifest
    except ManifestError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise ManifestError(f"invalid manifest {path}: {exc}") from exc


def parse_scan_output(data: bytes) -> tuple[ManifestEntry, ...]:
    fields = data.split(b"\x00")
    if fields and fields[-1] == b"":
        fields.pop()
    if len(fields) % 3:
        raise ManifestError("remote manifest scan returned a truncated NUL-delimited record")
    entries: list[ManifestEntry] = []
    for index in range(0, len(fields), 3):
        path, size_raw, mtime_raw = fields[index : index + 3]
        try:
            size = int(size_raw)
            mtime_ns = int(Decimal(mtime_raw.decode("ascii")) * Decimal(1_000_000_000))
        except (ValueError, UnicodeDecodeError, InvalidOperation) as exc:
            raise ManifestError("remote manifest scan returned invalid size or mtime data") from exc
        entries.append(ManifestEntry(path, size, mtime_ns))
    entries.sort(key=lambda entry: entry.path)
    if len({entry.path for entry in entries}) != len(entries):
        raise ManifestError("remote manifest scan returned duplicate paths")
    return tuple(entries)


class RemoteManifestScanner:
    def __init__(self, source: SourceConfig, runner: Runner):
        self.source = source
        self.runner = runner

    def scan(self) -> Manifest:
        encoded_path = base64.b64encode(self.source.path.encode("utf-8")).decode("ascii")
        command = [
            "ssh",
            *self.source.ssh_options,
            self.source.target,
            "sh",
            "-s",
            "--",
            encoded_path,
        ]
        LOG.info("scanning remote source manifest")
        result = self.runner.run(command, input=REMOTE_SCAN_SCRIPT)
        if result.returncode:
            detail = safe_stderr(result) or f"exit status {result.returncode}"
            raise ManifestError(
                "remote manifest scan failed; the source requires POSIX sh, base64, and GNU find: "
                + detail
            )
        return Manifest(
            self.source.host,
            self.source.user,
            self.source.path,
            parse_scan_output(result.stdout),
        )


def assert_same_source(expected: Manifest, actual: Manifest, *, max_examples: int = 10) -> None:
    if (
        expected.source_host,
        expected.source_user,
        expected.source_path,
    ) != (
        actual.source_host,
        actual.source_user,
        actual.source_path,
    ):
        raise SourceChangedError("configured/scanned source identity differs from the persistent manifest")
    expected_map = {entry.path: entry for entry in expected.entries}
    actual_map = {entry.path: entry for entry in actual.entries}
    missing = sorted(expected_map.keys() - actual_map.keys())
    added = sorted(actual_map.keys() - expected_map.keys())
    changed = sorted(
        path
        for path in expected_map.keys() & actual_map.keys()
        if (
            expected_map[path].size != actual_map[path].size
            or expected_map[path].mtime_ns != actual_map[path].mtime_ns
        )
    )
    if not (missing or added or changed):
        return
    pieces = []
    for label, paths in (("missing", missing), ("added", added), ("changed", changed)):
        if paths:
            examples = ", ".join(display_path(path) for path in paths[:max_examples])
            suffix = " ..." if len(paths) > max_examples else ""
            pieces.append(f"{label}={len(paths)} ({examples}{suffix})")
    raise SourceChangedError(
        "source no longer matches the persistent import manifest: " + "; ".join(pieces)
    )
