from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import signal
import sys
from pathlib import Path
from typing import Any

from .config import Config, load_config
from .controller import Controller
from .errors import ImporterError
from .locking import InstanceLock, lock_is_held
from .manifest import load_manifest
from .state import StateStore
from .util import decode_path, display_path, format_bytes, format_duration

DEFAULT_CONFIG = "/etc/rsync-parallel-import.toml"
LOG = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rsync-parallel-import",
        description="Persistent byte-balanced parallel imports using rsync over SSH",
    )
    parser.add_argument("--config", default=DEFAULT_CONFIG, help=f"TOML configuration (default: {DEFAULT_CONFIG})")
    parser.add_argument("--verbose", action="store_true", help="enable debug logging")
    subparsers = parser.add_subparsers(dest="operation", required=True)
    run = subparsers.add_parser("run", help="create or safely resume the import")
    run.add_argument(
        "--retry-failed",
        action="store_true",
        help="reopen terminal failed work after its cause has been corrected",
    )
    status = subparsers.add_parser("status", help="show persistent transfer status")
    status.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    subparsers.add_parser("verify", help="read-only comparison with the manifest/source")
    reset = subparsers.add_parser("reset", help="discard persistent import state (not destination data)")
    reset.add_argument(
        "--yes",
        action="store_true",
        help="confirm permanent removal of the current manifest and transfer state",
    )
    return parser


def configure_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def status_record(config: Config) -> dict[str, Any]:
    manifest_path = config.state_dir / "manifest.json"
    state_path = config.state_dir / "state.json"
    if not manifest_path.exists() and not state_path.exists():
        return {
            "phase": "uninitialized",
            "total_bytes": 0,
            "transferred_bytes": 0,
            "percentage": 0.0,
            "rate_bytes_per_second": 0.0,
            "eta_seconds": None,
            "active_workers": 0,
            "total_workers": config.transfer.workers,
            "retry_count": 0,
            "failed_count": 0,
            "failed_items": [],
            "last_error": None,
        }
    if not manifest_path.exists() or not state_path.exists():
        raise ImporterError(
            "state directory is only partially initialized; run will recover a missing initial state, "
            "but a missing manifest requires restoration or reset --yes"
        )
    manifest = load_manifest(manifest_path)
    state = StateStore(state_path).load()
    if state.manifest_digest != manifest.digest:
        raise ImporterError("state refers to a different manifest")
    percentage = (
        100.0
        if state.total_bytes == 0 and state.phase == "completed"
        else (state.transferred_bytes * 100.0 / state.total_bytes if state.total_bytes else 0.0)
    )
    failed_items = [
        {"path": display_path(decode_path(entry_id)), "error": error}
        for entry_id, error in sorted(state.failed.items())
    ]
    controller_active = lock_is_held(config.state_dir / "controller.lock")
    rate = state.rate_bytes_per_second if controller_active or state.phase == "completed" else 0.0
    eta = state.eta_seconds if controller_active or state.phase == "completed" else None
    return {
        "phase": state.phase,
        "manifest_digest": manifest.digest,
        "file_count": len(manifest.entries),
        "completed_files": len(state.completed),
        "total_bytes": state.total_bytes,
        "transferred_bytes": state.transferred_bytes,
        "percentage": min(100.0, percentage),
        "rate_bytes_per_second": rate,
        "eta_seconds": eta,
        "active_workers": state.active_workers if controller_active else 0,
        "total_workers": state.total_workers or config.transfer.workers,
        "retry_count": state.retry_count,
        "failed_count": len(state.failed),
        "failed_items": failed_items,
        "thresholds_emitted": sorted(state.thresholds_emitted),
        "last_error": state.last_error,
    }


def human_status(record: dict[str, Any]) -> str:
    line = (
        f"phase={record['phase']} "
        f"progress={record['percentage']:.1f}% "
        f"({format_bytes(record['transferred_bytes'])}/{format_bytes(record['total_bytes'])}) "
        f"rate={format_bytes(record['rate_bytes_per_second'])}/s "
        f"eta={format_duration(record['eta_seconds'])} "
        f"workers={record['active_workers']}/{record['total_workers']} "
        f"retries={record['retry_count']} failures={record['failed_count']}"
    )
    details = []
    if record.get("last_error"):
        details.append(f"last error: {record['last_error']}")
    for item in record.get("failed_items", [])[:10]:
        details.append(f"failed: {item['path']}: {item['error']}")
    if len(record.get("failed_items", [])) > 10:
        details.append(f"... {len(record['failed_items']) - 10} more failed item(s)")
    return "\n".join([line, *details])


def reset_state(config: Config, confirmed: bool) -> None:
    if not confirmed:
        raise ImporterError(
            "reset refused: pass --yes to discard the manifest and transfer state; "
            "destination files are never removed"
        )
    config.state_dir.mkdir(parents=True, exist_ok=True)
    lock_path = config.state_dir / "controller.lock"
    with InstanceLock(lock_path):
        for child in list(config.state_dir.iterdir()):
            if child == lock_path:
                continue
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()
    LOG.warning("persistent import state reset; destination was not modified")


def _install_signal_handlers(controller: Controller) -> dict[int, Any]:
    previous = {}

    def stop(signum, frame) -> None:  # type: ignore[no-untyped-def]
        controller.stop()

    for signum in (signal.SIGTERM, signal.SIGINT):
        previous[signum] = signal.signal(signum, stop)
    return previous


def _restore_signal_handlers(previous: dict[int, Any]) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.verbose)
    try:
        config = load_config(args.config)
        if args.operation == "status":
            record = status_record(config)
            print(json.dumps(record, sort_keys=True, ensure_ascii=True) if args.json else human_status(record))
            return 0
        if args.operation == "reset":
            reset_state(config, args.yes)
            return 0
        controller = Controller(config)
        previous = _install_signal_handlers(controller)
        try:
            if args.operation == "verify":
                issues = controller.verify()
                if issues:
                    print(f"verification failed: {len(issues)} issue(s)", file=sys.stderr)
                    for issue in issues[:50]:
                        print(issue, file=sys.stderr)
                    return 1
                print("verification successful: source snapshot and destination match the manifest")
                return 0
            if args.operation == "run":
                return 0 if controller.run(retry_failed=args.retry_failed) else 1
        finally:
            _restore_signal_handlers(previous)
    except (ImporterError, OSError) as exc:
        LOG.error("%s", exc)
        return 2
    parser.error("unknown operation")
    return 2
