from __future__ import annotations

import logging
import os
import shutil
import stat
from pathlib import Path

from .config import Config
from .errors import ManifestError, PrerequisiteError, SourceChangedError, StateError, TransferError
from .locking import InstanceLock
from .manifest import (
    Manifest,
    RemoteManifestScanner,
    assert_same_source,
    load_manifest,
    save_manifest,
)
from .process import SubprocessRunner, safe_stderr
from .progress import (
    RollingRate,
    entry_is_complete,
    make_snapshot,
    newly_crossed_thresholds,
    transferred_bytes,
)
from .rsync import RsyncExecutor
from .scheduler import Scheduler
from .state import StateStore, TransferState, initialize_state
from .util import display_path

LOG = logging.getLogger(__name__)


def verify_prerequisites(config: Config, runner: SubprocessRunner) -> None:
    missing = [name for name in ("ssh", "rsync") if shutil.which(name) is None]
    if missing:
        raise PrerequisiteError("required executable(s) not found in PATH: " + ", ".join(missing))
    version = runner.run(["rsync", "--version"], timeout=10)
    if version.returncode:
        raise PrerequisiteError("local rsync is not runnable: " + safe_stderr(version))
    command = ["ssh", *config.source.ssh_options, config.source.target, "true"]
    result = runner.run(command, timeout=config.transfer.ssh_check_timeout_seconds)
    if result.returncode:
        detail = safe_stderr(result) or f"exit status {result.returncode}"
        raise PrerequisiteError(
            f"non-interactive SSH prerequisite failed for {config.source.target}: {detail}; "
            "verify host keys, authentication, and BatchMode-style non-interactive access"
        )
    remote_rsync = runner.run(
        ["ssh", *config.source.ssh_options, config.source.target, "rsync", "--version"],
        timeout=config.transfer.ssh_check_timeout_seconds,
    )
    if remote_rsync.returncode:
        detail = safe_stderr(remote_rsync) or f"exit status {remote_rsync.returncode}"
        raise PrerequisiteError(f"remote rsync is not runnable for {config.source.target}: {detail}")


def verify_destination(manifest: Manifest, destination: Path) -> list[str]:
    issues: list[str] = []
    root = os.fsencode(destination)
    for entry in manifest.entries:
        path = os.path.join(root, entry.path)
        try:
            info = os.stat(path, follow_symlinks=False)
        except (FileNotFoundError, NotADirectoryError):
            issues.append(f"missing: {display_path(entry.path)}")
            continue
        except OSError as exc:
            issues.append(f"cannot stat {display_path(entry.path)}: {exc}")
            continue
        if not stat.S_ISREG(info.st_mode):
            issues.append(f"not a regular file: {display_path(entry.path)}")
        elif info.st_size != entry.size:
            issues.append(
                f"size mismatch: {display_path(entry.path)} "
                f"(expected {entry.size}, found {info.st_size})"
            )
        elif info.st_mtime_ns != entry.mtime_ns:
            issues.append(
                f"mtime mismatch: {display_path(entry.path)} "
                f"(expected {entry.mtime_ns}, found {info.st_mtime_ns})"
            )
    return issues


class Controller:
    def __init__(self, config: Config, runner: SubprocessRunner | None = None):
        self.config = config
        self.runner = runner or SubprocessRunner()
        self.manifest_path = config.state_dir / "manifest.json"
        self.state_store = StateStore(config.state_dir / "state.json")
        self.lock = InstanceLock(config.state_dir / "controller.lock")
        self.scanner = RemoteManifestScanner(config.source, self.runner)
        self.rsync = RsyncExecutor(config, self.runner)
        self._rate = RollingRate(config.transfer.rate_window_seconds)

    def stop(self) -> None:
        LOG.warning("shutdown requested; terminating active rsync workers")
        self.runner.terminate_all()

    def _load_or_initialize(self) -> tuple[Manifest, TransferState]:
        manifest_exists = self.manifest_path.exists()
        state_exists = self.state_store.exists()
        if not manifest_exists:
            if state_exists:
                raise StateError(
                    "state.json exists but manifest.json is missing; refusing to regenerate the snapshot; "
                    "restore the manifest or use reset --yes"
                )
            manifest = self.scanner.scan()
            self._validate_manifest_compatibility(manifest)
            save_manifest(self.manifest_path, manifest)
            state = initialize_state(manifest.digest, manifest.total_bytes)
            self.state_store.save(state)
            LOG.info(
                "created persistent manifest: files=%d bytes=%d digest=%s",
                len(manifest.entries),
                manifest.total_bytes,
                manifest.digest,
            )
            return manifest, state
        manifest = load_manifest(self.manifest_path)
        self._validate_manifest_compatibility(manifest)
        if not state_exists:
            # Safe recovery from interruption between the two atomic first-run writes.
            state = initialize_state(manifest.digest, manifest.total_bytes)
            self.state_store.save(state)
            LOG.warning("recovered missing initial state from the existing persistent manifest")
        else:
            state = self.state_store.load()
        if state.manifest_digest != manifest.digest:
            raise StateError("state refers to a different manifest; refusing unsafe resume")
        configured = (self.config.source.host, self.config.source.user, self.config.source.path)
        recorded = (manifest.source_host, manifest.source_user, manifest.source_path)
        if configured != recorded:
            raise ManifestError(
                "configured source differs from the persistent manifest; use the original source or reset --yes"
            )
        valid_ids = {entry.id for entry in manifest.entries}
        if not state.completed <= valid_ids or not set(state.failed) <= valid_ids:
            raise StateError("state refers to paths not present in the manifest")
        return manifest, state

    def _validate_manifest_compatibility(self, manifest: Manifest) -> None:
        reserved = os.fsencode(self.config.transfer.partial_dir_name)
        collisions = [entry.path for entry in manifest.entries if reserved in entry.path.split(b"/")]
        if collisions:
            examples = ", ".join(display_path(path) for path in collisions[:10])
            raise ManifestError(
                f"source contains the reserved rsync partial directory component "
                f"{self.config.transfer.partial_dir_name!r}: {examples}"
            )

    def _record_progress(self, manifest: Manifest, active_workers: int) -> None:
        count = transferred_bytes(
            manifest, self.config.destination, self.config.transfer.partial_dir_name
        )
        rate = self._rate.add(count)
        snapshot = make_snapshot(count, manifest.total_bytes, rate)
        state = self.state_store.load()
        crossed = newly_crossed_thresholds(
            snapshot.transferred_bytes, snapshot.total_bytes, state.thresholds_emitted
        )

        def update(current: TransferState) -> None:
            current.transferred_bytes = snapshot.transferred_bytes
            current.total_bytes = snapshot.total_bytes
            current.rate_bytes_per_second = snapshot.rate_bytes_per_second
            current.eta_seconds = snapshot.eta_seconds
            current.active_workers = active_workers
            current.total_workers = self.config.transfer.workers
            current.thresholds_emitted.update(crossed)

        self.state_store.mutate(update)
        for threshold in crossed:
            LOG.info("progress threshold reached: %d%%", threshold)

    def _set_failed(self, message: str) -> None:
        if not self.state_store.exists():
            return

        def fail(state: TransferState) -> None:
            state.phase = "failed"
            state.active_workers = 0
            state.last_error = message

        self.state_store.mutate(fail)

    def _set_interrupted(self, message: str) -> None:
        if not self.state_store.exists():
            return

        def interrupt(state: TransferState) -> None:
            # Transfer/reconciliation is deliberately resumable on the next run.
            state.active_workers = 0
            state.last_error = message

        self.state_store.mutate(interrupt)

    def _record_resumable_error(self, message: str) -> None:
        if not self.state_store.exists():
            return

        def record(state: TransferState) -> None:
            state.active_workers = 0
            state.last_error = message

        self.state_store.mutate(record)

    def run(self, *, retry_failed: bool = False) -> bool:
        with self.lock:
            try:
                verify_prerequisites(self.config, self.runner)
                self.config.destination.mkdir(parents=True, exist_ok=True)
                manifest, state = self._load_or_initialize()
                current_source = self.scanner.scan()
                assert_same_source(manifest, current_source)
                if state.phase == "completed":
                    LOG.info("import is already completed; nothing to do")
                    return True
                if state.phase == "failed" and not retry_failed:
                    raise TransferError(
                        "import has persistent failed work; inspect status, then use run --retry-failed "
                        "after correcting the cause, or reset --yes for a new snapshot"
                    )
                if retry_failed:
                    failed_ids = set(state.failed)

                    def reopen(current: TransferState) -> None:
                        current.failed.clear()
                        for entry_id in failed_ids:
                            current.attempts.pop(entry_id, None)
                        current.phase = "initialized"
                        current.last_error = None

                    state = self.state_store.mutate(reopen)
                    LOG.info("reopened %d persistently failed file(s) for retry", len(failed_ids))

                def transferring(current: TransferState) -> None:
                    current.phase = "transfer"
                    current.total_workers = self.config.transfer.workers
                    current.last_error = None

                state = self.state_store.mutate(transferring)
                recovered_ids = {
                    entry.id
                    for entry in manifest.entries
                    if entry.id not in state.completed
                    and entry.id not in state.failed
                    and entry_is_complete(entry, self.config.destination)
                }
                if recovered_ids:
                    def recover_completed(current: TransferState) -> None:
                        current.completed.update(recovered_ids)

                    state = self.state_store.mutate(recover_completed)
                    LOG.info(
                        "recovered %d already-complete file(s) from destination metadata",
                        len(recovered_ids),
                    )
                pending = [
                    entry
                    for entry in manifest.entries
                    if entry.id not in state.completed and entry.id not in state.failed
                ]
                LOG.info(
                    "transfer phase starting: pending_files=%d completed_files=%d workers=%d",
                    len(pending),
                    len(state.completed),
                    self.config.transfer.workers,
                )
                scheduler = Scheduler(
                    self.config.transfer,
                    self.rsync,
                    self.state_store,
                    monitor=lambda active: self._record_progress(manifest, active),
                    completion_probe=lambda entry: entry_is_complete(
                        entry, self.config.destination
                    ),
                )
                if not scheduler.run(pending):
                    if self.runner.stopping:
                        raise TransferError("controller stopped before transfer completed")
                    raise TransferError("one or more work items exhausted their retry limit")

                # The exact source snapshot is checked before a whole-tree operation.
                assert_same_source(manifest, self.scanner.scan())

                def reconciling(current: TransferState) -> None:
                    current.phase = "reconciliation"
                    current.active_workers = 0

                self.state_store.mutate(reconciling)
                LOG.info("final serial reconciliation starting with rsync -aH")
                reconciliation = self.rsync.reconcile()
                if not reconciliation.success:
                    raise TransferError(
                        "final reconciliation failed with rsync status "
                        f"{reconciliation.returncode}: {reconciliation.error}"
                    )
                LOG.info("final serial reconciliation completed")
                assert_same_source(manifest, self.scanner.scan())
                issues = verify_destination(manifest, self.config.destination)
                if issues:
                    examples = "; ".join(issues[:10])
                    suffix = "; ..." if len(issues) > 10 else ""
                    raise TransferError(
                        f"post-reconciliation verification found {len(issues)} issue(s): {examples}{suffix}"
                    )

                all_ids = {entry.id for entry in manifest.entries}

                def completed(current: TransferState) -> None:
                    current.phase = "completed"
                    current.completed = all_ids
                    current.failed.clear()
                    current.transferred_bytes = manifest.total_bytes
                    current.total_bytes = manifest.total_bytes
                    current.rate_bytes_per_second = 0.0
                    current.eta_seconds = 0.0
                    current.active_workers = 0
                    current.thresholds_emitted.update(range(10, 101, 10))
                    current.last_error = None

                before = self.state_store.load().thresholds_emitted
                self.state_store.mutate(completed)
                for threshold in sorted(set(range(10, 101, 10)) - before):
                    LOG.info("progress threshold reached: %d%%", threshold)
                LOG.info("import completed successfully")
                return True
            except Exception as exc:
                if self.runner.stopping:
                    self._set_interrupted(str(exc))
                    LOG.warning("import interrupted and remains resumable: %s", exc)
                elif isinstance(exc, (SourceChangedError, TransferError)):
                    self._set_failed(str(exc))
                    LOG.error("import failed: %s", exc)
                else:
                    self._record_resumable_error(str(exc))
                    LOG.error("controller error; persistent work remains resumable: %s", exc)
                raise

    def verify(self) -> list[str]:
        """Read-only source-snapshot and destination verification."""
        with self.lock:
            verify_prerequisites(self.config, self.runner)
            if not self.manifest_path.exists():
                raise ManifestError("no persistent manifest exists; run the import first")
            manifest = load_manifest(self.manifest_path)
            assert_same_source(manifest, self.scanner.scan())
            return verify_destination(manifest, self.config.destination)
