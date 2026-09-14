# rsync-parallel-import

`rsync-parallel-import` is a Linux-oriented controller for moving a relatively
small number of very large files from a remote source over SSH. It runs several
independent rsync processes so high-latency or lossy links can use multiple TCP
streams, while rsync retains responsibility for transfer correctness, archive
metadata, and partial-file recovery.

The controller creates one immutable manifest for an import, balances its
regular files across workers by bytes, retries workers independently, persists
completion and progress, and finishes with one serial whole-tree `rsync -aH`
reconciliation. It never adds `--delete`; unrelated destination data is not
removed.

## Safety and architecture

One controller process owns an import. A non-blocking `flock` in the state
directory prevents another controller from running against the same state.
Workers receive byte-exact, NUL-delimited `--files-from=- --from0` lists. Python
starts executables with argv arrays and `shell=False`; filenames are never
placed in a shell command or parsed by line.

On the first run the controller scans the source and atomically stores a
manifest containing each regular file's raw relative pathname (base64 in
JSON), size, and nanosecond mtime. That manifest is the import snapshot and the
progress denominator. Later runs reuse it. Before transfer, before
reconciliation, and after reconciliation, the complete source listing must
still match. Added, removed, resized, or re-timestamped files cause a visible
failure. The package cannot detect content modified while both size and mtime
are deliberately restored; use an immutable source snapshot when that threat
matters.

Each successful worker assignment is recorded atomically. A controller crash
before that update can only cause rsync to inspect the assignment again; rsync
will skip current files and resume partial ones. Worker partitions may split a
source hard-link set, so the parallel phase alone does **not** promise hard-link
preservation. The mandatory final serial `rsync -aH` pass repairs hard-link
relationships where the source and destination filesystems support them and
transfers anything genuinely missing.

“Reconciliation” and “verify” are intentionally different:

- Reconciliation is the modifying, automatic final `rsync -aH` operation. It
  never implies `--delete`.
- `rsync-parallel-import verify` is read-only. It rechecks the source snapshot,
  then compares each manifested destination regular file's size and mtime. It
  does not run rsync or repair anything.

The manifest covers regular files. Directory metadata, symlinks, devices, and
other archive objects are handled by the whole-tree reconciliation, but do not
contribute bytes to progress. Destination filesystems must preserve the source
mtime precision for strict verification.

## Requirements and installation

- Python 3.11 or newer
- local OpenSSH client and rsync
- remote SSH server, rsync, POSIX `sh`, GNU `find`, and GNU/coreutils `base64`
- a POSIX filesystem for controller state (`flock` and atomic rename)

Install from a checkout into a virtual environment or system environment:

```console
python3 -m venv /opt/rsync-parallel-import-venv
/opt/rsync-parallel-import-venv/bin/pip install .
/opt/rsync-parallel-import-venv/bin/rsync-parallel-import --help
```

When installing system-wide, adapt the service unit's `ExecStart` to the actual
installed executable. The package has no third-party runtime dependencies.

## SSH contract

This package does not create keys, edit `authorized_keys`, configure an agent,
install host keys, or manage credentials. As the Unix account that will run the
controller, this must already finish without a password, passphrase, or host-key
prompt:

```console
ssh source-user@source.example.net true
```

The controller checks this prerequisite and both local and remote rsync before
creating an import, and reports a clear error when one fails. `ssh_options` can enforce settings such as
`BatchMode=yes`; each TOML array element is exactly one SSH argument. Do not put
private key contents or secrets in the configuration. If an identity path is
needed, configure it as normal SSH client policy or as `"-i",
"/path/to/key"` tokens, with appropriate file permissions.

## Configuration

Copy [examples/rsync-parallel-import.toml](examples/rsync-parallel-import.toml)
to `/etc/rsync-parallel-import.toml` and edit it:

```toml
[source]
host = "source.example.net"
user = "source-user"
path = "/srv/export"
ssh_options = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]

[destination]
path = "/srv/import"

[state]
path = "/var/lib/rsync-parallel-import"

[transfer]
workers = 16
max_attempts = 5
backoff_initial_seconds = 5
backoff_max_seconds = 300
poll_interval_seconds = 2
rate_window_seconds = 60
partial_dir_name = ".rsync-parallel-import-partial"
ssh_check_timeout_seconds = 15
```

All tables reject unknown keys. Required values are `source.host`,
`source.user`, absolute `source.path`, absolute `destination.path`, and integer
`transfer.workers` (1–256). `[state]` is optional and defaults to
`/var/lib/rsync-parallel-import`. Destination and state paths may not overlap.

Transfer settings:

| Key | Default | Meaning |
| --- | ---: | --- |
| `workers` | 16 | Maximum concurrent rsync processes |
| `max_attempts` | 5 | Total failed attempts allowed for an assigned file before terminal failure |
| `backoff_initial_seconds` | 5 | Delay after the first failure |
| `backoff_max_seconds` | 300 | Exponential-backoff cap |
| `poll_interval_seconds` | 2 | Progress and worker polling interval |
| `rate_window_seconds` | 60 | Rolling throughput window |
| `partial_dir_name` | `.rsync-parallel-import-partial` | Reserved per-directory rsync partial area |
| `ssh_check_timeout_seconds` | 15 | Early SSH prerequisite timeout |

Compression is explicitly disabled because expected inputs are often already
compressed. The partial directory name must be one path component and should
not collide with source content; the controller rejects a manifest containing
that path component. rsync creates these directories alongside
destination files as needed and uses them on the next attempt.

## First run and manual operation

Create the destination and state directories with permissions for the intended
unprivileged account, then run:

```console
rsync-parallel-import --config /etc/rsync-parallel-import.toml run
```

The first source scan is persisted before workers start. State layout is:

```text
STATE_DIRECTORY/
├── controller.lock
├── manifest.json
└── state.json
```

Do not edit these files. Both JSON data files use atomic replacement and the
manifest has an integrity digest. If initialisation is interrupted after the
manifest write but before the state write, `run` safely reconstructs an empty
state from that existing manifest—it does not rescan the source.

Normal network failures affect only the relevant worker. The assignment is
retried with capped exponential backoff, while healthy workers continue. rsync
uses archive and partial semantics on every attempt. A controller process crash
or host reboot leaves the manifest, completed assignments, retry counts, and
threshold notifications on disk; the next `run` resumes them.

After `max_attempts`, affected paths remain in persistent failed state and the
command exits unsuccessfully. Correct the network, permissions, disk-space, or
source problem, inspect status, then explicitly reopen terminal work:

```console
rsync-parallel-import --config /etc/rsync-parallel-import.toml run --retry-failed
```

This clears attempt counts only for terminal failed paths and retains the same
manifest and completed work. A changed source snapshot still requires reset.

## Progress and status

```console
rsync-parallel-import --config /etc/rsync-parallel-import.toml status
rsync-parallel-import --config /etc/rsync-parallel-import.toml status --json
```

Status includes phase, logical manifested bytes transferred/total, percentage,
recent aggregate rate, ETA, active/configured workers, retries, terminal failed
paths, and the last error. Finished destination files count up to their expected
size. Resting rsync partial files count when their manifested path can be
identified. The controller also recognizes rsync's receiver-side
`.name.XXXXXX` temporary file shape while a worker is active. Very long names
that rsync must truncate, or a nonstandard rsync temporary naming scheme, may
not contribute until rsync finishes or parks the file in the partial directory.

Rate is `(newest bytes - oldest bytes) / elapsed time` over a rolling 60-second
window by default. ETA is remaining logical bytes divided by that rate. It is
shown as unknown until at least two useful samples exist, and rate history
starts fresh with a controller process (the byte counter itself is persistent).

When progress first reaches or passes each 10% boundary, the controller logs an
exact, searchable message such as:

```text
progress threshold reached: 30%
```

Emitted thresholds are saved in `state.json`, so restarts do not repeat old
notifications. A large jump can emit several newly crossed boundaries once.

To perform a non-modifying check after or during administration:

```console
rsync-parallel-import --config /etc/rsync-parallel-import.toml verify
```

Verification takes the same exclusive controller lock. It reports source
snapshot changes separately from missing or mismatched destination files.

## systemd

The repository includes
[systemd/rsync-parallel-import.service](systemd/rsync-parallel-import.service).
Choose the service account; the example uses `rsync-import`. That account needs:

- write and traversal access to the destination;
- read/write access to the state directory;
- working non-interactive SSH access to the configured source;
- read access to its SSH configuration and private key, if used.

Example setup (paths and ownership are administrator choices):

```console
sudo install -d -o rsync-import -g rsync-import /srv/import /var/lib/rsync-parallel-import
sudo install -m 0644 systemd/rsync-parallel-import.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now rsync-parallel-import.service
systemctl status rsync-parallel-import.service
journalctl -u rsync-parallel-import.service -f
```

Edit `User`, `Group`, `ExecStart`, and both `ReadWritePaths` first. The unit waits
for `network-online.target`, runs unprivileged, writes normal readable Python
logs to stdout/stderr for journald, and restarts after controller failure.
Enabling it starts it again after reboot. The persistent lock prevents duplicate
controllers even if the service is accidentally invoked twice. SIGTERM asks the
controller to terminate active rsync children, records zero active workers, and
leaves the current transfer/reconciliation phase resumable.
The example also rate-limits repeated controller failures to five starts per 30
minutes, preventing a terminal persistent failure from creating an endless
tight restart loop.

## Reset and starting a new snapshot

Changing the configured source or accepting source changes is never implicit.
To discard only the controller's current manifest/state and create a new import:

```console
sudo systemctl stop rsync-parallel-import.service
rsync-parallel-import --config /etc/rsync-parallel-import.toml reset --yes
rsync-parallel-import --config /etc/rsync-parallel-import.toml run
```

`--yes` is mandatory, reset refuses while another controller holds the lock,
and destination files are not removed. The lock file remains as an empty state
directory marker. Reset does not undo files already transferred; the new run's
rsync processes will compare them normally.

## Troubleshooting and recovery

- **SSH prerequisite failed:** run the documented `ssh ... true` command as the
  service user. Resolve authentication, host keys, DNS, firewall, or SSH policy
  outside this package.
- **Manifest scan failed:** confirm remote `sh`, coreutils `base64`, and GNU
  `find` are in the non-interactive SSH PATH and the source is readable.
- **Source no longer matches:** restore the immutable source snapshot, or stop
  the service and use `reset --yes` to intentionally define a new one.
- **Worker failures:** inspect `status`, `journalctl`, free space, permissions,
  and rsync's diagnostic. Use `run --retry-failed` only after fixing the cause.
- **Reconciliation failure:** no completion is claimed. A later
  `run --retry-failed` (even when the failed-file count is zero) reopens the
  failed phase and performs safe rsync comparisons before trying reconciliation
  again.
- **Missing/corrupt manifest:** the controller will not silently regenerate it.
  Restore the matching state backup or use the explicit reset procedure.
- **Permission or timestamp verification errors:** ensure the destination
  filesystem supports regular Unix archive metadata and the service user owns
  or can update the relevant paths.

Logs intentionally omit full SSH/rsync command lines so private identity paths
and sensitive SSH option values are not exposed. The trust boundary includes
the configured remote account and source host: remote command execution and
rsync necessarily trust them to present the intended tree. No destination
deletion is requested, but rsync will update a destination path when it differs
from its manifested source file, which is normal import behavior.

## Tests

After installing the package from the checkout, run the complete test suite
with:

```console
python -m unittest discover -v
```

The tests use standard-library `unittest`, temporary fixtures, fake subprocess
runners, and no SSH server. Local `rsync` is required for the real local-rsync
integration tests. CI installs rsync and runs the suite on Python 3.11, 3.12,
and 3.13. The same lightweight source checks used by CI can be run with
`python -m compileall -q src tests` and `python -m tabnanny src tests`.

It covers strict TOML parsing, manifests and unusual Unix names, deterministic
and atomic persistence, locking, byte balancing, rate/ETA and persistent
thresholds, retry transitions, source-change detection, resume selection,
worker isolation, argv/NUL command construction, reconciliation flags, reset
safety, status, verification, and CLI behavior.

## License

MIT; see [LICENSE](LICENSE).
