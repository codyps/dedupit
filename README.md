# dedupit

An incremental Rust tool for finding and sharing duplicate **whole files and
partial file ranges**. It scans filesystems mounted on Unix; Linux filesystems
that implement `FIDEDUPERANGE` can also share their storage. This includes btrfs
extent deduplication and reflink-enabled XFS. Filesystems without that operation
still support incremental duplicate reports.

Existing files retain their names, inodes, permissions, and independent write
semantics. The tool never substitutes hardlinks, deletes duplicate files, or
overwrites file data. Native deduplication uses the filesystem's atomic
compare-and-share operation. Hashes select candidates; the kernel checks the
actual bytes, including against concurrent writes.

## Usage

Requires Rust 1.85+ and a C compiler for bundled SQLite. Native sharing requires
Linux, filesystem support, and write permission on each destination file. Store
the index on a local filesystem with reliable SQLite locking.

```sh
cargo build --release --locked

# Discover and list duplicates without changing file storage.
./target/release/dedupit --database /var/cache/dedupit.sqlite --list /data

# Share matches during scanning, with whole-file jobs before partial ranges.
./target/release/dedupit --database /var/cache/dedupit.sqlite --apply /data

# Repeat the same command to process additions and changes incrementally.
./target/release/dedupit --database /var/cache/dedupit.sqlite --apply /data

# Require generation-based btrfs discovery, including after a restart.
# The invoking process needs CAP_SYS_ADMIN and CAP_DAC_READ_SEARCH (normally root).
sudo ./target/release/dedupit --database /var/cache/dedupit.sqlite \
  --scanner btrfs --apply /data

# Force the portable metadata walk on any supported Unix filesystem.
./target/release/dedupit --database /var/cache/dedupit.sqlite --scanner portable /data

# Choose either granularity; use a separate index for a different configuration.
./target/release/dedupit --database files.sqlite --granularity file --apply /data
./target/release/dedupit --database chunks.sqlite --granularity chunk \
  --chunk-size 4096 --apply /data

# JSON statistics, or JSON Lines with paths and offsets followed by statistics.
./target/release/dedupit --database /var/cache/dedupit.sqlite --json --list /data
```

The index directory must already exist. Roots, chunk size, and granularity are
bound to an index; changing them requires a new index. Multiple roots, nested
roots, regular-file roots, hardlinks, and non-UTF-8 Unix paths are supported.
Listings escape paths in text output and include raw path bytes in JSON output.
Paths and offsets in reports are hash-based candidates from the last scan, not
a filesystem-wide atomic snapshot. File and chunk duplicate totals overlap;
do not add them together.

Default chunk size is 128 KiB. Supported sizes are powers of two from 4 KiB to
16 MiB. Smaller chunks find finer partial matches at the cost of more index
space, hashing, and ioctl calls. Use `dedupit service` for continuous incremental
operation, or schedule the original one-shot command yourself.

## Native service and statistics

The foreground service handles scheduling, SIGTERM/SIGINT, disk-space checks,
and report persistence inside the Rust process. No shell or Python supervisor
is needed. Sharing remains opt-in on the command line:

```sh
# Directory service; wait an hour between runs and pause below 5 GiB free.
sudo dedupit service --state-directory /var/lib/dedupit --apply /data

# Linux/Btrfs: privately mount the top-level tree and scan every subvolume.
# Includes hidden subvolumes and bypasses read-only bind mounts of the Nix store.
sudo dedupit service --device /dev/mapper/root --scanner btrfs --apply

# Current progress, last complete report, and cumulative counters.
sudo dedupit stats
sudo dedupit stats --json
```

`--interval SECONDS` defaults to 3600 seconds after a run finishes or pauses.
`--minimum-free-bytes BYTES` defaults to 5368709120 (5 GiB), checked before
opening the index and every 30 seconds during work. The guard measures available
space on the **state-directory filesystem**; put the index on the scanned
volume when that volume needs protection. It is a polling guard, not a quota.
`--chunk-size` defaults to 1 MiB for the service, matching whole-volume usage.
Roots, chunk size and granularity remain bound to the index.

`--device` requires root/CAP_SYS_ADMIN and an already available Btrfs block
device. The process makes its own mount namespace private and mounts
`subvolid=5` using native syscalls. Its default mount path is
`STATE_DIRECTORY/filesystem`; `--mount-directory` selects another stable path.
The mount disappears on exit and never appears in the host namespace. Do not
change that path when reusing an index. Existing directory-based invocations
and portable scanning still work without whole-volume privileges.

Reports are private files in `--state-directory` (default `/var/lib/dedupit`):

- `filesystem.sqlite`: incremental index, reusable after interruption.
- `filesystem-status.json`: state, timestamps (Unix seconds), free space,
  current/latest run counters, errors, and cumulative totals. Updated at most
  every five seconds when work reaches a checkpoint, and at run boundaries.
- `filesystem-result.json`: last complete run's statistics, including file errors.
  An interrupted or paused run does not replace it.

The `All recorded` column includes live work and interrupted scans. Snapshot
deltas are persisted with the current scan, so restarting retains previously
recorded activity without counting a snapshot twice. A final snapshot is saved
on graceful cancellation or scan failure; a forced kill can lose work since the
last snapshot. JSON exposes these counters as `status.activity`; the older
`status.totals` counters retain their completed-run meaning. Separate counters
track completed, partial, failed, paused, and interrupted scans.

The service records its phase, including opening the index, scanning, summarizing,
and sharing. Duplicate-group counts are only available after summarization and
show `-` while the scan is unfinished. The number of scans started is shown even
before the first scan completes. Migration retains old completed counters and
any saved unfinished snapshot; work lost by older versions cannot be recovered.
Earlier one-shot reports remain readable.
Human-readable output groups counts, uses binary size units and compact elapsed
times, and compares current, last-complete, and cumulative counters in columns.
`stats` also reads the live database footprint (DB + WAL + SHM file sizes); JSON
includes this as `database` with byte counts, or `null` before index creation.
File sizes are sampled separately and can change while the service is running.
Bytes deduped
are **logical bytes accepted by the kernel**, not measured physical savings.
A crash or forced kill can leave the latest status stale; `stats` shows its age.
Reports use atomic replacement. Fatal scan failures and busy files are logged
and retried at the next interval. A failure to persist reports exits the service.
Only one service may own a state directory, including while waiting between runs.

Cancellation checkpoints run between metadata pages, files, hash chunks, and
sharing ioctls. Stopping rolls back the active SQLite batch; committed progress
remains reusable. A kernel operation or database migration can delay stopping;
a service manager may still impose a stop timeout.

On hardlink-heavy trees, Btrfs inode-to-path lookup can exceed the kernel's
4 KiB response limit. Dedupit falls back to paginated inode references and
resolves the parent directories, preserving all aliases and scan-root scoping.
This limit does not require a portable scan or indicate filesystem corruption.

## Nix and NixOS

```sh
nix build .#dedupit
nix develop -c cargo test --locked --features test-failpoints
```

The flake exports `packages.<system>.dedupit` (also `default`) and
`nixosModules.default` (also `dedupit`). Import the NixOS module with the host's
Nixpkgs; it builds the package with that same package set:

```nix
# flake.nix inputs
inputs.dedupit.url = "git+ssh://git@github.com/codyps/dedupit.git";
inputs.dedupit.inputs.nixpkgs.follows = "nixpkgs";

# NixOS modules list
modules = [
  inputs.dedupit.nixosModules.default
  {
    services.dedupit = {
      enable = true;
      device = "/dev/mapper/root";
      # apply = true;                 # false selects report-only mode
      # interval = 3600;
      # minimumFreeBytes = 5368709120;
      # chunkSize = 1048576;
    };
  }
];
```

`dedupit.service` starts at boot and runs the binary directly. There is no timer,
mount helper, or interpreter. The module persists state in `/var/lib/dedupit`
and mounts the volume at `/run/dedupit/filesystem` inside the service namespace.
It uses idle I/O scheduling, nice 19, a 2 GiB memory soft limit and a 4 GiB hard
limit. Systemd retries process failures after five minutes and allows 45 seconds
to stop. These standard unit settings can be overridden in the host configuration.
The package, including `dedupit stats`, is installed in the system profile.
On an ephemeral-root system, persist `/var/lib/dedupit` or its parent.

With `--apply`, sharing runs between committed scan batches, so duplicates can
start sharing storage before the full scan finishes. Each sharing pass considers
whole-file jobs before chunk jobs, with separate budgets of 1024 ranges, 64 MiB
of accepted/already-shared bytes, or one second per kind. Budgets are checked
between complete jobs; a single large job can exceed them. Forward-only cursors
avoid repeatedly sweeping old ranges. Cached sources and targets await namespace
and content reconciliation before use; deferred matches and requeued older jobs
are handled by a final pass. Failed jobs remain retryable on the next invocation.
JSON statistics include `interleaved_batches`. Report-only runs do not share data.

Hashing, sharing completion records and native discovery retain the same durable
batch boundaries during interleaving. Native extent maps stay provisional until
the final filesystem commit/confirmation barrier. This changes when sharing
starts; it does not evict index entries or impose a total index-size limit.

Database commits default to **1024 operations per batch**. Batches also flush
after 64 MiB of processed data or one second, checked between atomic operations;
a single large file, directory job, metadata page, or sharing job can exceed
those budgets. `--commit-batch-size 1` selects individual operation commits;
values through 4096 are supported. The policy can change when restarting the
same index. SQLite remains in WAL mode with `synchronous=FULL`.

File hashes are staged directly in the batch transaction. A failed file's entire
cache entry, including partial hashes, is invalidated before the batch can
commit, preserving successful neighbors. If that cleanup fails, the batch is
poisoned and the run aborts: none of its staged changes can commit. Directory
reconciliation uses [savepoints](https://www.sqlite.org/lang_savepoint.html) to
roll back partial namespace edits. Btrfs hashes, identity/mapping records, and
their queue acknowledgements commit together. Discovery cursors and the work
they enqueue also commit together. Apply completion records are staged only
after the kernel reports success and file identities are revalidated. A process
crash discards the unfinished batch: discovery/hashing repeats its uncommitted
operations, and apply safely repeats sharing whose completion did not commit.
Previously committed batches remain reusable. Phase boundaries and normal exit
flush the final batch; fatal errors and unwinding roll it back. Batching trades
bounded repeated work after a crash for fewer synchronous commits.

Native btrfs `--apply` captures destination extent maps through its already-open
file descriptors. Maps and completion records share the apply batch. The maps
remain provisional until a final filesystem commit flushes btrfs's delayed inode
updates. A batched metadata search then confirms only inode items whose size,
mtime, ctime and incarnation still match the indexed file. The same discovery batches durably queue unrelated inode and directory changes
before advancing their cursors or generation boundary.

After successful mapping confirmation, an otherwise idle invocation needs no
destination reopening or mapping refresh. After a crash, startup queues every unconfirmed mapping.
Unchanged files refresh their maps without reading contents or losing committed
completions when the maps still match. A mismatched provisional map requeues
sharing jobs without rereading unchanged contents; changed files are fully rehashed because the
filesystem transaction might have rolled back independently of SQLite. File
hashes never adopt new timestamps merely because deduplication succeeded. If a
post-sharing metadata probe fails, its job remains pending for safe replay.

`--verify` ignores metadata caching and rehashes everything. Use it periodically,
after restoring an index, or for filesystems with coarse/unreliable timestamps.
It also reconsiders successful work; physical layout changes made by external
tools may not always change the metadata used by the cache. Verification always
performs a portable full reconciliation and resets the native crawl baseline.
Switching from portable discovery to btrfs discovery likewise establishes a new
baseline. Existing version-1/2/3 indexes upgrade transactionally to version 4
without rereading files; legacy zero-filled chunk candidates are removed.

Exit code 0 means the run finished without operational errors. Exit code 2
means per-file errors occurred; other files were still processed. Fatal errors
such as a locked/incompatible index exit nonzero. Unsupported native operations
are counted separately and do not constitute successful work. Their jobs remain
retryable. Use report-only mode on filesystems without native sharing.

## How incrementality works

`--scanner auto` (the default) uses btrfs generation discovery when all input
scopes are accessible through its metadata ioctls. It reports a reason and falls
back to the portable scanner if the required privileges, filesystem support,
or accessible subvolume roots are missing. `--scanner btrfs` requires the native
scanner and fails explicitly instead of silently falling back.

When a bind mount hides the containing subvolume root, native discovery can
mount that root privately through Linux's detached mount API (Linux 5.2+).
This requires mount privileges, access to the mounted block device, `/proc`,
and `/sys/fs/btrfs`. The device is pinned by descriptor and checked against
the filesystem's kernel device inventory; the mounted filesystem and subvolume
identities are checked again before use. The metadata mount is read-only,
nodev, nosuid, noexec and close-on-exec. It has no mount point, does not change
the caller's mount namespace, and is released when its last descriptor closes,
including on errors or process exit. Discovery maps paths back through the
original bind mount: hidden siblings are never indexed, listed or shared.
Unavailable mount support follows the same auto-fallback/required-scanner error
rules above. `--scanner portable` never creates these metadata mounts.

The btrfs scanner follows the generation-filtered crawl model used by
[bees](https://github.com/Zygo/bees/blob/master/docs/how-it-works.md):

* Each subvolume has a persistent UUID-based identity, completed transaction
  watermark, active generation window, and full tree-key cursor. A filesystem
  sync followed by `START_SYNC`/`WAIT_SYNC` establishes a committed upper bound;
  buffered writes are not silently excluded from the scan.
* `TREE_SEARCH_V2` uses a lower leaf-generation filter. The upper leaf bound
  remains unrestricted: newer COW leaves can contain older unprocessed items.
  Returned inode items are filtered by their own transaction IDs against the
  active window. A page's discovered inode jobs and next cursor commit together.
* After a restart, an unfinished metadata search resumes at its saved key.
  Enumeration may advance the watermark once its work is durably queued; failed
  file jobs remain independently retryable. Stable hashes and queue
  acknowledgements become durable in the same batch. An interruption can repeat
  operations in the unfinished batch; earlier committed batches are preserved.
* Changed COW files compare their old and new extent mappings. Only affected
  aligned chunks are reread, including truncation/EOF boundaries and newly
  allocated data. Splitting a large extent around a small write preserves the
  hashes of unchanged mappings. Whole-file fingerprints are reconstructed from
  cached chunk hashes, including in `--granularity file` mode. NOCOW/NODATASUM
  files and new inode incarnations conservatively receive a full rehash.
* Changed directory jobs reconcile their immediate children to detect deletions.
  Directory renames queue known descendants and walk the moved subtree when
  needed, so old files moved into scope are discovered. Nested subvolumes and
  newly created snapshots get independent initial crawls even when their data's
  inode generations predate the parent's watermark. UUID changes or a lower
  committed generation invalidate the affected baseline.

An idle native run does not walk the directory tree or open every indexed file.
It still probes metadata access, enumerates relevant subvolume references, and
synchronizes the filesystem. Scope filtering may inspect metadata outside a
requested subdirectory of a subvolume; it hashes/shares files only within the
requested roots. A changed directory can require reading all of that directory's
names; a moved subtree can require a subtree walk. Index writes on the scanned
filesystem can themselves cause new metadata generations; placing the index on
another local filesystem reduces that feedback.

The portable fallback works as follows:

* SQLite stores file identity `(device, inode)`, size, nanosecond mtime/ctime,
  filesystem identity, and BLAKE3 range fingerprints. Unchanged files require
  metadata checks but no content reads. Unchanged observations use a temporary
  table, avoiding a durable database write/fsync per file.
* Changed files are hashed with one reusable chunk buffer. Their old range
  records are replaced within the enclosing batch only after verifying stable
  descriptor metadata and path identity. Interrupted or unstable scans cannot
  commit a partially hashed file. A changed file is read in full; this does not
  require filesystem change journals or kernel-specific dirty-block tracking.
* `SEEK_DATA` skips full chunks of sparse holes when available, with a dense
  fallback. Whole-file fingerprints combine size and the ordered positions,
  lengths, and BLAKE3 hashes of nonzero chunks. Holes and allocated zeroes have
  the same whole-file fingerprint; hashing does not require reading huge holes.
  All-zero chunks are omitted from chunk candidates as well, so sharing a hole
  with allocated zero data cannot change duplicate reports without changing bytes.
* Indexed fingerprint lookups select a representative for each duplicate group,
  rather than comparing every pair. Pending jobs and successful source/target
  relationships persist across runs. Report-only runs preserve pending work for
  a later `--apply`. Full-file success consumes redundant destination chunk jobs.
  Unique pending ranges are cleared in one batch. Summary counts are cached and
  transactionally invalidated, avoiding a full index aggregation on unchanged runs.
* Replacing or deleting a source invalidates dependent completion records and
  requeues surviving targets. Deleted paths are pruned only after a complete
  directory walk. Directory traversal errors preserve unvisited records, so
  totals may include old observations; native application revalidates files.
* A process lock permits one scanner/applicator per index. SQLite uses WAL,
  foreign keys, and full synchronous commits. The index, WAL, journal, lock,
  shared-memory file, and their hardlinks are excluded from scanning.

Every **portable** run still walks directory entries and reads metadata: generic Unix APIs
do not supply a persistent, portable change journal. Incrementality saves
content reads and repeated sharing operations, not traversal. Content reads use
a lazily allocated chunk buffer reused across files; native mapping comparisons use memory proportional to
the extent count of the changed file. Native directory reconciliation streams
cached child paths; it retains the names of the current directory only when
there are previously indexed files to check. Very wide directories and highly
fragmented files can therefore exceed ordinary scan memory usage. Duplicate
listings stream groups and their ranges in index order, without sorting all
duplicate paths or loading singleton paths. Native queue selection seeks by
subvolume, job kind and inode; failed jobs remain retryable without repeatedly
scanning the already attempted prefix. Newly queued earlier inodes are picked up
in another pass during the same invocation.

## Native sharing and correctness

On Linux the backend issues `FIDEDUPERANGE`, which provides atomic
compare-and-share, without changing file length. A whole-file job covers the
complete file, split into requests of at most 16 MiB, including an EOF tail.
The backend handles ioctl-level errors, per-destination errors, differing data,
and partial progress separately. Zero/invalid progress never counts as success.
Requests use filesystem-aligned offsets; the kernel decides whether a particular
EOF tail or inode is eligible.

On btrfs the same operation shares individual logical ranges even when the rest
of the files differs. The filesystem UUID and sector size come from
`BTRFS_IOC_FS_INFO`, allowing candidates across subvolumes with different device
IDs. Cross-mount sharing remains subject to the running kernel's restrictions.
FIEMAP skips a request when ordinary, unencoded mappings already reference the
same physical bytes. Compressed or otherwise ambiguous mappings always fall
through to kernel verification. Physical mappings are never accessed directly.

The engine reopens paths without following symlinks (including intermediate
components), rejects special files, checks indexed identity/timestamps before
sharing, and verifies them again afterward. It never refreshes a fingerprint to
silently bless a concurrent writer. Nonoverlapping duplicates within one file
can share too; hardlinked names are indexed once.

Successful work is recorded **after** sharing. A crash between the ioctl and
the database commit can repeat a harmless dedupe request. A multi-request
whole-file job can be partially shared after an interruption; retry completes
it. There is no multi-file transaction, snapshot guarantee, or rollback of
successful sharing, and none is necessary to preserve contents.

The metadata fingerprint is a performance hint, not a proof that content has
never changed. Coarse timestamps, a restored database, network metadata caches,
or privileged timestamp manipulation can cause missed duplicates until
`--verify`. Even stale or colliding fingerprints cannot authorize overwriting
different bytes: native sharing always performs the kernel comparison.

The program reports logical bytes accepted by the kernel and bytes already
shared, **not physical space freed**. Snapshots, compression, other references,
and filesystem metadata affect physical savings. Later writes or defragmentation
can break sharing.

## Limits

There is no universal safe in-place dedupe operation. Filesystems exposing only
clone/copy operations, or lacking shared storage entirely, receive reports only.
In particular, discovery works on other Unix platforms, but the current native
backend is Linux-only. The `Backend` trait allows adding other atomic native
implementations.

Native discovery requires Linux metadata ioctl capabilities and an accessible
subvolume root, or the privileges/support for a detached metadata mount described
above. Overlaid/nested mounts inside a requested scope conservatively
require the portable scanner; ordinary nested btrfs subvolumes are supported.
The kernel's inode-path ioctl has a 4 KiB result limit. An overflowing hardlink
set is retained as a failed, retryable job with an explicit error; use
`--scanner portable` for such a scope. Metadata permission or concurrent-change
errors retain jobs rather than advancing past unrecorded work.

Partial discovery uses a fixed logical chunk grid, independent of physical
extent fragmentation. It finds chunks at different file offsets when aligned to
that grid, but not arbitrary byte-shifted repeats, differences inside a chunk,
or partial matches smaller than the configured chunk size. Whole-file matches
include EOF tails. Empty files have no storage ranges and are not reported.
Pure sparse holes are excluded from chunk reports. Entirely zero-valued files
can still match as whole files.

The filesystem may reject sharing for read-only snapshots, incompatible inode
flags, swap files, encryption, block alignment, or other native restrictions.
No destructive fallback is attempted. Indexes should be kept in a trusted
directory; the process has the privileges of its invoking user.

## Profiling

`tools/profile.py` compares optimized binaries on reproducible disposable data.
It measures initial scans, unchanged scans, a single changed file, and a dense
64 MiB file with 4 KiB chunks. `--apply` adds native sharing of 2,500 small files
and an unchanged apply restart. Setup/indexing time is excluded from apply time;
each repetition rewrites destinations to break sharing from previous runs.
The harness checks range inventories across binaries and file bytes after apply.

```sh
# Save an optimized baseline binary before making changes, then build again.
CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --release --locked
python3 tools/profile.py --binary /path/to/before --binary target/release/dedupit \
  --directory target/profile-comparison --repetitions 5 --apply
```

Requires Python 3.9+ and GNU `time` (`--time /path/to/time` if needed). The
directory must be new and on the filesystem being measured; `--apply` needs
native sharing support. Results include every sample's wall time, CPU time,
peak RSS, and application statistics in `results.json`. Binary order alternates
between repetitions. Data is warm in the page cache, so these results measure
processing and commit costs rather than cold-device read throughput. Privileged
btrfs generation discovery is covered by the QEMU correctness/stress suite;
this benchmark selects portable discovery explicitly.

CPU profiles of unchanged scans identified repeated SQL preparation as a major
cost. Heap profiles identified per-component path strings, per-file chunk
buffers, and per-chunk copies of cached hashes. Scan and apply statements now
use a bounded prepared-statement cache; hashing reuses scratch storage and reads
cached hashes directly. Linux uses `openat2` with
[`RESOLVE_NO_SYMLINKS`](https://man7.org/linux/man-pages/man2/openat2.2.html)
to validate every path component in one call where supported, retaining the
component-by-component fallback. Bounded database batches reduce filesystem
sync overhead while retaining atomic file/job updates and identity validation.
`--commit-batch-size 1` is available for comparing commit policies. Hashing and
index insertion dominate dense small-chunk scans. Use CPU/heap profilers
separately from wall-time benchmarks.

For file-count scaling, `tools/profile_scale.py` creates **one million actual
4 KiB files** in 1,000 directories, with 128 deterministic content groups. Fixture
generation streams filenames and is excluded from measurements. It measures a
fresh index, unchanged scans, single-file changes and optional complete JSON
listing, recording CPU, wall time, peak RSS and index size. Each binary uses its
own database. The harness checks expected duplicate counts, SQLite integrity and
foreign keys, and records a complete range-inventory digest for comparison.

```sh
python3 tools/profile_scale.py --binary /path/to/before \
  --directory target/profile-million --label before --repetitions 3 --list
python3 tools/profile_scale.py --binary target/release/dedupit \
  --directory target/profile-million --label after --repetitions 3 --reuse --list
cmp target/profile-million/{before,after}.inventory
```

Pass `--time /path/to/time` to select GNU `time`, and optionally
`--commit-batch-size 64` to compare commit policies. All data and indexes remain
in the chosen directory for inspection. Measurements use a warm filesystem
cache and durable commits; initial scans and listing each have one timing sample,
while unchanged and changed runs repeat. Portable restart time grows with file
count because every file still needs a metadata check. Native idle discovery
avoids that traversal, although filesystem synchronization can dominate runtime.
Index storage grows with files and cached chunks; a smaller chunk size increases
that cost.

CPU profiles of large reports also identified JSON value construction and
formatted stdout writes as major costs. Reports now serialize borrowed fields
directly into a 64 KiB output buffer, preserving the JSON schema and byte-path
arrays without constructing per-record JSON trees. The buffer flushes before
final statistics, and output errors propagate. Recorded million-file measurements
and their conditions are in [benchmarks/million-files.json](benchmarks/million-files.json).

For **native discovery and apply at scale**, `tools/profile_btrfs_qemu.py`
boots an isolated guest with CAP_SYS_ADMIN, a 24 GiB btrfs data disk and a separate
8 GiB ext4 index disk. Release binaries and GNU `time` measure each new CLI
process. Defaults are one million real files, three idle/change samples, 4 GiB
of guest RAM and two CPUs. `--accel auto` uses accessible KVM or records TCG
explicitly; emulated CPU timings must be interpreted accordingly.

```sh
python3 tools/profile_btrfs_qemu.py --kernel /path/to/bzImage \
  --modules /lib/modules/KERNEL_VERSION --busybox /path/to/static/busybox \
  --files 1000000 --repetitions 3
```

Install QEMU, btrfs-progs, e2fsprogs, cpio and GNU `time`; the same matching-kernel
requirements as the correctness VM apply. The manual **Native btrfs scale
benchmark** GitHub Actions workflow installs these dependencies and uploads the
full serial log and JSON measurements. This expensive benchmark is separate
from the mandatory correctness CI. Use `--files 1024` for a quick smoke run.

Four files within the requested count are controls: two identical 8 MiB files
and two 384 KiB files sharing only a middle 128 KiB range. Other files are 4 KiB
members of 128 deterministic groups. Samples cover initial native indexing,
initial `--apply`, post-apply mapping reconciliation, idle restarts, small-file
writes, 4 KiB writes in the large shared destination, new duplicate files and a
write to the representative of a large duplicate group. Catch-up samples are
reported separately, including the first restart after sharing changes extent
mappings. Newly created duplicate files increase the final count by the number
of repetitions. The representative case reports how many completion records
its change invalidates and how many jobs get replayed.

All original file bytes and inode identities are checked against the fixture
model after sharing and writes. A fresh portable `--verify` scan must have the
same complete logical inventory, with SQLite integrity and foreign-key checks
on both indexes. The runner rejects fallback discovery, missing samples, partial
verification, non-idle idle samples or omitted full-file/partial-range sharing.
Measurements and progress are streamed to `target/btrfs-scale-qemu/`; disposable
VM disks are removed afterward. These results include VM storage and filesystem
synchronization costs and do not represent bare-metal throughput.

A [recorded million-file native run](benchmarks/native-btrfs-million-files.json)
on QEMU/KVM with two vCPUs, 4 GiB RAM and Linux 6.8 measured 251 seconds for
initial indexing and 156 seconds for 999,869 full-file jobs plus one partial-range
job. The first post-apply mapping refresh took another 43.9 seconds, opening
999,870 changed destinations but reading no contents. Subsequent idle `--apply`
runs had a median of 46 ms with no old-file opens, content reads or sharing jobs.
Small-file writes took 186 ms; 4 KiB writes inside an 8 MiB shared file took
172 ms, reading one 128 KiB chunk and reusing 63. Adding/applying a new duplicate
took 155 ms. A representative-source write took 3.72 seconds and replayed 7,811
completion jobs while hashing only the changed 4 KiB file. Small-change and idle
times are medians of three samples; initial operations and representative writes
are single samples. Peak application RSS was 9.5 MiB, with a 686 MiB native index
after reconciliation. All 1,000,003 final files passed byte checks, original inode
identities were preserved, and the index matched a fresh full scan. These are VM
measurements with the conditions and every sample retained in the JSON record.

The earlier native record predates mapping capture during apply. The
[updated million-file run](benchmarks/native-btrfs-apply-cache-million-files.json)
confirmed 999,870 destination mappings during `--apply`. Its very first following
invocation took **148 ms**, with **zero input-file opens, content reads, or sharing
jobs**, replacing the earlier 43.9-second refresh. Subsequent idle applies had a
113 ms median. All 1,000,003 final files passed byte/inode checks and matched a
fresh full scan; each 4 KiB write in the shared 8 MiB control still reused 63 of
64 cached chunks. The native index occupied 652 MiB; peak native CLI RSS was
9.5 MiB.

Initial apply took 242.7 seconds, including map capture, filesystem commit and
batched discovery acknowledgement. Apply plus its first follow-up therefore
totaled **242.9 seconds**, versus **200.3 seconds** in the earlier record. These
separate hosted runs do not establish a throughput improvement: unaffected
initial indexing and fresh full scans also differed substantially between hosts.
The JSON retains both totals and every sample rather than presenting the removed
refresh as free work. The benchmark workflow's `compare_baseline=true` option
runs the earlier baseline and current build sequentially on one host, in fresh
VMs with the same resources and fixture model, for a more useful comparison.

A [matched 100,000-file KVM comparison](benchmarks/native-btrfs-apply-cache-paired-100k.json)
on one host measured apply plus first follow-up at **15.35 seconds before** and
**11.91 seconds after**, a **22.4% reduction**. Apply itself increased from 7.18
to 11.89 seconds, while the following refresh fell from 8.17 seconds to 16 ms
with no input-file opens. Both variants passed all 100,003 final byte/inode and
fresh-scan checks. This is one timing pair, not an estimate of the percentage
gain at a million files. The million-file run reduced explicit database batches
for apply plus first follow-up from 1,962 to 985 by sharing apply transactions
with mapping updates.

The comparison gate rejects different accelerators, VM resources, guest kernels
or fixture sizes, unverified runs, and reused executable hashes. Project release
artifacts are cleaned between worktrees while dependency artifacts stay cached.

## Validation

```sh
cargo test --locked --all-targets
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
```

Tests exercise incremental reads, pending/completed state across restart,
source deletion, partial progress, unstable files, unsupported backends,
hardlinks, non-UTF-8 paths, sparse/dense equality, and exclusive locking.
Native tests run when the temporary directory is on btrfs and explicitly print
a skip reason otherwise:

```sh
TMPDIR=/path/on/btrfs cargo test --locked native_ -- --nocapture
```

Native tests cover partial extent sharing, whole files exceeding 16 MiB with
EOF tails, copy-on-write isolation, and deliberately false hash matches rejected
by the kernel. Injected-source tests exercise metadata cursor recovery, durable
queue replay, generation bounds, small COW writes, truncation, sparse extension,
NOCOW, inode reuse, scope moves, directory renames/deletions, and rollback.

Generation-discovery integration tests require privileges as well as btrfs:

```sh
sudo env TMPDIR=/path/on/btrfs DEDUPIT_REQUIRE_BTRFS_SCANNER=1 \
  cargo test --locked --test btrfs_discovery -- --nocapture
```

Without those prerequisites these tests print a skip reason; setting the
environment variable makes an unavailable scanner fail the tests. On every push
and pull request, CI runs ordinary sharing tests on a dedicated btrfs loopback
filesystem and required privileged discovery tests in a separate QEMU job,
including snapshot discovery and agreement with a fresh full rehash. The QEMU
job uploads its serial log even on failure.

The QEMU suite includes a seeded differential stress test: three fixed seeds,
16 rounds per seed, and 16 shuffled mutation types per round (at least 768
operations). It covers copies, writes, appends, truncation/extension, sparse writes,
hole punching, hardlinks, symlinks, file/directory renames and deletions, moves
across the scan boundary, atomic replacement, non-UTF-8 names, empty files and
sizes around chunk boundaries. Each scan
starts a new CLI process. Every fourth round stops and kills a process inside
an uncommitted native file batch after a previous batch committed, makes
additional changes while dedupit is stopped, then resumes from the same database;
each seed must exercise an interruption.
Every round also runs `--apply`. Controlled incompressible files require both
whole-file and partial-range jobs to complete, followed by writes to the shared
sources to verify independent copy-on-write behavior. On another four-round
cycle the test kills an apply process with new completion records staged inside
an unfinished batch, modifies files during downtime, and resumes `--apply`;
each seed must exercise an apply interruption too.

Seven additional tests exercise 23 deterministic crash cases, concurrent writes during apply confirmation, and file-failure
cleanup: portable scans
with spilled uncommitted WAL pages, btrfs discovery cursors and hash/queue
acknowledgements, and apply before/after commits and between kernel sharing and
database acknowledgement. An independent reader captures the complete durable
database state while the child is stopped; recovery must reproduce that state
exactly, including hashes, queue entries, cursors and completion records. Restart
checks prove committed files/jobs are reused, reconcile downtime mutations,
compare against a fresh full scan, verify contents and copy-on-write isolation,
and check SQLite integrity/foreign keys. Injected file errors prove successful
neighbors survive and failed-file records are removed; an injected cleanup
failure must abort and roll back the entire unfinished batch. Random stress uses two-operation
batches; deterministic cases cover both 64- and 1024-operation batches.

CI also boots a separate two-disk VM, with btrfs data and an ext4 index. After
SQLite durably records sharing, the guest stops before btrfs commits the new
mapping. The host sends SIGKILL to QEMU without guest sync or shutdown, then
boots the same disks again. Recovery must detect the rolled-back mapping,
repeat kernel sharing without rehashing, preserve bytes and original inode
identities, and finish with an idle run. A successful reboot alone cannot pass:
the test requires actual lost sharing and a new 4 KiB deduplication operation.
The log is uploaded alongside the discovery-suite log. Run it separately with:

```sh
python3 tools/test_btrfs_powercut_qemu.py \
  --kernel /path/to/bzImage \
  --modules /lib/modules/KERNEL_VERSION \
  --busybox /path/to/static/busybox
```

The QEMU runner builds with the `test-failpoints` feature and requires all seven
batch crash, concurrent-write, and mapping-recovery tests in the compiled test listing, so omission of the feature or a
required test fails CI. Stop hooks and the tiny SQLite cache used to force WAL
spill exist only in that opt-in test build; normal builds contain neither.

An independent in-memory model records expected bytes, file identities,
hardlink aliases, copies, sparse extension, hole punching, renames, deletions,
and symlink targets. The test reads every modeled file inside and outside the
scan scope and compares its actual bytes with this model after mutations,
apply runs, interruptions, and restarts. This catches corruption even if an
incremental scan and a fresh scan would agree on the same corrupted contents.

After every mutation batch, the test compares the incremental index against a
new empty database scanned with `--scanner portable --verify`. It checks the
complete file inventory by device/inode (hardlinks may choose different path
representatives), validates that indexed paths still resolve to those files,
compares fingerprints and every whole-file/chunk range hash, checks duplicate
counts/bytes and SQLite integrity, and requires an idle restart to read no data
and visit no old files, including on an idle `--apply` restart. This tests process
interruption, not guest power loss.

To run the privileged tests without granting the host additional capabilities,
use the QEMU runner. It boots an x86_64 Linux kernel with a small initramfs,
checks that the guest has `CAP_SYS_ADMIN`, and runs all discovery tests with
`DEDUPIT_REQUIRE_BTRFS_SCANNER=1` on a disposable 1 GiB btrfs disk mounted with
zstd compression. Both correctness runners automatically use accessible KVM;
`--accel tcg` forces software emulation, which works without `/dev/kvm`. An
explicit `--accel kvm` request fails if KVM is unavailable. The guest
has no networking or shared host directories. A failed test, skipped scanner,
boot failure, timeout, ignored test, or incomplete test suite fails the runner.

Requires Python 3.9+, Cargo and its linker, `ldd`, `cpio`, `mkfs.btrfs`,
`qemu-system-x86_64`, a static BusyBox with mount/insmod/poweroff applets, and
an x86_64 kernel with its matching modules. Compressed `.ko.zst` modules also
require the host `zstd` command. For example:

```sh
python3 tools/test-btrfs-qemu.py \
  --kernel /path/to/bzImage \
  --modules /path/to/lib/modules/KERNEL_VERSION \
  --busybox /path/to/static/busybox
```

The runner builds the current Rust discovery tests and includes their dynamic
libraries in the guest, preserving their paths (including Nix store paths).
`--cargo` and `--qemu` select executables outside `PATH`. Serial output is saved
to `target/btrfs-qemu.log`; the timeout defaults to 900 seconds. The temporary
disk and initramfs are removed after the run. This runner was exercised with
QEMU 11.1.1 and Linux 6.18.55. To replay a failing stress seed or increase the
workload, add `--stress-seed DECIMAL_U64 --stress-rounds N` (1 through 256).
Seed, round, mutations, and interruptions are recorded in the serial log.
Direct Cargo runs accept `DEDUPIT_STRESS_SEED` and `DEDUPIT_STRESS_ROUNDS`.

Kernel interfaces: [FIDEDUPERANGE](https://man7.org/linux/man-pages/man2/ioctl_fideduperange.2.html),
[btrfs deduplication](https://btrfs.readthedocs.io/en/latest/Deduplication.html),
[FIEMAP](https://docs.kernel.org/filesystems/fiemap.html), and
[Linux UAPI structures](https://github.com/torvalds/linux/blob/master/include/uapi/linux/fs.h).
