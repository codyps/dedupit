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

# Deduplicate whole files first, then remaining matching partial ranges.
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
space, hashing, and ioctl calls. There is no background service: schedule the
same command periodically for continuous incremental operation.

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

`--verify` ignores metadata caching and rehashes everything. Use it periodically,
after restoring an index, or for filesystems with coarse/unreliable timestamps.
It also reconsiders successful work; physical layout changes made by external
tools may not always change the metadata used by the cache. Verification always
performs a portable full reconciliation and resets the native crawl baseline.
Switching from portable discovery to btrfs discovery likewise establishes a new
baseline. Existing version-1/2 indexes upgrade transactionally to version 3
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

Native discovery requires an accessible subvolume root and Linux metadata ioctl
capabilities. Overlaid/nested mounts inside a requested scope conservatively
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

Four additional tests exercise 15 deterministic crash cases and file-failure
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

The QEMU runner builds with the `test-failpoints` feature and requires all four
batch crash tests in the compiled test listing, so omission of the feature or a
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
zstd compression. Software emulation (TCG) works without `/dev/kvm`. The guest
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
to `target/btrfs-qemu.log`; the timeout defaults to 600 seconds. The temporary
disk and initramfs are removed after the run. This runner was exercised with
QEMU 11.1.1 and Linux 6.18.55. To replay a failing stress seed or increase the
workload, add `--stress-seed DECIMAL_U64 --stress-rounds N` (1 through 256).
Seed, round, mutations, and interruptions are recorded in the serial log.
Direct Cargo runs accept `DEDUPIT_STRESS_SEED` and `DEDUPIT_STRESS_ROUNDS`.

Kernel interfaces: [FIDEDUPERANGE](https://man7.org/linux/man-pages/man2/ioctl_fideduperange.2.html),
[btrfs deduplication](https://btrfs.readthedocs.io/en/latest/Deduplication.html),
[FIEMAP](https://docs.kernel.org/filesystems/fiemap.html), and
[Linux UAPI structures](https://github.com/torvalds/linux/blob/master/include/uapi/linux/fs.h).
