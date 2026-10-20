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

`--verify` ignores metadata caching and rehashes everything. Use it periodically,
after restoring an index, or for filesystems with coarse/unreliable timestamps.
It also reconsiders successful work; physical layout changes made by external
tools may not always change the metadata used by the cache. Verification always
performs a portable full reconciliation and resets the native crawl baseline.
Switching from portable discovery to btrfs discovery likewise establishes a new
baseline. Existing version-1 indexes upgrade transactionally to version 2.

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
  file jobs remain independently retryable. Hashing acknowledges a job only after
  its stable per-file transaction commits. An interruption during hashing can
  repeat the unfinished file, but not previously completed files.
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
  records are replaced in a single transaction only after verifying stable
  descriptor metadata and path identity. Interrupted or unstable scans cannot
  commit a partially hashed file. A changed file is read in full; this does not
  require filesystem change journals or kernel-specific dirty-block tracking.
* `SEEK_DATA` skips full chunks of sparse holes when available, with a dense
  fallback. Whole-file fingerprints combine size and the ordered positions,
  lengths, and BLAKE3 hashes of nonzero chunks. Holes and allocated zeroes have
  the same whole-file fingerprint; hashing does not require reading huge holes.
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
a bounded chunk buffer; native mapping comparisons use memory proportional to
the extent count of the changed file. Duplicate listings stream.

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
QEMU 11.1.1 and Linux 6.18.55: **5 passed, 0 failed, no skips**.

Kernel interfaces: [FIDEDUPERANGE](https://man7.org/linux/man-pages/man2/ioctl_fideduperange.2.html),
[btrfs deduplication](https://btrfs.readthedocs.io/en/latest/Deduplication.html),
[FIEMAP](https://docs.kernel.org/filesystems/fiemap.html), and
[Linux UAPI structures](https://github.com/torvalds/linux/blob/master/include/uapi/linux/fs.h).
