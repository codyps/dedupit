# dedupit

Incremental deduplication for whole files and partial file ranges. Works on
Unix; Linux filesystems supporting `FIDEDUPERANGE`, including Btrfs and
reflink-enabled XFS, can also share duplicate storage.

Files keep their names, inodes, permissions, and independent write behavior.
Dedupit never deletes files or replaces them with hardlinks. Hashes identify
candidates; the kernel compares actual bytes before sharing storage.

## Quick start

Requires Rust 1.85+ and a C compiler for bundled SQLite.

```sh
cargo build --release --locked

# Find duplicates without changing file storage.
./target/release/dedupit --database /path/to/index.sqlite --list /data

# Share duplicates. Repeat to process additions and changes incrementally.
./target/release/dedupit --database /path/to/index.sqlite --apply /data
```

The index directory must exist. Use a local filesystem with reliable SQLite
locking. Sharing requires write permission on destination files and filesystem
support. Without `--apply`, runs only update the index and report duplicates.

Useful options:

| Option | Purpose |
| --- | --- |
| `--list` | List duplicate paths and offsets. |
| `--json` | Emit JSON statistics; with `--list`, emit JSON Lines. |
| `--scanner auto` | Use Btrfs discovery when available, otherwise report why and fall back to a portable walk (default). |
| `--scanner btrfs` | Require native discovery; normally needs root (`CAP_SYS_ADMIN` and `CAP_DAC_READ_SEARCH`). |
| `--scanner portable` | Always walk directory entries and check file metadata. |
| `--granularity file\|chunk\|both` | Select whole files, chunks, or both (default). |
| `--chunk-size BYTES` | Power of two from 4 KiB to 16 MiB; default 128 KiB. |
| `--verify` | Rehash all files with a portable full scan and reconsider completed sharing. |

Roots, chunk size, and granularity are bound to the index; use a new index to
change them. Smaller chunks find finer matches but increase index size and work.
Multiple roots, file roots, hardlinks, and non-UTF-8 Unix paths are supported.
See `dedupit --help` and each subcommand's `--help` for all options.

## Continuous service

The foreground service schedules scans, handles shutdown, checks free space,
and saves reports. These examples assume `dedupit` is on `PATH`.

```sh
# Scan directories continuously.
sudo dedupit service --state-directory /var/lib/dedupit --apply /data

# Btrfs: privately mount the top-level tree and scan every subvolume.
sudo dedupit service --device /dev/mapper/root --scanner btrfs --apply

sudo dedupit stats
sudo dedupit stats --json
sudo dedupit stats --errors
```

Defaults are `/var/lib/dedupit` for state, 1 MiB chunks, and a 3,600-second wait
between runs. The service pauses below 5 GiB available on the **state-directory
filesystem**, checked before opening the index and every 30 seconds during work.
Adjust with `--interval` and `--minimum-free-bytes`. This is a polling guard,
not a quota.

`--device` requires root/mount privileges and an available Btrfs block device.
It includes hidden subvolumes through a private mount that disappears on exit.
Keep `--mount-directory` stable when reusing an index.

State includes the reusable `filesystem.sqlite` index, live
`filesystem-status.json`, and the last complete `filesystem-result.json`.
Normal stats read small snapshots; `--errors` streams detailed error logs.
Only one service can own a state directory. Preserve it across reboots,
including on systems with ephemeral roots.

Stats distinguish current sharing estimates from cumulative activity:

- **Current sharing** covers verified indexed files and includes existing
  reflinks, snapshots, and sharing by other tools. It catches up as scanning
  and reconciliation progress; it is not a whole-volume free-space measurement.
- **Physical estimate** accounts for encoded allocation sizes within its shown
  coverage, excluding metadata and RAID overhead.
- **Dedupe accepted** counts logical bytes accepted by sharing operations.
  Repeated work may count again; this is not current space saved.

## NixOS

```sh
nix build .#dedupit
```

Add the flake input and import its module:

```nix
# Flake inputs
inputs.dedupit.url = "git+ssh://git@github.com/codyps/dedupit.git";
inputs.dedupit.inputs.nixpkgs.follows = "nixpkgs";

# NixOS modules
modules = [
  inputs.dedupit.nixosModules.default
  {
    services.dedupit = {
      enable = true;
      device = "/dev/mapper/root";
      # apply = false;  # Report only; the module enables sharing by default.
    };
  }
];
```

The module installs `dedupit`, starts the service at boot, and keeps state in
`/var/lib/dedupit`. It uses idle I/O scheduling and memory limits of 2 GiB soft /
4 GiB hard. Options include `interval`, `minimumFreeBytes`, `chunkSize`, and
`databaseStorage`; see [nix/module.nix](nix/module.nix).

## Index storage

`--database-storage` selects a Btrfs policy; other filesystems keep their normal
storage behavior:

| Policy | Main database | WAL |
| --- | --- | --- |
| `cow` (default) | Filesystem defaults | Filesystem defaults |
| `nocow` | Uncompressed NOCOW | NOCOW |
| `compressed` | COW with zstd requested | Normal COW |
| `hybrid` | COW with zstd requested | NOCOW |

NOCOW disables Btrfs data checksums for these files. SQLite retains full
synchronous commits and recovery under every policy. Converting an existing
COW database to NOCOW requires offline compaction:

```sh
sudo systemctl stop dedupit.service
sudo dedupit compact --database /var/lib/dedupit/filesystem.sqlite --storage nocow
sudo systemctl start dedupit.service
```

Compaction validates and atomically replaces the database, preserving scan
progress. Allow enough space for both copies. A `.compact` file left by a killed
process can be removed before retrying; the original remains authoritative until
replacement.

## Behavior and limits

- Btrfs discovery tracks filesystem generations and reuses unchanged chunk
  hashes. Idle runs avoid a full tree walk. Portable scans check every file's
  metadata and read changed files in full.
- Committed progress survives interruption. An unfinished batch is retried;
  successful sharing may safely repeat. Shutdown can wait for a kernel operation
  or database migration.
- Sharing preserves contents through the kernel's atomic compare-and-share
  operation. There is no multi-file snapshot or rollback of completed sharing.
  Symlinks and special files are not followed for sharing.
- Read-only files can serve as donors for writable copies. Two read-only copies
  cannot be merged. Incompatible checksum policies and other filesystem
  restrictions can prevent sharing; there is no destructive fallback.
- Reports describe hash-based candidates from the last scan. Whole-file and
  chunk totals overlap. Empty files and all-zero chunks have no reported storage
  ranges. Compression, snapshots, and later writes affect actual space savings.
- Metadata caching can miss changes with unreliable timestamps or a restored
  index. Use `--verify` periodically or after restoring an index. The kernel
  still rejects sharing between different bytes.
- Exit code `0` means completion without operational errors; `2` means per-file
  errors occurred while other files were processed. Fatal errors exit nonzero.
  Unsupported sharing operations are counted separately and remain retryable.

## Development

```sh
cargo test --locked --all-targets
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings

# Run native sharing tests on Btrfs.
TMPDIR=/path/on/btrfs cargo test --locked native_ -- --nocapture

# Require privileged Btrfs discovery tests.
sudo env TMPDIR=/path/on/btrfs DEDUPIT_REQUIRE_BTRFS_SCANNER=1 \
  cargo test --locked --test btrfs_discovery -- --nocapture
```

CI exercises native sharing, discovery, crash recovery, concurrent writes, and
power loss in disposable Btrfs environments. The [QEMU correctness runner](tools/test-btrfs-qemu.py)
and [power-cut runner](tools/test_btrfs_powercut_qemu.py) accept a kernel,
matching modules, and a static BusyBox; use `--help` for setup options.

Profiling tools cover [small fixtures](tools/profile.py),
[million-file portable scans](tools/profile_scale.py), and
[native Btrfs scans in QEMU](tools/profile_btrfs_qemu.py).
Recorded measurements and their conditions are in [benchmarks/](benchmarks/).
