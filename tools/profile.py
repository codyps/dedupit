#!/usr/bin/env python3
"""Repeatable warm-cache release benchmarks, with per-child CPU and peak RSS.

Creates disposable fixtures inside a new directory; never scans user data.
Requires Python 3.9+ on Linux. Optional --apply uses actual native sharing.
"""
import argparse
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import sqlite3
import statistics
import subprocess
import tempfile
import time


def measured(command, time_binary):
    # GNU time forks from its small, freshly exec'd process. Direct wait4 from
    # Python can include the parent's inherited RSS before exec, exaggerating
    # the Rust process's peak. CPU time includes the small time-wrapper overhead.
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as error, \
            tempfile.NamedTemporaryFile() as memory:
        start = time.perf_counter()
        with subprocess.Popen([time_binary, "-f", "%M", "-o", memory.name, "--"] + command,
                              stdout=output, stderr=error) as child:
            _, status, usage = os.wait4(child.pid, 0)
            child.returncode = os.waitstatus_to_exitcode(status)
        elapsed = time.perf_counter() - start
        output.seek(0)
        error.seek(0)
        if child.returncode:
            raise RuntimeError(error.read().decode(errors="replace"))
        stats = json.load(output)
        peak_rss = int(memory.read())
    if stats["errors"] or stats["stale"] or stats["unsupported"]:
        raise RuntimeError(f"Incomplete benchmark run: {stats}")
    return dict(seconds=elapsed, cpu_seconds=usage.ru_utime + usage.ru_stime,
                peak_rss_kib=peak_rss, stats=stats)


def clear_database(database):
    for suffix in ("", "-wal", "-shm", "-journal", ".dedupit-lock"):
        database.with_name(database.name + suffix).unlink(missing_ok=True)


def inventory(database):
    # Independent indexes should have identical logical observations, including
    # after apply. IDs and timestamps are deliberately excluded.
    with closing(sqlite3.connect(database)) as connection:
        digest = hashlib.sha256()
        for row in connection.execute(
            "SELECT f.path,r.kind,r.offset,r.length,r.hash FROM ranges r "
            "JOIN files f ON f.id=r.file_id ORDER BY f.path,r.kind,r.offset"
        ):
            for value in row:
                data = value if isinstance(value, bytes) else str(value).encode()
                digest.update(len(data).to_bytes(8, "little"))
                digest.update(data)
        return digest.digest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, action="append", required=True,
                        help="repeat to compare binaries on identical fixtures")
    parser.add_argument("--directory", type=Path, required=True,
                        help="new directory on the filesystem to benchmark")
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--files", type=int, default=2500)
    parser.add_argument("--dense-mib", type=int, default=64)
    parser.add_argument("--time", default=shutil.which("time") or "/usr/bin/time",
                        help="GNU time executable, used for peak RSS")
    parser.add_argument("--apply", action="store_true",
                        help="also benchmark native apply on disposable files")
    args = parser.parse_args()
    if min(args.repetitions, args.files, args.dense_mib) < 1:
        parser.error("counts must be positive")
    args.directory.mkdir(parents=True, exist_ok=False)
    root = args.directory.resolve()
    small, dense = root / "small", root / "dense"
    small.mkdir()
    dense.mkdir()
    rng = random.Random(20261020)
    groups = [rng.randbytes(16384) for _ in range(64)]
    paths = []
    for i in range(args.files):
        directory = small / str(i // 100)
        directory.mkdir(exist_ok=True)
        path = directory / str(i)
        path.write_bytes(groups[i % len(groups)])
        paths.append(path)
    with (dense / "data").open("wb") as file:
        for _ in range(args.dense_mib):
            file.write(rng.randbytes(1024 * 1024))
    binaries = [path.resolve(strict=True) for path in args.binary]
    rows = []
    reference = {}
    for repetition in range(args.repetitions):
        # Alternate order to reduce systematic drift between versions.
        order = binaries if repetition % 2 == 0 else list(reversed(binaries))
        for binary in order:
            database = root / "index.sqlite"
            clear_database(database)
            paths[0].write_bytes(groups[0])
            base = [str(binary), "--scanner", "portable", "--json",
                    "--database", str(database)]

            def run(case, scope, *flags):
                result = measured(base + list(flags) + [str(scope)], args.time)
                result.update(binary=str(binary), case=case, repetition=repetition)
                observed = inventory(database)
                if case in reference and reference[case] != observed:
                    raise RuntimeError(f"Index mismatch: {binary}, {case}")
                reference[case] = observed
                rows.append(result)
                (root / "results.json").write_text(json.dumps(rows, indent=2) + "\n")
                print(f"{binary.name}: {case}: {result['seconds']:.3f}s "
                      f"CPU={result['cpu_seconds']:.3f}s "
                      f"RSS={result['peak_rss_kib']}KiB", flush=True)
                return result["stats"]

            stats = run("cold-small", small)
            assert stats["files_hashed"] == args.files
            stats = run("idle-small", small)
            assert stats["bytes_hashed"] == 0
            with paths[0].open("r+b") as file:
                file.write(b"changed!")
            stats = run("changed-small", small)
            assert stats["files_hashed"] == 1 and stats["bytes_hashed"] == 16384
            clear_database(database)
            stats = run("dense-4k", dense, "--chunk-size", "4096")
            assert stats["bytes_hashed"] == args.dense_mib * 1024 * 1024
            if args.apply:
                clear_database(database)
                # Rewrite every destination to break sharing from earlier runs.
                # Setup and indexing are outside the apply measurement.
                for i, path in enumerate(paths):
                    path.write_bytes(groups[i % len(groups)])
                measured(base + [str(small)], args.time)
                stats = run("apply-small", small, "--apply")
                assert stats["jobs_completed"] == max(0, args.files - 64)
                stats = run("apply-idle", small, "--apply")
                assert stats["jobs_attempted"] == 0 and stats["bytes_hashed"] == 0
                for i, path in enumerate(paths):
                    if path.read_bytes() != groups[i % len(groups)]:
                        raise RuntimeError(f"Contents changed by apply: {path}")
    print("\nMedians (warm page cache, durable SQLite commits):")
    for binary in binaries:
        for case in dict.fromkeys(row["case"] for row in rows):
            runs = [r for r in rows if r["binary"] == str(binary) and r["case"] == case]
            print(f"{binary.name}: {case}: "
                  f"{statistics.median(r['seconds'] for r in runs):.3f}s "
                  f"CPU={statistics.median(r['cpu_seconds'] for r in runs):.3f}s "
                  f"RSS={statistics.median(r['peak_rss_kib'] for r in runs):.0f}KiB")
    print(f"Raw samples: {root / 'results.json'}")


if __name__ == "__main__":
    main()
