#!/usr/bin/env python3
"""Measure real million-file scans with bounded-memory fixture generation.

Warm filesystem cache; durable production commits. Fixture setup is excluded.
Repeat with another binary and the same directory to compare identical trees.
"""
import argparse
from contextlib import closing
import json
from pathlib import Path
import random
import shutil
import sqlite3
import subprocess
import tempfile
import time

from profile import clear_database, inventory, measured


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--files', type=int, default=1_000_000)
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--time', default=shutil.which('time') or '/usr/bin/time',
                        help='GNU time executable')
    parser.add_argument('--label', default='baseline')
    parser.add_argument('--commit-batch-size', type=int, choices=range(1, 4097),
                        metavar='1..4096', help='override the binary default')
    parser.add_argument('--reuse', action='store_true', help='reuse completed fixture')
    parser.add_argument('--skip-cold', action='store_true', help='reuse this label\'s index')
    parser.add_argument('--list', action='store_true', help='also time complete JSON listing to /dev/null')
    args = parser.parse_args()
    if min(args.files, args.repetitions) < 1:
        parser.error('counts must be positive')
    directory = args.directory.resolve()
    root = directory / 'files'
    manifest = directory / 'fixture.json'
    expected = dict(files=args.files, size=4096, groups=128, per_directory=1000,
                    seed=20261020)
    rng = random.Random(expected['seed'])
    groups = [rng.randbytes(expected['size']) for _ in range(expected['groups'])]
    if args.reuse:
        if json.loads(manifest.read_text()) != expected:
            raise RuntimeError('fixture parameters differ')
    else:
        root.mkdir(parents=True, exist_ok=False)
        started = time.perf_counter()
        for i in range(args.files):
            parent = root / str(i // expected['per_directory'])
            if i % expected['per_directory'] == 0:
                parent.mkdir()
            (parent / str(i)).write_bytes(groups[i % len(groups)])
            if (i + 1) % 100_000 == 0:
                print(f'Created {i + 1:,} files in {time.perf_counter()-started:.1f}s', flush=True)
        manifest.write_text(json.dumps(expected) + '\n')
    database = directory / (args.label + '.sqlite')
    results = directory / (args.label + '.json')
    rows = json.loads(results.read_text()) if args.skip_cold else []
    if not args.skip_cold:
        clear_database(database)
    base = [str(args.binary.resolve(strict=True)), '--scanner', 'portable', '--json',
            '--database', str(database), str(root)]
    if args.commit_batch_size is not None:
        base += ['--commit-batch-size', str(args.commit_batch_size)]
    first = root / '0' / '0'
    first.write_bytes(groups[0])

    def run(case):
        row = measured(base, args.time)
        row.update(case=case, binary=str(args.binary.resolve()), files=args.files,
                   commit_batch_size=args.commit_batch_size,
                   database_bytes=database.stat().st_size)
        stats = row['stats']
        assert stats['files_seen'] == args.files, stats
        assert stats['files_hashed'] == (args.files if case == 'cold' else
                                        1 if case == 'changed' else 0), stats
        unique = min(args.files, len(groups))
        if case == 'changed' and args.files > len(groups):
            unique += 1
        assert stats['duplicate_files'] == args.files - unique, stats
        assert stats['duplicate_chunks'] == args.files - unique, stats
        rows.append(row)
        results.write_text(json.dumps(rows, indent=2) + '\n')
        print(f"{args.label} {case}: {row['seconds']:.3f}s CPU={row['cpu_seconds']:.3f}s "
              f"RSS={row['peak_rss_kib']}KiB DB={row['database_bytes']/2**20:.1f}MiB", flush=True)

    if not args.skip_cold:
        run('cold')
    for _ in range(args.repetitions):
        # Reconcile the previous changed contents before measuring idle.
        if first.read_bytes() != groups[0]:
            first.write_bytes(groups[0])
            measured(base, args.time)
        elif args.skip_cold:
            measured(base, args.time)
        run('idle')
        with first.open('r+b') as output:
            output.write(b'changed!')
        run('changed')
    if args.list:
        # Include the CLI's metadata reconciliation and JSON serialization, but
        # avoid retaining gigabytes of reports in the harness or on disk.
        with tempfile.NamedTemporaryFile() as output:
            fmt = '{"seconds":%e,"user_seconds":%U,"system_seconds":%S,"peak_rss_kib":%M}'
            subprocess.run([args.time, '-f', fmt, '-o', output.name, '--',
                            *base, '--list'], stdout=subprocess.DEVNULL, check=True)
            row = json.load(output)
        row.update(case='list', binary=str(args.binary.resolve()), files=args.files,
                   database_bytes=database.stat().st_size,
                   cpu_seconds=row['user_seconds'] + row['system_seconds'])
        rows.append(row)
        results.write_text(json.dumps(rows, indent=2) + '\n')
        print(f"{args.label} list: {row}", flush=True)
    # Compare complete observations between versions, outside timed regions.
    digest = inventory(database).hex()
    (directory / (args.label + '.inventory')).write_text(digest + '\n')
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute('PRAGMA integrity_check').fetchone() == ('ok',)
        assert not connection.execute('PRAGMA foreign_key_check').fetchall()
    print(f'Inventory SHA256: {digest}; raw samples: {results}', flush=True)


if __name__ == '__main__':
    main()
