#!/usr/bin/env python3
"""Release-mode native btrfs scale/apply benchmark in an isolated Linux VM.

Uses two disposable disks: btrfs inputs and ext4 persistent indexes. Every sample
restarts the actual CLI. KVM is used when available; auto otherwise selects TCG.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import subprocess
import tempfile
import time

spec = importlib.util.spec_from_file_location(
    'qemu_helpers', Path(__file__).with_name('test-btrfs-qemu.py'))
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)


REPEATED = {'idle-before-apply', 'idle-after-apply', 'idle-after-changes',
            'small-file-write-apply', 'large-file-4k-write-apply', 'new-duplicate-apply',
            'post-new-duplicate-reconcile'}
REQUIRED = REPEATED | {'initial-native-index', 'initial-apply', 'post-apply-reconcile',
                       'representative-write-apply', 'post-change-reconcile', 'oracle-full-scan'}


def validate(report, serial, returncode, files, repetitions):
    samples = report['samples']
    result = report.get('result', {})
    if (returncode != 0 or 'SKIP:' in serial or
            not re.search(r'^DEDUPIT_GUEST_EXIT=0$', serial, re.MULTILINE) or
            report.get('fixture', {}).get('files') != files or
            result.get('verified_files') != files + repetitions or
            not all(result.get(key) is True for key in
                    ['contents_verified', 'identities_preserved', 'native_matches_oracle'])):
        raise RuntimeError('benchmark must finish native apply, verify all files, and match a fresh full scan')
    names = [sample['case'] for sample in samples]
    if not REQUIRED.issubset(names) or any(names.count(name) != repetitions for name in REPEATED):
        raise RuntimeError('benchmark sample is missing or incomplete')
    for sample in samples:
        stats = sample['stats']
        if stats['errors'] or stats['stale'] or stats['unsupported']:
            raise RuntimeError('benchmark contains unsuccessful file operations')
        if sample['case'] != 'oracle-full-scan' and (
                stats['btrfs_subvolumes'] <= 0 or stats['scanner_fallbacks']):
            raise RuntimeError('benchmark must use native btrfs discovery')
        if (sample['case'].startswith('idle-') or sample['case'].endswith('-reconcile')) and any(stats[key] for key in
                ['files_seen', 'files_hashed', 'bytes_hashed', 'jobs_attempted', 'discovery_jobs']):
            raise RuntimeError('idle sample performed old-file work')
    if result.get('initial_deduped_bytes', 0) <= 0:
        raise RuntimeError('benchmark did not actually deduplicate data')
    if (result.get('initial_partial_range_jobs') != 1 or
            result.get('initial_whole_file_jobs') != files - 4 - 128 + 1):
        raise RuntimeError('benchmark must complete full-file and partial-range sharing')
    first = next(s for s in samples if s['case'] == 'initial-native-index')
    oracle = next(s for s in samples if s['case'] == 'oracle-full-scan')
    if first['stats']['files_hashed'] != files or oracle['stats']['files_hashed'] != files + repetitions:
        raise RuntimeError('benchmark indexed fewer files than requested')


def stream(command, log_path, report, output, timeout):
    deadline = time.monotonic() + timeout
    pending = b''
    with log_path.open('wb') as log, subprocess.Popen(
            command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT) as child:
        selector = selectors.DefaultSelector()
        selector.register(child.stdout, selectors.EVENT_READ)
        try:
            while True:
                if time.monotonic() > deadline:
                    raise TimeoutError(f'QEMU benchmark timed out; inspect {log_path}')
                if not selector.select(timeout=1):
                    continue
                chunk = os.read(child.stdout.fileno(), 65536)
                if not chunk:
                    break
                log.write(chunk)
                log.flush()
                pending += chunk
                lines = pending.split(b'\n')
                pending = lines.pop()
                for raw in lines:
                    line = raw.decode(errors='replace').rstrip('\r')
                    if line.startswith('DEDUPIT_BENCH_'):
                        kind, payload = line.split(' ', 1)
                        value = json.loads(payload)
                        if kind == 'DEDUPIT_BENCH_META':
                            report['fixture'] = value
                        elif kind == 'DEDUPIT_BENCH_SAMPLE':
                            report['samples'].append(value)
                        elif kind == 'DEDUPIT_BENCH_RESULT':
                            report['result'] = value
                        output.write_text(json.dumps(report, indent=2) + '\n')
                        print(line, flush=True)
                    elif line.startswith('DEDUPIT_GUEST_EXIT='):
                        print(line, flush=True)
            return child.wait(timeout=max(1, deadline-time.monotonic()))
        finally:
            selector.close()
            if child.poll() is None:
                child.kill()
                child.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kernel', type=Path, required=True)
    parser.add_argument('--modules', type=Path, required=True)
    parser.add_argument('--busybox', required=True)
    parser.add_argument('--cargo', default='cargo')
    parser.add_argument('--qemu', default='qemu-system-x86_64')
    parser.add_argument('--time', default=shutil.which('time') or '/usr/bin/time')
    parser.add_argument('--mkfs-ext4', default='mkfs.ext4')
    parser.add_argument('--files', type=int, default=1_000_000)
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--memory-mib', type=int, default=4096)
    parser.add_argument('--cpus', type=int, default=2)
    parser.add_argument('--data-gib', type=int, default=24)
    parser.add_argument('--state-gib', type=int, default=8)
    parser.add_argument('--accel', choices=['auto', 'kvm', 'tcg'], default='auto')
    parser.add_argument('--timeout', type=int, default=9000)
    parser.add_argument('--log', type=Path, default=Path('target/btrfs-scale-qemu/serial.log'))
    parser.add_argument('--output', type=Path, default=Path('target/btrfs-scale-qemu/results.json'))
    args = parser.parse_args()
    if not 260 <= args.files <= 5_000_000 or not 1 <= args.repetitions <= 20:
        parser.error('files must be 260..5000000 and repetitions 1..20')
    if min(args.memory_mib, args.cpus, args.data_gib, args.state_gib, args.timeout) <= 0:
        parser.error('VM budgets must be positive')
    accel = args.accel
    if accel == 'auto':
        accel = 'kvm' if os.access('/dev/kvm', os.R_OK | os.W_OK) else 'tcg'
    build = subprocess.check_output([args.cargo, 'build', '--release', '--locked',
                                    '--bin', 'dedupit', '--example', 'profile_btrfs',
                                    '--message-format=json'], text=True)
    artifacts = [json.loads(line) for line in build.splitlines()]

    def artifact(name):
        matches = [a['executable'] for a in artifacts if a.get('executable')
                   and a.get('target', {}).get('name') == name]
        if len(matches) != 1:
            raise RuntimeError(f'expected one executable for {name}: {matches}')
        return matches[0]

    cli, driver = artifact('dedupit'), artifact('profile_btrfs')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.log.parent.mkdir(parents=True, exist_ok=True)
    report = {'environment': {'accelerator': accel, 'memory_mib': args.memory_mib,
                             'cpus': args.cpus, 'data_gib': args.data_gib,
                             'state_gib': args.state_gib, 'kernel_image': args.kernel.name,
                             'qemu_version': subprocess.check_output([args.qemu, '--version'], text=True).splitlines()[0],
                             'cli_sha256': hashlib.sha256(Path(cli).read_bytes()).hexdigest(),
                             'driver_sha256': hashlib.sha256(Path(driver).read_bytes()).hexdigest(),
                             'git_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                             'data_mount': 'btrfs,compress=zstd', 'index_mount': 'ext4',
                             'build': 'release; default features',
                             'cache': 'guest cache retained; fixture setup and verification excluded from native timings'},
              'samples': []}
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    with tempfile.TemporaryDirectory(prefix='dedupit-btrfs-scale-') as temporary:
        work = Path(temporary)
        root = work / 'root'
        for directory in ['bin', 'dev', 'proc', 'sys', 'tmp', 'data', 'state']:
            (root / directory).mkdir(parents=True, exist_ok=True)
        helpers.copy_binary(args.busybox, root, '/bin/busybox')
        (root / 'bin/sh').symlink_to('busybox')
        helpers.copy_binary(cli, root, '/dedupit')
        helpers.copy_binary(driver, root, '/profile-btrfs')
        helpers.copy_binary(args.time, root, '/measure-time')
        modules = helpers.modules_for(args.modules, ['virtio_pci', 'virtio_blk', 'btrfs', 'ext4'], root)
        init = root / 'init'
        init.write_text('''#!/bin/sh
export PATH=/bin
/bin/busybox --install -s /bin
finish() { echo "DEDUPIT_GUEST_EXIT=$1"; sync; poweroff -f; }
trap 'finish 1' EXIT
set -eu
umask 022
mount -t devtmpfs devtmpfs /dev
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t tmpfs tmpfs /tmp
uname -a
grep CapEff /proc/self/status
cap=$(sed -n 's/^CapEff:[[:space:]]*//p' /proc/self/status)
[ $((0x$cap & 0x200000)) -ne 0 ]
''' + modules + '''
for attempt in $(seq 1 30); do
    [ ! -b /dev/vdb ] || break
    sleep 1
done
mount -t btrfs -o compress=zstd /dev/vda /data
mount -t ext4 /dev/vdb /state
export TMPDIR=/state RUST_BACKTRACE=1
''' + f'/profile-btrfs --cli /dedupit --time /measure-time --root /data/input --state /state --files {args.files} --repetitions {args.repetitions}\n' + '''
umount /data
umount /state
trap - EXIT
finish 0
''')
        init.chmod(0o755)
        files = b'\0'.join(str(p.relative_to(root)).encode()
                           for p in sorted(root.rglob('*'))) + b'\0'
        initrd = work / 'initrd.cpio'
        with initrd.open('wb') as output:
            subprocess.run(['cpio', '--null', '-o', '--format=newc', '--owner=0:0'],
                           input=files, cwd=root, stdout=output, check=True)
        data, state = work / 'data.img', work / 'state.img'
        for disk, gib in [(data, args.data_gib), (state, args.state_gib)]:
            with disk.open('wb') as output:
                output.truncate(gib * 1024**3)
        subprocess.run(['mkfs.btrfs', '-f', str(data)], check=True)
        subprocess.run([args.mkfs_ext4, '-t', 'ext4', '-F', '-q', '-E',
                        'lazy_itable_init=0,lazy_journal_init=0', str(state)], check=True)
        command = [args.qemu, '-accel', accel, '-m', str(args.memory_mib),
                   '-smp', str(args.cpus), '-display', 'none', '-serial', 'stdio',
                   '-monitor', 'none', '-nic', 'none', '-no-reboot',
                   '-kernel', str(args.kernel), '-initrd', str(initrd),
                   '-append', 'console=ttyS0 rdinit=/init panic=-1',
                   '-drive', f'file={data},format=raw,if=virtio',
                   '-drive', f'file={state},format=raw,if=virtio']
        print(f'Running {args.files:,}-file native benchmark with {accel}; log: {args.log}', flush=True)
        code = stream(command, args.log, report, args.output, args.timeout)
        validate(report, args.log.read_text(), code, args.files, args.repetitions)
        report['validated'] = True
        args.output.write_text(json.dumps(report, indent=2) + '\n')
        print(f'Validated native benchmark: {args.output}', flush=True)


if __name__ == '__main__':
    main()
