#!/usr/bin/env python3
"""Power-cut a VM after SQLite commits sharing, before btrfs commits its maps.

Two disposable disks deliberately give SQLite (ext4) and data (btrfs) independent
durability. SIGKILL terminates QEMU without guest shutdown, sync, or unmount.
The second boot must observe lost sharing, replay it, and preserve file bytes.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import re
import selectors
import subprocess
import tempfile
import time

spec = importlib.util.spec_from_file_location(
    'qemu_helpers', Path(__file__).with_name('test-btrfs-qemu.py'))
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)


def records(serial):
    decoder = json.JSONDecoder()
    return [decoder.raw_decode(serial[match.start():])[0]
            for match in re.finditer(r'^\{', serial, re.MULTILINE)]


def validate(cut, recovered):
    snapshots = records(cut)
    if (not snapshots or snapshots[-1].get('completed') != 1
            or snapshots[-1].get('unconfirmed') != 1
            or snapshots[-1].get('autocommit') is not True):
        raise RuntimeError('power cut must follow a durable completion with an unconfirmed mapping')
    if ('SKIP:' in cut + recovered or
            not re.search(r'^DEDUPIT_POWER_RECOVERY_OK\r?$', recovered, re.MULTILINE)):
        raise RuntimeError('recovery must verify bytes and inodes without skipping')
    stats = records(recovered)
    if len(stats) != 2:
        raise RuntimeError('missing recovery or idle statistics')
    replay, idle = stats
    for sample in stats:
        if (sample['errors'] or sample['stale'] or sample['unsupported']
                or sample['btrfs_subvolumes'] <= 0 or sample['scanner_fallbacks']):
            raise RuntimeError('power-cut recovery must use native discovery without errors')
    if (replay['jobs_completed'] != 1 or replay['bytes_deduped'] != 4096
            or replay['bytes_hashed'] != 0):
        raise RuntimeError(f'rolled-back sharing must replay without rehashing: {replay}')
    if any(idle[key] for key in ['files_seen', 'bytes_hashed', 'jobs_attempted', 'discovery_jobs']):
        raise RuntimeError(f'power-cut recovery must finish idle: {idle}')


def boot(command, log, timeout, cut):
    deadline = time.monotonic() + timeout
    pending = b''
    serial = bytearray()
    with subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT) as child:
        selector = selectors.DefaultSelector()
        selector.register(child.stdout, selectors.EVENT_READ)
        try:
            while time.monotonic() < deadline:
                if not selector.select(timeout=1):
                    continue
                chunk = child.stdout.read1(65536)
                if not chunk:
                    break
                log.write(chunk)
                log.flush()
                serial.extend(chunk)
                pending += chunk
                lines = pending.split(b'\n')
                pending = lines.pop()
                if cut and any(line.rstrip(b'\r') == b'DEDUPIT_POWER_CUT_READY' for line in lines):
                    child.kill()
                    if child.wait(timeout=10) != -9:
                        raise RuntimeError('VM was not killed abruptly')
                    return serial.decode(errors='replace')
            if time.monotonic() >= deadline:
                raise TimeoutError('power-cut VM boot timed out')
            code = child.wait(timeout=10)
            output = serial.decode(errors='replace')
            if cut or code != 0 or 'DEDUPIT_POWER_RECOVERY_OK\r\n' not in output:
                raise RuntimeError('VM did not reach its required power-cut/recovery checkpoint')
            return output
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
    parser.add_argument('--mkfs-ext4', default='mkfs.ext4')
    parser.add_argument('--timeout', type=int, default=300)
    parser.add_argument('--log', type=Path, default=Path('target/btrfs-powercut-qemu.log'))
    args = parser.parse_args()
    build = subprocess.check_output([args.cargo, 'build', '--locked', '--features',
                                    'test-failpoints', '--bin', 'dedupit', '--message-format=json'], text=True)
    executables = [item['executable'] for line in build.splitlines()
                   if (item := json.loads(line)).get('executable')
                   and item.get('target', {}).get('name') == 'dedupit']
    if len(executables) != 1:
        raise RuntimeError('expected one test-enabled dedupit executable')
    args.log.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='dedupit-powercut-') as temporary:
        work = Path(temporary)
        root = work / 'root'
        for name in ['bin', 'dev', 'proc', 'sys', 'tmp', 'data', 'state']:
            (root / name).mkdir(parents=True, exist_ok=True)
        helpers.copy_binary(args.busybox, root, '/bin/busybox')
        (root / 'bin/sh').symlink_to('busybox')
        helpers.copy_binary(executables[0], root, '/dedupit')
        modules = helpers.modules_for(args.modules, ['virtio_pci', 'virtio_blk', 'btrfs', 'ext4'], root)
        init = root / 'init'
        init.write_text('''#!/bin/sh
export PATH=/bin
/bin/busybox --install -s /bin
set -eu
mount -t devtmpfs devtmpfs /dev
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t tmpfs tmpfs /tmp
grep CapEff /proc/self/status
''' + modules + '''
for attempt in $(seq 1 30); do
    [ ! -b /dev/vdb ] || break
    sleep 1
done
mount -t btrfs -o compress=zstd /dev/vda /data
mount -t ext4 /dev/vdb /state
export TMPDIR=/state RUST_BACKTRACE=1
if [ ! -f /state/prepared ]; then
    mkdir /data/input
    dd if=/dev/urandom of=/data/input/a bs=4096 count=1
    cp /data/input/a /data/input/b
    sha256sum /data/input/a /data/input/b > /state/expected-sha256
    stat -c '%i:%s:%f:%h' /data/input/a /data/input/b > /state/expected-inodes
    /dedupit --database /state/index.sqlite --scanner btrfs --chunk-size 4096 --json /data/input
    touch /state/prepared
    sync
    DEDUPIT_TEST_FAILPOINT=apply:before-mapping-sync DEDUPIT_TEST_READY=/state/ready \
        /dedupit --database /state/index.sqlite --scanner btrfs --chunk-size 4096 --json --apply /data/input &
    dedupit_pid=$!
    for attempt in $(seq 1 300); do
        if [ -f /state/ready ] && grep -q 'State:.*T' /proc/$dedupit_pid/status; then
            cat /state/ready
            echo DEDUPIT_POWER_CUT_READY
            while true; do sleep 1; done
        fi
        sleep 0.1
    done
    exit 1
fi
sha256sum -c /state/expected-sha256
stat -c '%i:%s:%f:%h' /data/input/a /data/input/b > /state/actual-inodes
cmp /state/expected-inodes /state/actual-inodes
echo DEDUPIT_POWER_RECOVERY_STATS
/dedupit --database /state/index.sqlite --scanner btrfs --chunk-size 4096 --json --apply /data/input
sha256sum -c /state/expected-sha256
echo DEDUPIT_POWER_IDLE_STATS
/dedupit --database /state/index.sqlite --scanner btrfs --chunk-size 4096 --json --apply /data/input
echo DEDUPIT_POWER_RECOVERY_OK
umount /data
umount /state
poweroff -f
''')
        init.chmod(0o755)
        files = b'\0'.join(str(p.relative_to(root)).encode() for p in sorted(root.rglob('*'))) + b'\0'
        initrd = work / 'initrd.cpio'
        with initrd.open('wb') as output:
            subprocess.run(['cpio', '--null', '-o', '--format=newc', '--owner=0:0'],
                           input=files, cwd=root, stdout=output, check=True)
        data, state = work / 'data.img', work / 'state.img'
        for disk in [data, state]:
            with disk.open('wb') as output:
                output.truncate(1024**3)
        subprocess.run(['mkfs.btrfs', '-f', str(data)], check=True)
        subprocess.run([args.mkfs_ext4, '-t', 'ext4', '-F', '-q', '-E',
                        'lazy_itable_init=0,lazy_journal_init=0', str(state)], check=True)
        command = [args.qemu, '-accel', 'tcg', '-m', '1024', '-smp', '2', '-display', 'none',
                   '-serial', 'stdio', '-monitor', 'none', '-nic', 'none', '-no-reboot',
                   '-kernel', str(args.kernel), '-initrd', str(initrd),
                   '-append', 'console=ttyS0 rdinit=/init panic=-1',
                   '-drive', f'file={data},format=raw,if=virtio',
                   '-drive', f'file={state},format=raw,if=virtio']
        with args.log.open('wb') as log:
            cut = boot(command, log, args.timeout, True)
            recovered = boot(command, log, args.timeout, False)
        validate(cut, recovered)
        print(f'Power cut verified: durable completion replayed, bytes and inodes preserved; {args.log}')


if __name__ == '__main__':
    main()
