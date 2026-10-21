#!/usr/bin/env python3
"""Run mandatory native discovery tests as root in an isolated Linux VM.

Requires an x86_64 Linux host, Cargo, QEMU, mkfs.btrfs, cpio, a static
BusyBox, and a kernel with its matching modules directory. No host root or KVM
is required. Only a disposable disk image is exposed to the guest.
"""
import argparse
import gzip
import json
import lzma
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


def copy_binary(binary, root, destination=None):
    """Keep interpreter/library paths intact, including Nix store paths."""
    binary = Path(binary).absolute()
    target = root / str(destination or binary).lstrip("/")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(binary, target)
    result = subprocess.run(["ldd", str(binary)], capture_output=True, text=True)
    if result.returncode and "not a dynamic executable" not in result.stderr + result.stdout:
        raise RuntimeError(result.stderr + result.stdout)
    for dependency in set(re.findall(r"(/[^\s()]+)", result.stdout)):
        source = Path(dependency)
        target = root / dependency.lstrip("/")
        if target.exists():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


def modules_for(directory, names, root):
    dependencies = {}
    for line in (directory / "modules.dep").read_text().splitlines():
        module, deps = line.split(":", 1)
        dependencies[module] = deps.split()
    ordered = []

    def visit(module):
        if module in ordered:
            return
        for dependency in dependencies[module]:
            visit(dependency)
        ordered.append(module)

    # Crypto algorithms are requested by name at mount time, so modules.dep
    # does not express this btrfs dependency. Prefer the portable provider.
    for provider in ["crc32c-cryptoapi", "crc32c_generic"]:
        if any(Path(p).name.split(".")[0] == provider for p in dependencies):
            names = [provider] + names
            break
    builtin = directory / "modules.builtin"
    builtins = (
        {Path(p).name.split(".")[0] for p in builtin.read_text().splitlines()}
        if builtin.exists() else set()
    )
    for name in names:
        if name in builtins:
            continue
        matches = [p for p in dependencies if Path(p).name.split(".")[0] == name]
        if len(matches) != 1:
            raise RuntimeError(f"expected one module for {name}: {matches}")
        visit(matches[0])
    commands = []
    for module in ordered:
        source = directory / module
        destination = Path("modules") / (source.name.split(".ko")[0] + ".ko")
        content = source.read_bytes()
        if source.suffix == ".xz":
            content = lzma.decompress(content)
        elif source.suffix == ".gz":
            content = gzip.decompress(content)
        elif source.suffix == ".zst":
            content = subprocess.check_output(["zstd", "-d", "-c", str(source)])
        elif source.suffix != ".ko":
            raise RuntimeError(f"unsupported module compression: {source}")
        (root / destination).parent.mkdir(exist_ok=True)
        (root / destination).write_bytes(content)
        commands.append(f"insmod /{destination}")
    return "\n".join(commands)


def validate_guest(serial, returncode, expected_tests):
    """Require the complete test suite, not just a successful guest shutdown."""
    summary = re.search(
        r"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; "
        r"(\d+) measured; (\d+) filtered out;", serial, re.MULTILINE
    )
    if (
        returncode != 0
        or expected_tests <= 0
        or not re.search(r"^DEDUPIT_GUEST_EXIT=0$", serial, re.MULTILINE)
        or "SKIP:" in serial
        or summary is None
        or tuple(map(int, summary.groups())) != (expected_tests, 0, 0, 0, 0)
    ):
        raise RuntimeError("guest must pass every discovery test without skips or ignores")


REQUIRED_BATCH_TESTS = {
    "batch_crashes::failed_file_cleanup_keeps_batch_neighbors_and_cleanup_failure_aborts_the_batch",
    "batch_crashes::portable_batches_recover_uncommitted_spilled_wal_and_keep_committed_files",
    "batch_crashes::native_batches_keep_discovery_cursors_and_hash_acknowledgements_atomic",
    "batch_crashes::apply_batches_replay_only_uncommitted_completions_and_preserve_cow_contents",
}


def validate_listing(listing):
    names = {line.removesuffix(": test") for line in listing.splitlines()
             if line.endswith(": test")}
    missing = REQUIRED_BATCH_TESTS - names
    if missing:
        raise RuntimeError(f"required batch crash tests not compiled: {sorted(missing)}")
    return len(names)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kernel", type=Path, required=True, help="x86_64 bzImage")
    parser.add_argument("--modules", type=Path, required=True,
                        help="matching lib/modules/<kernel-version> directory")
    parser.add_argument("--busybox", required=True, help="static BusyBox executable")
    parser.add_argument("--qemu", default="qemu-system-x86_64")
    parser.add_argument("--cargo", default="cargo")
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--log", type=Path, default=Path("target/btrfs-qemu.log"))
    parser.add_argument("--stress-seed", type=int, help="replay one stress seed (decimal u64)")
    parser.add_argument("--stress-rounds", type=int, default=16,
                        help="mutation/scan rounds per stress seed (1..256)")
    args = parser.parse_args()
    if not 1 <= args.stress_rounds <= 256:
        parser.error("--stress-rounds must be between 1 and 256")
    if args.stress_seed is not None and not 0 <= args.stress_seed < 2**64:
        parser.error("--stress-seed must fit a u64")
    applets = subprocess.check_output([args.busybox, "--list"], text=True).splitlines()
    for required in ["mount", "umount", "insmod", "poweroff", "sh", "grep", "sed"]:
        if required not in applets:
            raise RuntimeError(f"BusyBox lacks required applet: {required}")
    build = subprocess.run([args.cargo, "test", "--locked", "--features", "test-failpoints", "--test", "btrfs_discovery",
                            "--no-run", "--message-format=json"],
                           check=True, stdout=subprocess.PIPE, text=True)
    artifacts = [json.loads(line) for line in build.stdout.splitlines()]
    executables = [a["executable"] for a in artifacts
                   if a.get("executable") and a.get("profile", {}).get("test")]
    if len(executables) != 1:
        raise RuntimeError(f"expected one test executable: {executables}")
    listing = subprocess.check_output([executables[0], "--list", "--format", "terse"], text=True)
    expected_tests = validate_listing(listing)
    args.log.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="dedupit-qemu-") as temporary:
        work = Path(temporary)
        root = work / "root"
        for directory in ["bin", "dev", "proc", "sys", "tmp", "test"]:
            (root / directory).mkdir(parents=True, exist_ok=True)
        copy_binary(args.busybox, root, "/bin/busybox")
        (root / "bin/sh").symlink_to("busybox")
        copy_binary(executables[0], root, "/discovery-tests")
        # Integration tests spawn the actual CLI at Cargo's compiled-in path.
        binaries = [a["executable"] for a in artifacts if a.get("executable")
                    and a.get("target", {}).get("kind") == ["bin"]
                    and a["target"]["name"] == "dedupit" and not a["profile"]["test"]]
        if len(binaries) != 1:
            raise RuntimeError(f"expected one dedupit CLI executable: {binaries}")
        copy_binary(binaries[0], root)
        module_commands = modules_for(args.modules, ["virtio_pci", "virtio_blk", "btrfs"], root)
        init = root / "init"
        stress_environment = f"export DEDUPIT_STRESS_ROUNDS={args.stress_rounds}\n"
        if args.stress_seed is not None:
            stress_environment += f"export DEDUPIT_STRESS_SEED={args.stress_seed}\n"
        init.write_text("""#!/bin/sh
export PATH=/bin
/bin/busybox --install -s /bin
finish() {
    echo "DEDUPIT_GUEST_EXIT=$1"
    sync
    poweroff -f
}
trap 'finish 1' EXIT
set -eu
mount -t devtmpfs devtmpfs /dev
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t tmpfs tmpfs /tmp
uname -r
grep CapEff /proc/self/status
cap=$(sed -n 's/^CapEff:[[:space:]]*//p' /proc/self/status)
[ $((0x$cap & 0x200000)) -ne 0 ]
""" + module_commands + """
for attempt in $(seq 1 30); do
    [ ! -b /dev/vda ] || break
    sleep 1
done
mount -t btrfs -o compress=zstd /dev/vda /test
export TMPDIR=/test DEDUPIT_REQUIRE_BTRFS_SCANNER=1 RUST_BACKTRACE=1
""" + stress_environment + """/discovery-tests --nocapture --test-threads=1
umount /test
trap - EXIT
finish 0
""")
        init.chmod(0o755)
        files = b"\0".join(str(p.relative_to(root)).encode()
                           for p in sorted(root.rglob("*"))) + b"\0"
        initrd = work / "initrd.cpio"
        with initrd.open("wb") as output:
            subprocess.run(["cpio", "--null", "-o", "--format=newc", "--owner=0:0"],
                           input=files, cwd=root, stdout=output, check=True)
        disk = work / "btrfs.img"
        with disk.open("wb") as image:
            image.truncate(1024 * 1024 * 1024)
        subprocess.run(["mkfs.btrfs", "-f", str(disk)], check=True)
        command = [args.qemu, "-accel", "tcg", "-m", "1024", "-smp", "2",
                   "-display", "none", "-serial", "stdio", "-monitor", "none",
                   "-nic", "none", "-no-reboot", "-kernel", str(args.kernel),
                   "-initrd", str(initrd), "-append", "console=ttyS0 rdinit=/init panic=-1",
                   "-drive", f"file={disk},format=raw,if=virtio"]
        print("Running QEMU with TCG; serial output:", args.log, flush=True)
        with args.log.open("w") as log:
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT,
                                    timeout=args.timeout)
        serial = args.log.read_text()
        start = serial.find("running ")
        print(serial[start:] if start != -1 else serial[-6000:])
        try:
            validate_guest(serial, result.returncode, expected_tests)
        except RuntimeError as error:
            raise SystemExit(f"{error}; inspect {args.log}") from error


if __name__ == "__main__":
    main()
