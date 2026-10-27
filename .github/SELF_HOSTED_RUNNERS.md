# Warbler CI runners

All CI and manually dispatched benchmark jobs use `runs-on: warbler-linux`.

Warbler's GARM scale set creates disposable Ubuntu 24.04 x86-64 VMs and scales to
zero when idle. The workflow selects the scale-set name alone, rather than
persistent-runner OS labels. Each Linux job installs its required base tools
before checkout because the cloud image is not GitHub's hosted tool image.

The `codyps-warbler-garm` GitHub App installation must be granted this repository.
GARM also needs a repository registration using the existing `codyps-app`
credential and a `warbler-linux` scale set. See the
[Warbler runner administration guide](https://github.com/codyps/nixos-config/blob/main/hosts/warbler/garm.md).
Jobs queue while the pool is unavailable; there is no GitHub-hosted fallback.

The disposable VM provides passwordless sudo. Tests install QEMU and a guest
kernel, and the native btrfs job needs loop devices, btrfs kernel support, and
permission to mount and unmount its temporary filesystem. These jobs must run
in the GARM VM rather than an unprivileged container. Nested KVM is optional;
QEMU retains its software-emulation fallback.

The all-features test job also mounts a temporary btrfs filesystem for `TMPDIR`.
Its crash-recovery failpoints need successful deduplication; ext4 accepts the
portable scan but cannot reach the sharing failpoint. All test assertions and
feature coverage are retained.
