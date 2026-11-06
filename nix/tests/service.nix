{ pkgs }:
pkgs.testers.runNixOSTest {
  name = "dedupit-native-service";
  nodes.machine = { lib, ... }: {
    imports = [ ../module.nix ];
    virtualisation.emptyDiskImages = [ 512 ];
    services.dedupit = {
      enable = true;
      device = "/dev/vdb";
      interval = 1;
      minimumFreeBytes = 0;
    };
    # Format and populate only the disposable test disk before starting dedupit.
    systemd.services.dedupit.wantedBy = lib.mkForce [ ];
    environment.systemPackages = [ pkgs.btrfs-progs pkgs.e2fsprogs ];
  };
  testScript = ''
    import json
    start_all()
    machine.wait_for_unit("multi-user.target")
    machine.succeed("mkfs.btrfs /dev/vdb")
    machine.succeed("mkdir -p /mnt/fixture && mount /dev/vdb /mnt/fixture")
    machine.succeed("btrfs subvolume create /mnt/fixture/one")
    machine.succeed("btrfs subvolume create /mnt/fixture/two")
    machine.succeed("dd if=/dev/urandom of=/mnt/fixture/one/a bs=1M count=2 status=none")
    machine.succeed("cp --reflink=never /mnt/fixture/one/a /mnt/fixture/two/b")
    # A large alias set overflows INO_PATHS and exercises regular/extended
    # inode references. The scoped alias must survive partial-path omission.
    machine.succeed("mkdir /mnt/fixture/one/aliases /mnt/fixture/one/scoped")
    for i in range(200):
        name = f"link-{i:04d}-" + "x" * 180
        machine.succeed(f"ln /mnt/fixture/one/a /mnt/fixture/one/aliases/{name}")
    machine.succeed("ln /mnt/fixture/one/a /mnt/fixture/one/scoped/only")
    machine.succeed("sync")
    scoped = json.loads(machine.succeed("dedupit --database /tmp/scoped.sqlite --scanner btrfs --json /mnt/fixture/one/scoped"))
    assert scoped["files_hashed"] == 1, scoped
    assert scoped["errors"] == [], scoped
    machine.succeed("systemctl start dedupit.service")
    machine.wait_until_succeeds("dedupit stats --json | grep -q '\"completed_runs\": [1-9]'")
    report = json.loads(machine.succeed("dedupit stats --json"))
    assert report["status"]["totals"]["bytes_deduped"] >= 2 * 1024 * 1024, report
    assert report["last_result"]["btrfs_subvolumes"] >= 3, report
    assert report["last_result"]["errors"] == [], report
    assert report["status"]["activity"]["estimated_bytes_saved"] >= 2 * 1024 * 1024, report
    assert report["status"]["activity"]["savings_measured_bytes"] > 0, report
    machine.wait_until_succeeds("dedupit stats --json | grep -q '\"saved_uncompressed_bytes\": [1-9]'")
    current = json.loads(machine.succeed("dedupit stats --json"))["sharing_inventory"]
    best = current["estimate"] or current["refresh"]
    # The prior hardlinks existed before dedupit. The current inventory must
    # include their sharing, rather than merely sum this tool's accepted work.
    assert best["saved_uncompressed_bytes"] >= 200 * 2 * 1024 * 1024, current
    assert best["checked_file_bytes"] > 0 and best["verified_files"] > 0, current
    machine.fail("mountpoint /run/dedupit/filesystem")
    machine.succeed("cmp /mnt/fixture/one/a /mnt/fixture/two/b")
    machine.succeed("systemctl stop dedupit.service")
    machine.succeed("test ! -e /run/dedupit/filesystem")
    previous = report["status"]["totals"]["completed_runs"]
    machine.succeed("systemctl start dedupit.service")
    machine.wait_until_succeeds("dedupit stats --json | grep -q '\"state\": \"completed\"'")
    report = json.loads(machine.succeed("dedupit stats --json"))
    assert report["status"]["totals"]["completed_runs"] > previous, report
    assert report["last_result"]["bytes_hashed"] == 0, report
    machine.succeed("systemctl stop dedupit.service")
    # Compressed copies should report encoded allocation sizes, not their
    # much larger logical lengths. Run outside the service's periodic loop.
    machine.succeed("mkdir /mnt/fixture/compressed")
    machine.succeed("btrfs property set /mnt/fixture/compressed compression zstd")
    machine.succeed("head -c 4194304 /dev/zero | base64 > /mnt/fixture/compressed/a")
    machine.succeed("cp --reflink=never /mnt/fixture/compressed/a /mnt/fixture/compressed/b && sync")
    compressed = json.loads(machine.succeed("dedupit --database /tmp/compressed.sqlite --scanner btrfs --granularity file --apply --json /mnt/fixture/compressed"))
    assert compressed["errors"] == [], compressed
    assert 0 < compressed["estimated_bytes_saved"] < compressed["bytes_deduped"] // 2, compressed
    assert compressed["savings_measured_bytes"] > 0, compressed
    # An external reflink retains the destination's old allocation, so sharing
    # that destination must not be attributed as reclaimed physical space.
    machine.succeed("mkdir -p /mnt/fixture/held/data")
    machine.succeed("dd if=/dev/urandom of=/mnt/fixture/held/data/a bs=1M count=1 status=none")
    machine.succeed("cp --reflink=never /mnt/fixture/held/data/a /mnt/fixture/held/data/b")
    machine.succeed("cp --reflink=always /mnt/fixture/held/data/b /mnt/fixture/held/keep && sync")
    held = json.loads(machine.succeed("dedupit --database /tmp/held.sqlite --scanner btrfs --granularity file --apply --json /mnt/fixture/held/data"))
    assert held["errors"] == [], held
    assert held["bytes_deduped"] > 0, held
    assert held["estimated_bytes_saved"] == 0, held
    assert held["savings_measured_bytes"] > 0, held
    # Equal content across COW/NOCOW classes must be skipped, while equal
    # content within each class still shares without changing file policies.
    machine.succeed("mkdir -p /mnt/fixture/mixed/nocow /mnt/fixture/mixed/cow")
    machine.succeed("chattr +C /mnt/fixture/mixed/nocow")
    machine.succeed("dd if=/dev/urandom of=/mnt/fixture/mixed/nocow/a bs=4096 count=16 status=none")
    machine.succeed("cp --reflink=never /mnt/fixture/mixed/nocow/a /mnt/fixture/mixed/nocow/b")
    machine.succeed("cp --reflink=never /mnt/fixture/mixed/nocow/a /mnt/fixture/mixed/cow/a")
    machine.succeed("cp --reflink=never /mnt/fixture/mixed/nocow/a /mnt/fixture/mixed/cow/b && sync")
    mixed = json.loads(machine.succeed("dedupit --database /tmp/mixed.sqlite --scanner btrfs --granularity file --apply --json /mnt/fixture/mixed"))
    assert mixed["errors"] == [], mixed
    assert mixed["jobs_completed"] >= 2, mixed
    assert mixed["incompatible_ranges"] >= 1, mixed
    machine.succeed("cmp /mnt/fixture/mixed/nocow/a /mnt/fixture/mixed/cow/b")
    machine.succeed("lsattr /mnt/fixture/mixed/nocow/a | grep -q C")
    machine.fail("lsattr /mnt/fixture/mixed/cow/a | cut -d ' ' -f1 | grep -q C")

  '';
}
