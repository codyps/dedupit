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
    environment.systemPackages = [ pkgs.btrfs-progs ];
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
  '';
}
