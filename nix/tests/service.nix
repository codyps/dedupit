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
    machine.succeed("mkdir /mnt/fixture && mount /dev/vdb /mnt/fixture")
    machine.succeed("btrfs subvolume create /mnt/fixture/one")
    machine.succeed("btrfs subvolume create /mnt/fixture/two")
    machine.succeed("dd if=/dev/urandom of=/mnt/fixture/one/a bs=1M count=2 status=none")
    machine.succeed("cp --reflink=never /mnt/fixture/one/a /mnt/fixture/two/b")
    machine.succeed("sync")
    machine.succeed("systemctl start dedupit.service")
    machine.wait_until_succeeds("dedupit stats --json | grep -q '\"completed_runs\": [1-9]'")
    report = json.loads(machine.succeed("dedupit stats --json"))
    assert report["status"]["totals"]["bytes_deduped"] >= 2 * 1024 * 1024, report
    assert report["last_result"]["btrfs_subvolumes"] >= 3, report
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
