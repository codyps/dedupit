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
    environment.systemPackages = [ pkgs.btrfs-progs pkgs.e2fsprogs pkgs.sqlite ];
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
    # Keep the live index on the scanned volume, reachable through a different
    # mount path. Its aliases must be rejected before open/close can drop locks.
    machine.succeed("mkdir -p /mnt/fixture/state /var/lib/dedupit; mount --bind /mnt/fixture/state /var/lib/dedupit")
    machine.succeed("systemctl start dedupit.service")
    machine.wait_until_succeeds("dedupit stats --json | grep -q '\"completed_runs\": [1-9]'")
    # Exercise concurrent external SQLite clients across active/idle reader and
    # writer handoffs. Previously scanning SHM aliases discarded POSIX locks.
    machine.succeed("for i in $(seq 1 40); do sqlite3 -readonly -cmd '.timeout 2000' /var/lib/dedupit/filesystem.sqlite 'SELECT count(*) FROM files' >/dev/null; sleep 0.05; done")
    machine.succeed("systemctl is-active dedupit.service")
    assert machine.succeed("systemctl show dedupit.service -p NRestarts --value").strip() == "0"
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
    assert best["incremental"] and best["bootstrap_complete"], current
    # Let an external hardlink change pass through native Btrfs discovery into
    # the inventory. It must replace current totals, not accumulate history.
    machine.succeed("ln /mnt/fixture/two/b /mnt/fixture/two/extra; sync")
    def inventory_saved():
        value = json.loads(machine.succeed("dedupit stats --json"))["sharing_inventory"]
        return (value["estimate"] or value["refresh"])["saved_uncompressed_bytes"]
    machine.wait_until_succeeds("sqlite3 -readonly -cmd '.timeout 2000' /var/lib/dedupit/filesystem.sqlite \"SELECT count(*) FROM inventory_verified WHERE links=2\" | grep -q '[1-9]'")
    # Inventory groups are independently durable; allow their bounded reducer.
    import time
    for _ in range(90):
        if inventory_saved() >= best["saved_uncompressed_bytes"] + 2 * 1024 * 1024:
            break
        time.sleep(1)
    else:
        raise AssertionError("hardlink change did not update current savings")
    machine.succeed("rm /mnt/fixture/two/extra; sync")
    machine.fail("mountpoint /run/dedupit/filesystem")
    machine.succeed("cmp /mnt/fixture/one/a /mnt/fixture/two/b")
    machine.succeed("systemctl stop dedupit.service")
    machine.succeed("test ! -e /run/dedupit/filesystem")
    previous = report["status"]["totals"]["completed_runs"]
    machine.succeed("systemctl start dedupit.service")
    machine.wait_until_succeeds("dedupit stats --json | grep -q '\"state\": \"completed\"'")
    report = json.loads(machine.succeed("dedupit stats --json"))
    assert report["status"]["totals"]["completed_runs"] > previous, report
    # The database now lives in scope, so its small mutable JSON reports are
    # legitimate new work. The unchanged multi-MiB fixtures must remain cached.
    assert report["last_result"]["bytes_hashed"] < 65536, report
    assert report["last_result"]["files_unchanged"] >= 2, report
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
    # Index in an explicit order, then apply. All writable copies must converge
    # on the immutable donor even when some were already deduplicated earlier.
    for readonly_first in [False, True]:
        case = "/mnt/fixture/readonly-" + str(readonly_first)
        db = "/tmp/readonly-" + str(readonly_first) + ".sqlite"
        machine.succeed(f"mkdir -p {case}")
        if not readonly_first:
            machine.succeed(f"dd if=/dev/urandom of={case}/a bs=4096 count=16 status=none; cp --reflink=never {case}/a {case}/b; sync")
            machine.succeed(f"dedupit --database {db} --scanner btrfs --granularity file --apply --json {case}")
            machine.succeed(f"btrfs subvolume create {case}/readonly; cp --reflink=never {case}/a {case}/readonly/copy; sync; btrfs property set -t s {case}/readonly ro true")
        else:
            machine.succeed(f"btrfs subvolume create {case}/readonly; dd if=/dev/urandom of={case}/readonly/copy bs=4096 count=16 status=none; btrfs property set -t s {case}/readonly ro true")
            machine.succeed(f"dedupit --database {db} --scanner btrfs --granularity file --json {case}")
            machine.succeed(f"cp --reflink=never {case}/readonly/copy {case}/a; cp --reflink=never {case}/a {case}/b; sync")
        readonly = json.loads(machine.succeed(f"dedupit --database {db} --scanner btrfs --granularity file --apply --json {case}"))
        assert readonly["errors"] == [], readonly
        assert readonly["jobs_completed"] >= 2, readonly
        machine.succeed(f"cmp {case}/a {case}/readonly/copy; cmp {case}/b {case}/readonly/copy")
        machine.succeed(f"btrfs property get -t s {case}/readonly ro | grep -q 'ro=true'")
        # FIEMAP exposes the actual shared physical allocation, independently of
        # counters or comparisons of file contents.
        mappings = machine.succeed(f"filefrag -v {case}/a {case}/b {case}/readonly/copy")
        extents = [line.split()[3] for line in mappings.splitlines() if line.strip().startswith("0:")]
        assert len(extents) == 3 and len(set(extents)) == 1, mappings
        again = json.loads(machine.succeed(f"dedupit --database {db} --scanner btrfs --granularity file --apply --json {case}"))
        assert again["errors"] == [], again

    # A read-only copy in another checksum class must never become that class's
    # cached donor. In particular, repeated scans must terminate without EINVAL.
    machine.succeed("mkdir -p /mnt/fixture/readonly-policy; btrfs subvolume create /mnt/fixture/readonly-policy/ro")
    machine.succeed("chattr +C /mnt/fixture/readonly-policy/ro; dd if=/dev/urandom of=/mnt/fixture/readonly-policy/ro/copy bs=4096 count=16 status=none")
    machine.succeed("cp --reflink=never /mnt/fixture/readonly-policy/ro/copy /mnt/fixture/readonly-policy/a; btrfs property set -t s /mnt/fixture/readonly-policy/ro ro true; sync")
    for _ in range(2):
        policy = json.loads(machine.succeed("timeout 30 dedupit --database /tmp/readonly-policy.sqlite --scanner btrfs --granularity file --apply --json /mnt/fixture/readonly-policy"))
        assert policy["errors"] == [], policy
        assert policy["bytes_deduped"] == 0, policy

    # Offline compaction preserves hashes and supports explicit storage policies.
    maintenance_db = "/mnt/fixture/maintenance.sqlite"
    maintenance_root = "/mnt/fixture/readonly-False"
    machine.succeed(f"dedupit --database {maintenance_db} --scanner btrfs --granularity file --json {maintenance_root}")
    for storage in ["compressed", "hybrid", "nocow"]:
        machine.succeed(f"dedupit compact --database {maintenance_db} --storage {storage}")
        if storage == "nocow":
            machine.succeed(f"lsattr {maintenance_db} | grep -q C")
        else:
            machine.succeed(f"btrfs property get {maintenance_db} compression | grep -q zstd")
            machine.fail(f"lsattr {maintenance_db} | cut -d ' ' -f1 | grep -q C")
        resumed = json.loads(machine.succeed(f"dedupit --database {maintenance_db} --database-storage {storage} --scanner btrfs --granularity file --apply --json {maintenance_root}"))
        assert resumed["bytes_hashed"] == 0, resumed
        assert resumed["errors"] == [], resumed

    # Native inline identities must survive an actual v5 upgrade without reading
    # file data again, and still detect changes beyond any stored byte prefix.
    inline_root = "/mnt/fixture/inline-case"
    inline_db = "/tmp/inline.sqlite"
    machine.succeed(f"mkdir {inline_root}; printf '%1800s' x > {inline_root}/a; cp --reflink=never {inline_root}/a {inline_root}/b; sync")
    inline_command = f"dedupit --database {inline_db} --scanner btrfs --granularity both --chunk-size 4096 --json {inline_root}"
    initial = json.loads(machine.succeed(inline_command))
    assert initial["duplicate_files"] == 1, initial
    assert machine.succeed(f"sqlite3 {inline_db} 'SELECT max(length(descriptor)) FROM file_extents'").strip() == "53"
    # Released v5 had no derived incremental inventory tables or triggers.
    import shlex
    inventory_objects = "SELECT type||' '||name FROM sqlite_master WHERE name LIKE 'inventory_%' ORDER BY CASE type WHEN 'trigger' THEN 0 WHEN 'index' THEN 1 ELSE 2 END"
    for object_name in machine.succeed(f"sqlite3 {inline_db} {shlex.quote(inventory_objects)}").splitlines():
        machine.succeed(f"sqlite3 {inline_db} {shlex.quote('DROP ' + object_name)}")
    # Reconstruct the real v5 duplicated cache and full inline payloads.
    downgrade = "PRAGMA foreign_keys=OFF; BEGIN; CREATE TABLE chunk_cache_v5(file_id INTEGER REFERENCES files(id) ON DELETE CASCADE,offset INTEGER,length INTEGER,hash BLOB,nonzero INTEGER,PRIMARY KEY(file_id,offset)); INSERT INTO chunk_cache_v5 SELECT * FROM chunk_cache; DROP VIEW chunk_cache; DROP TABLE chunk_cache_extra; DROP TRIGGER invalidate_extent_baseline_range; ALTER TABLE chunk_cache_v5 RENAME TO chunk_cache; CREATE TRIGGER invalidate_extent_baseline AFTER DELETE ON chunk_cache BEGIN DELETE FROM extent_state WHERE file_id=OLD.file_id; END; UPDATE file_extents SET descriptor=CAST(substr(descriptor,1,21)||printf('%1799s',char())||'x' AS BLOB); PRAGMA user_version=5; COMMIT;"
    import shlex
    machine.succeed(f"sqlite3 {inline_db} {shlex.quote(downgrade)}")
    machine.succeed(f"dedupit compact --database {inline_db} --storage cow")
    resumed = json.loads(machine.succeed(inline_command))
    assert resumed["bytes_hashed"] == 0, resumed
    assert resumed["duplicate_files"] == 1, resumed
    machine.succeed(f"touch {inline_root}/a; sync")
    metadata_only = json.loads(machine.succeed(inline_command))
    assert metadata_only["bytes_hashed"] == 0, metadata_only
    machine.succeed(f"printf y | dd of={inline_root}/a bs=1 seek=1799 conv=notrunc status=none; sync")
    edited = json.loads(machine.succeed(inline_command))
    assert edited["bytes_hashed"] == 1800, edited
    assert edited["duplicate_files"] == 0, edited
    assert machine.succeed(f"sqlite3 {inline_db} 'SELECT count(*) FROM chunk_cache_extra'").strip() == "0"

  '';
}
