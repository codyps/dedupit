{ config, lib, pkgs, utils, ... }:
let
  cfg = config.services.dedupit;
  deviceUnit = "${utils.escapeSystemdPath cfg.device}.device";
  args = [
    "service"
    "--state-directory"
    "/var/lib/dedupit"
    "--device"
    cfg.device
    "--mount-directory"
    "/run/dedupit/filesystem"
    "--scanner"
    "btrfs"
    "--chunk-size"
    (toString cfg.chunkSize)
    "--interval"
    (toString cfg.interval)
    "--minimum-free-bytes"
    (toString cfg.minimumFreeBytes)
  ] ++ lib.optional cfg.apply "--apply";
in
{
  options.services.dedupit = {
    enable = lib.mkEnableOption "native incremental whole-volume Btrfs deduplication";
    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.callPackage ./package.nix { src = ../.; };
      description = "Dedupit package providing the native service and statistics commands.";
    };
    device = lib.mkOption {
      type = lib.types.str;
      description = "Btrfs block device. All subvolumes are scanned through a private top-level mount.";
      example = "/dev/mapper/root";
    };
    apply = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = "Share duplicate ranges using kernel verification; false only records duplicate statistics.";
    };
    chunkSize = lib.mkOption {
      type = lib.types.ints.between 4096 16777216;
      default = 1048576;
      description = "Logical chunk size in bytes (must be a power of two). Bound to the persistent index.";
    };
    interval = lib.mkOption {
      type = lib.types.ints.positive;
      default = 3600;
      description = "Seconds after each run or free-space pause before retrying.";
    };
    minimumFreeBytes = lib.mkOption {
      type = lib.types.ints.unsigned;
      default = 5368709120;
      description = "Pause below this much available index-filesystem space, checked at most every 30 seconds during work.";
    };
  };
  config = lib.mkIf cfg.enable {
    assertions = [
      { assertion = lib.hasPrefix "/dev/" cfg.device; message = "services.dedupit.device must be an absolute /dev path."; }
      { assertion = builtins.bitAnd cfg.chunkSize (cfg.chunkSize - 1) == 0; message = "services.dedupit.chunkSize must be a power of two."; }
    ];
    environment.systemPackages = [ cfg.package ];
    systemd.services.dedupit = {
      description = "Incremental whole-volume Btrfs deduplication";
      wantedBy = [ "multi-user.target" ];
      requires = [ deviceUnit ];
      after = [ "local-fs.target" deviceUnit ];
      unitConfig.RequiresMountsFor = [ "/var/lib/dedupit" ];
      serviceConfig = {
        Type = "simple";
        ExecStart = lib.escapeShellArgs ([ "${cfg.package}/bin/dedupit" ] ++ args);
        Restart = "on-failure";
        RestartSec = "5min";
        TimeoutStopSec = "45s";
        User = "root";
        StateDirectory = "dedupit";
        StateDirectoryMode = "0700";
        RuntimeDirectory = "dedupit";
        RuntimeDirectoryMode = "0700";
        UMask = "0077";
        Nice = 19;
        IOSchedulingClass = "idle";
        CPUWeight = 10;
        IOWeight = 10;
        MemoryHigh = "2G";
        MemoryMax = "4G";
        PrivateMounts = true;
        NoNewPrivileges = true;
      };
    };
  };
}
