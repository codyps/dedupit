{ lib, rustPlatform, src ? ../. }:
rustPlatform.buildRustPackage {
  pname = "dedupit";
  version = "0.1.0";
  inherit src;
  cargoLock.lockFile = src + "/Cargo.lock";
  meta = {
    description = "Incremental file and extent deduplication with kernel-verified sharing";
    homepage = "https://github.com/codyps/dedupit";
    license = lib.licenses.mit;
    mainProgram = "dedupit";
    platforms = lib.platforms.unix;
  };
}
