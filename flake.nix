{
  description = "Incremental deduplication with a native service and NixOS module";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      nixosModules.default = import ./nix/module.nix;
      nixosModules.dedupit = self.nixosModules.default;
      packages = forSystems (system:
        let pkgs = import nixpkgs { inherit system; }; in
        rec {
          dedupit = pkgs.callPackage ./nix/package.nix { src = self; };
          default = dedupit;
        });
      checks = forSystems (system: { inherit (self.packages.${system}) dedupit; }
        // nixpkgs.lib.optionalAttrs (system == "x86_64-linux") {
        service = import ./nix/tests/service.nix { pkgs = nixpkgs.legacyPackages.${system}; };
      });
      devShells = forSystems (system:
        let pkgs = import nixpkgs { inherit system; }; in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [ cargo rustc rustfmt clippy ];
          };
        });
      formatter = forSystems (system: nixpkgs.legacyPackages.${system}.nixpkgs-fmt);
    };
}
