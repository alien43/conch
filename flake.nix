{
  description = "Conch - tiny etcd coordination suite";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        packages.default = pkgs.buildGoModule {
          pname = "conch";
          version = "0.1.1";
          src = ./.;
          vendorHash = "sha256-VvkBKlLwDe6wnWebQQLx4WwyBwSRPhc5qyVqTeq44Ak=";
          subPackages = [ "cmd/conch" ];
        };

        devShells.default = pkgs.mkShell {
          packages = [ pkgs.go pkgs.etcd ];
        };
      }
    );
}
