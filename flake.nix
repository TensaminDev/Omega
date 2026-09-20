{
  description = "Omega";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    iota = {
      url = "git+https://git.methanium.net/tensamin/iota";
      flake = false;
    };
    mtp-type-maps = {
      url = "git+https://git.methanium.net/tensamin/mtp-type-maps";
      flake = false;
    };
  };

  outputs =
    inputs@{
      self,
      nixpkgs,
      flake-parts,
      rust-overlay,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      perSystem =
        {
          self',
          pkgs,
          system,
          ...
        }:
        let
          rustPkgs = import nixpkgs {
            inherit system;
            overlays = [ (import rust-overlay) ];
          };
          rustToolchain = rustPkgs.rust-bin.stable.latest.default.override {
            extensions = [
              "rust-src"
              "rust-analyzer"
              "clippy"
              "rustfmt"
            ];
          };
        in
        {
          packages = {
            default = self'.packages.omega;
            omega = pkgs.rustPlatform.buildRustPackage {
              pname = "omega";
              version = "0.1.0";
              src = ./.;
              postPatch = ''
                cp -R ${inputs.iota}/iota-identity ./iota-identity
                substituteInPlace Cargo.toml \
                  --replace-fail 'path = "../iota/iota-identity"' 'path = "./iota-identity"'
                substituteInPlace iota-identity/Cargo.toml \
                  --replace-fail 'bb0f682b735de5ebb36bb41dc699260578341828' '1f19a0d897c265d1e3f590a876f95e766ff99318'
              '';
              cargoLock = {
                lockFile = ./Cargo.lock;
                allowBuiltinFetchGit = true;
              };
              nativeBuildInputs = with pkgs; [
                cmake
                perl
                pkg-config
              ];
              buildInputs = with pkgs; [
                openssl
                libmysqlclient
              ];
              MTP_TYPE_MAPS = "${inputs.mtp-type-maps}/type-maps.yaml";
              dontUseCmakeConfigure = true;
            };
          };

          devShells.default = pkgs.mkShell {
            nativeBuildInputs = with pkgs; [
              rustToolchain
              git
              cmake
              perl
              pkg-config
            ];
            buildInputs = with pkgs; [
              openssl
              libmysqlclient
            ];
          };
        };

      flake = {
        nixosModules.default =
          {
            config,
            pkgs,
            lib,
            ...
          }:
          let
            cfg = config.services.omega;
            defaultPackage =
              self.packages.${pkgs.stdenv.hostPlatform.system}.default
                or (throw "omega: no pre-built package for system ${pkgs.stdenv.hostPlatform.system}");
          in
          {
            options.services.omega = {
              enable = lib.mkEnableOption "Omega, Tensamin's central server";

              dataDir = lib.mkOption {
                type = lib.types.str;
                default = "/var/lib/omega";
                description = "Directory where Omega stores its data and reads certificates from.";
              };

              certFile = lib.mkOption {
                type = lib.types.str;
                description = "Runtime path to the TLS certificate.";
              };

              keyFile = lib.mkOption {
                type = lib.types.str;
                description = "Runtime path to the TLS private key.";
              };

              environmentFiles = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
                description = ''
                  Environment files to load for the Omega service.
                  Must provide at least DB_URL, PRIVATE_KEY, and PUBLIC_KEY.
                '';
              };

              port = lib.mkOption {
                type = lib.types.port;
                default = 443;
                description = "TCP and UDP port for Omega's combined listener.";
              };

              identityFile = lib.mkOption {
                type = lib.types.nullOr lib.types.str;
                default = null;
                description = "Runtime path to an existing Omega private keyring, or null to generate one.";
              };

              publicIdentityFile = lib.mkOption {
                type = lib.types.nullOr lib.types.str;
                default = null;
                description = "Runtime path to Omega's matching public key bundle.";
              };

              bindAddress = lib.mkOption {
                type = lib.types.str;
                default = "0.0.0.0";
                description = "IP address to bind the MTP/QUIC server to.";
              };

              openFirewall = lib.mkOption {
                type = lib.types.bool;
                default = true;
                description = "Whether to open the firewall for ports used by Omega.";
              };

              package = lib.mkOption {
                type = lib.types.package;
                default = defaultPackage;
                description = "The Omega package to use.";
              };
            };

            config = lib.mkIf cfg.enable {
              users.users.omega = {
                isSystemUser = true;
                group = "omega";
                home = cfg.dataDir;
                createHome = true;
                description = "Omega service user";
              };

              users.groups.omega = { };

              systemd.services.omega = {
                description = "Tensamin Omega";
                wantedBy = [ "multi-user.target" ];
                after = [ "network.target" ];

                environment = {
                  PORT = toString cfg.port;
                  BIND_ADDRESS = cfg.bindAddress;
                };

                serviceConfig = {
                  Type = "simple";
                  User = "omega";
                  Group = "omega";
                  WorkingDirectory = cfg.dataDir;
                  ExecStart = "${cfg.package}/bin/omega";
                  Restart = "always";
                  RestartSec = "5s";

                  EnvironmentFile = cfg.environmentFiles;

                  AmbientCapabilities = [ "CAP_NET_BIND_SERVICE" ];
                  CapabilityBoundingSet = [ "CAP_NET_BIND_SERVICE" ];

                  ExecStartPre = [
                    (
                      "+"
                      + pkgs.writeShellScript "omega-setup-certs" ''
                        install -d -m 0700 -o omega -g omega ${cfg.dataDir}/certs
                        install -m 0644 -o omega -g omega ${cfg.certFile} ${cfg.dataDir}/certs/cert.pem
                        ${pkgs.openssl}/bin/openssl pkcs8 -topk8 -nocrypt \
                          -in ${cfg.keyFile} \
                          -out ${cfg.dataDir}/certs/key.pem
                        chown omega:omega ${cfg.dataDir}/certs/key.pem
                        chmod 0600 ${cfg.dataDir}/certs/key.pem
                        ${lib.optionalString (cfg.identityFile != null) ''
                          install -m 0600 -o omega -g omega ${cfg.identityFile} ${cfg.dataDir}/omega.mk
                        ''}
                        ${lib.optionalString (cfg.publicIdentityFile != null) ''
                          install -m 0644 -o omega -g omega ${cfg.publicIdentityFile} ${cfg.dataDir}/omega.mpkb
                        ''}
                      ''
                    )
                  ];

                  ProtectSystem = "strict";
                  ProtectHome = true;
                  PrivateTmp = true;
                  NoNewPrivileges = true;
                  ReadWritePaths = cfg.dataDir;
                  ProtectKernelTunables = true;
                  ProtectKernelModules = true;
                  ProtectControlGroups = true;
                  RestrictRealtime = true;
                  RestrictSUIDSGID = true;
                  LockPersonality = true;
                  MemoryDenyWriteExecute = true;
                };
              };

              networking.firewall = lib.mkIf cfg.openFirewall {
                allowedTCPPorts = [ cfg.port ];
                allowedUDPPorts = [ cfg.port ];
              };
            };
          };
      };
    };
}
