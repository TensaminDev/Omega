{
  description = "Omega";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    ttp = {
      url = "git+https://git.methanium.net/Tensamin/TTP.git";
      flake = false;
    };
  };

  outputs = inputs@{ self, nixpkgs, flake-parts, rust-overlay, ttp, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      perSystem = { self', pkgs, system, ... }:
        let
          rustPkgs = import nixpkgs {
            inherit system;
            overlays = [ (import rust-overlay) ];
          };
          rustToolchain = rustPkgs.rust-bin.stable.latest.default.override {
            extensions = [ "rust-src" "rust-analyzer" "clippy" "rustfmt" ];
          };
        in
        {
          packages = {
            default = self'.packages.omega;
            omega = pkgs.rustPlatform.buildRustPackage {
              pname = "omega";
              version = "0.1.0";
              src = ./.;
              cargoLock = {
                lockFile = ./Cargo.lock;
                allowBuiltinFetchGit = true;
              };
              nativeBuildInputs = with pkgs; [ cmake perl pkg-config ];
              buildInputs = with pkgs; [ openssl libmysqlclient ];
              dontUseCmakeConfigure = true;
              preConfigure = ''
                if [ -d ../cargo-vendor-dir/ttp-core-0.1.0 ]; then
                  cp ${ttp}/ttp-codec.json ../cargo-vendor-dir/ttp-codec.json
                fi
              '';
            };
          };

          devShells.default = pkgs.mkShell {
            nativeBuildInputs = with pkgs; [ rustToolchain git cmake perl pkg-config ];
            buildInputs = with pkgs; [ openssl libmysqlclient ];
          };
        };

      flake = {
        nixosModules.default = { config, pkgs, lib, ... }:
          let
            cfg = config.services.omega;
            defaultPackage = self.packages.${pkgs.stdenv.hostPlatform.system}.default or (throw "omega: no pre-built package for system ${pkgs.stdenv.hostPlatform.system}");
          in
          {
            options.services.omega = {
              enable = lib.mkEnableOption "Omega, Tensamin's central server";

              dataDir = lib.mkOption {
                type = lib.types.str;
                default = "/var/lib/omega";
                description = "Directory where Omega stores its data and reads certificates from.";
              };

              apiCertFile = lib.mkOption {
                type = lib.types.path;
                description = "Path to the SSL certificate file for the web/API server (e.g. ACME fullchain.pem). Copied to server_cert.pem at runtime.";
              };

              apiKeyFile = lib.mkOption {
                type = lib.types.path;
                description = "Path to the SSL private key file for the web/API server (e.g. ACME key.pem). Converted to PKCS#8 and copied to server_key.pem at runtime.";
              };

              transportCertFile = lib.mkOption {
                type = lib.types.path;
                description = "Path to the SSL certificate file for the TTP/QUIC transport (e.g. ACME fullchain.pem). Copied to transport_cert.pem at runtime.";
              };

              transportKeyFile = lib.mkOption {
                type = lib.types.path;
                description = "Path to the SSL private key file for the TTP/QUIC transport (e.g. ACME key.pem). Converted to PKCS#8 and copied to transport_key.pem at runtime.";
              };

              environmentFiles = lib.mkOption {
                type = lib.types.listOf lib.types.path;
                default = [ ];
                description = ''
                  Environment files to load for the Omega service.
                  Must provide at least DB_URL, PRIVATE_KEY, and PUBLIC_KEY.
                '';
              };

              apiPort = lib.mkOption {
                type = lib.types.port;
                default = 9188;
                description = "Port for the web/API server.";
              };

              transportPort = lib.mkOption {
                type = lib.types.port;
                default = 9187;
                description = "Port for the TTP/QUIC transport server.";
              };

              ttpBind = lib.mkOption {
                type = lib.types.str;
                default = "0.0.0.0";
                description = "IP address to bind the TTP/QUIC transport server to.";
              };

              bindAddress = lib.mkOption {
                type = lib.types.str;
                default = "0.0.0.0";
                description = "IP address to bind the HTTP/API server to.";
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

                serviceConfig = {
                  Type = "simple";
                  User = "omega";
                  Group = "omega";
                  WorkingDirectory = cfg.dataDir;
                  ExecStart = "${cfg.package}/bin/omega";
                  Restart = "always";
                  RestartSec = "5s";

                  Environment = [
                    "API_PORT=${toString cfg.apiPort}"
                    "OMIKRON_PORT=${toString cfg.transportPort}"
                    "TTP_BIND=${cfg.ttpBind}"
                    "BIND_ADDRESS=${cfg.bindAddress}"
                  ];

                  EnvironmentFile = cfg.environmentFiles;

                  AmbientCapabilities = [ "CAP_NET_BIND_SERVICE" ];
                  CapabilityBoundingSet = [ "CAP_NET_BIND_SERVICE" ];

                  ExecStartPre = [
                    ("+" + pkgs.writeShellScript "omega-setup-certs" ''
                      mkdir -p ${cfg.dataDir}/certs
                      cp ${cfg.apiCertFile} ${cfg.dataDir}/certs/server_cert.pem
                      ${pkgs.openssl}/bin/openssl pkcs8 -topk8 -nocrypt \
                        -in ${cfg.apiKeyFile} \
                        -out ${cfg.dataDir}/certs/server_key.pem
                      cp ${cfg.transportCertFile} ${cfg.dataDir}/certs/transport_cert.pem
                      ${pkgs.openssl}/bin/openssl pkcs8 -topk8 -nocrypt \
                        -in ${cfg.transportKeyFile} \
                        -out ${cfg.dataDir}/certs/transport_key.pem
                      chown -R omega:omega ${cfg.dataDir}
                    '')
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
                allowedTCPPorts = [ cfg.transportPort cfg.apiPort ];
                allowedUDPPorts = [ cfg.transportPort cfg.apiPort ];
              };
            };
          };
      };
    };
}
