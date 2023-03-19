let
  #pkgs = import (fetchTarball("https://github.com/NixOS/nixpkgs/archive/048958000f08aebb9ff0350a5b09d8f8a17c674e.tar.gz")) {};
  #pkgs = import (fetchTarball("channel:nixos-22.11")) {};
  pkgs = import (fetchTarball("channel:nixpkgs-unstable")) {};
in pkgs.clangStdenv.mkDerivation {
  name = "wora";
  buildInputs = [ pkgs.cargo pkgs.rustc pkgs.libstatgrab pkgs.libclang
  pkgs.pkg-config pkgs.clippy ];
  LIBCLANG_PATH = "${pkgs.llvmPackages_11.libclang.lib}/lib";
}
