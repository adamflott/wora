let
  pkgs = import (fetchTarball("channel:nixpkgs-unstable")) {};
in pkgs.clangStdenv.mkDerivation {
  name = "wora";
  buildInputs = [ pkgs.cargo pkgs.rustc pkgs.libclang pkgs.pkg-config pkgs.clippy ];
  LIBCLANG_PATH = "${pkgs.llvmPackages_16.libclang.lib}/lib";
}
