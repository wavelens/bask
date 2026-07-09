/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

{
  description = "Bask";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { nixpkgs, flake-utils, ... }: flake-utils.lib.eachDefaultSystem (system: let
    pkgs = import nixpkgs {
      inherit system;
    };

    rustEnv = with pkgs.rustPackages; [
      clippy
    ];
  in
  {
    packages = rec {
      bask = pkgs.callPackage ./nix/packages/bask.nix { };
      default = bask;
    };

    devShells.default = with pkgs; mkShell {
      buildInputs = [
        stdenv.cc.cc.lib
        pam
      ];

      packages = [
        cargo
        cargo-nextest
        rustc
        rustfmt
        rustEnv
        maturin
        python3
      ];

      RUST_BACKTRACE = 1;
    };
  });
}
