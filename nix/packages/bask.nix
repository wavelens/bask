/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

{ lib
, rustPlatform
, python3
}: let
  cargoToml = builtins.fromTOML (builtins.readFile ../../Cargo.toml);
in python3.pkgs.buildPythonPackage {
  pname = "bask";
  version = cargoToml.workspace.package.version;
  pyproject = true;

  src = lib.cleanSource ../..;

  cargoDeps = rustPlatform.importCargoLock {
    lockFile = ../../Cargo.lock;
  };

  buildAndTestSubdir = "crates/bask-python";

  nativeBuildInputs = [
    rustPlatform.cargoSetupHook
    rustPlatform.maturinBuildHook
  ];

  pythonImportsCheck = [ "bask" ];

  meta = {
    license = with lib.licenses; [ mit asl20 ];
    platforms = lib.platforms.unix;
  };
}
