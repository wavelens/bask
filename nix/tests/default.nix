/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

{ self, inputs, pkgs, ... }:
let
  inherit (inputs) nixpkgs;
  inherit (nixpkgs) lib;

  pythonExec = name: file: pkgs.writers.writePython3 name {
    libraries = [ self.packages.${pkgs.stdenv.hostPlatform.system}.default ];
    doCheck = false;
  } file;

  removePy = name: builtins.replaceStrings [ ".py" ] [ "" ] name;

  python-examples = builtins.attrNames (lib.filterAttrs (name: type: type == "regular" && lib.hasSuffix ".py" name) (builtins.readDir ../../crates/bask-python/examples));
in builtins.listToAttrs (map (example-file: lib.nameValuePair (removePy example-file) (pkgs.runCommand "${removePy example-file}-test" { } (let
  testExec = pythonExec (removePy example-file) ../../crates/bask-python/examples/${example-file};
in ''
  ${testExec}
  echo "Test ${removePy example-file} passed" | tee -a $out
''
))) python-examples)
