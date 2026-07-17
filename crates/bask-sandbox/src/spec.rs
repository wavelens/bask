/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

/// Isolation strength the caller declares; `spawn` selects a backend that satisfies it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Isolation {
    Local,
    Container,
}

impl Default for Isolation {
    fn default() -> Self {
        Isolation::Local
    }
}

/// Network exposure inside the sandbox. Defaults to none.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Network {
    #[default]
    None,
    Bridge,
    Host,
}

/// Resource ceilings applied to the sandbox and each exec.
#[derive(Clone, Debug, Default)]
pub struct Limits {
    pub cpus: Option<f32>,
    pub memory_bytes: Option<u64>,
    pub timeout: Option<Duration>,
    pub max_output_bytes: Option<usize>,
}

/// A file written into the sandbox filesystem before the first exec.
#[derive(Clone, Debug)]
pub struct SeedFile {
    pub path: PathBuf,
    pub contents: Vec<u8>,
    pub mode: u32,
}

/// Full description of a sandbox to spawn.
#[derive(Clone, Debug)]
pub struct SandboxSpec {
    pub isolation: Isolation,
    pub image: Option<String>,
    pub workdir: PathBuf,
    pub env: BTreeMap<String, String>,
    pub network: Network,
    pub limits: Limits,
    pub seed_files: Vec<SeedFile>,
}

impl Default for SandboxSpec {
    fn default() -> Self {
        SandboxSpec {
            isolation: Isolation::Local,
            image: None,
            workdir: PathBuf::from("/work"),
            env: BTreeMap::new(),
            network: Network::None,
            limits: Limits::default(),
            seed_files: Vec::new(),
        }
    }
}

/// One command to run in the sandbox. `command` is argv; there is no implicit shell.
#[derive(Clone, Debug)]
pub struct ExecRequest {
    pub command: Vec<String>,
    pub stdin: Option<Vec<u8>>,
    pub timeout: Option<Duration>,
}

impl ExecRequest {
    pub fn new(command: Vec<String>) -> Self {
        ExecRequest {
            command,
            stdin: None,
            timeout: None,
        }
    }
}

/// Captured result of one exec. A timeout yields `exit_code == 124`.
#[derive(Clone, Debug)]
pub struct ExecResult {
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub truncated: bool,
    pub duration: Duration,
}

pub(crate) fn truncate(mut bytes: Vec<u8>, max: Option<usize>) -> (Vec<u8>, bool) {
    match max {
        Some(limit) if bytes.len() > limit => {
            bytes.truncate(limit);
            (bytes, true)
        }
        _ => (bytes, false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_defaults_are_local_and_no_network() {
        let spec = SandboxSpec::default();
        assert_eq!(spec.isolation, Isolation::Local);
        assert_eq!(spec.network, Network::None);
        assert!(spec.image.is_none());
    }

    #[test]
    fn truncate_caps_and_flags() {
        let (out, cut) = truncate(vec![1, 2, 3, 4], Some(2));
        assert_eq!(out, vec![1, 2]);
        assert!(cut);
        let (out, cut) = truncate(vec![1, 2], Some(8));
        assert_eq!(out, vec![1, 2]);
        assert!(!cut);
    }
}
