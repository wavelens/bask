/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

/// Isolation strength the caller declares; `spawn` selects a backend that satisfies it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Isolation {
    Local,
    #[default]
    OsSandbox,
    Container,
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
            isolation: Isolation::OsSandbox,
            image: None,
            workdir: PathBuf::from("/work"),
            env: BTreeMap::new(),
            network: Network::None,
            limits: Limits {
                timeout: Some(Duration::from_secs(300)),
                max_output_bytes: Some(1 << 20),
                ..Limits::default()
            },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_defaults_are_secure() {
        let spec = SandboxSpec::default();
        assert_eq!(spec.isolation, Isolation::OsSandbox);
        assert_eq!(spec.network, Network::None);
        assert!(spec.image.is_none());
        assert!(spec.limits.timeout.is_some());
        assert!(spec.limits.max_output_bytes.is_some());
    }
}
