/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Instant;

use async_trait::async_trait;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::spec::{ExecRequest, ExecResult, SandboxSpec, truncate};
use crate::{Error, Result, Sandbox};

/// A no-isolation backend: commands run as host subprocesses rooted in a temp dir.
pub(crate) struct LocalSandbox {
    root: TempDir,
    max_output_bytes: Option<usize>,
    default_timeout: Option<std::time::Duration>,
    env: Vec<(String, String)>,
}

impl LocalSandbox {
    pub(crate) async fn spawn(spec: &SandboxSpec) -> Result<Self> {
        let root = TempDir::new()?;
        for seed in &spec.seed_files {
            let target = resolve(root.path(), &seed.path);
            if let Some(parent) = target.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::write(&target, &seed.contents).await?;
        }
        Ok(LocalSandbox {
            root,
            max_output_bytes: spec.limits.max_output_bytes,
            default_timeout: spec.limits.timeout,
            env: spec
                .env
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        })
    }
}

fn resolve(root: &Path, path: &Path) -> PathBuf {
    root.join(path.strip_prefix("/").unwrap_or(path))
}

#[async_trait]
impl Sandbox for LocalSandbox {
    async fn exec(&self, req: ExecRequest) -> Result<ExecResult> {
        let (program, args) = req
            .command
            .split_first()
            .ok_or_else(|| Error::Exec("empty command".into()))?;
        let mut cmd = Command::new(program);
        cmd.args(args).current_dir(self.root.path()).env_clear();
        if let Some(path) = std::env::var_os("PATH") {
            cmd.env("PATH", path);
        }
        cmd.envs(self.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let started = Instant::now();
        let mut child = cmd.spawn()?;
        if let Some(stdin) = req.stdin {
            if let Some(mut handle) = child.stdin.take() {
                handle.write_all(&stdin).await?;
            }
        }

        let timeout = req.timeout.or(self.default_timeout);
        let output = match timeout {
            Some(limit) => match tokio::time::timeout(limit, child.wait_with_output()).await {
                Ok(res) => res?,
                Err(_) => {
                    return Ok(ExecResult {
                        exit_code: 124,
                        stdout: Vec::new(),
                        stderr: b"timed out".to_vec(),
                        truncated: false,
                        duration: started.elapsed(),
                    });
                }
            },
            None => child.wait_with_output().await?,
        };

        let (stdout, cut_out) = truncate(output.stdout, self.max_output_bytes);
        let (stderr, cut_err) = truncate(output.stderr, self.max_output_bytes);
        Ok(ExecResult {
            exit_code: output.status.code().unwrap_or(-1),
            stdout,
            stderr,
            truncated: cut_out || cut_err,
            duration: started.elapsed(),
        })
    }

    async fn write_file(&self, path: &Path, contents: &[u8]) -> Result<()> {
        let target = resolve(self.root.path(), path);
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&target, contents).await?;
        Ok(())
    }

    async fn read_file(&self, path: &Path) -> Result<Vec<u8>> {
        Ok(tokio::fs::read(resolve(self.root.path(), path)).await?)
    }

    async fn teardown(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
