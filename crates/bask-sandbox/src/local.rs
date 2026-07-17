/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::spec::{ExecRequest, ExecResult, SandboxSpec};
use crate::{Error, Result, Sandbox};

/// A no-isolation backend: commands run as host subprocesses in a temp dir. Provides NO isolation
/// and must not be used for adversarial code; use the container backend for that.
pub(crate) struct LocalSandbox {
    root: TempDir,
    max_output_bytes: Option<usize>,
    default_timeout: Option<Duration>,
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

/// Read a stream into a buffer, appending only up to `max` bytes but draining the rest so the
/// child never blocks on a full pipe. Returns the buffer and whether bytes were discarded.
async fn read_capped<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    max: Option<usize>,
) -> std::io::Result<(Vec<u8>, bool)> {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::new();
    let mut truncated = false;
    let mut chunk = [0u8; 8192];
    loop {
        let n = reader.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        match max {
            Some(m) if buf.len() >= m => truncated = true,
            Some(m) => {
                let take = (m - buf.len()).min(n);
                buf.extend_from_slice(&chunk[..take]);
                if take < n {
                    truncated = true;
                }
            }
            None => buf.extend_from_slice(&chunk[..n]),
        }
    }
    Ok((buf, truncated))
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
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let started = Instant::now();
        let mut child = cmd.spawn()?;
        if let Some(stdin) = req.stdin
            && let Some(mut handle) = child.stdin.take()
        {
            handle.write_all(&stdin).await?;
        }

        let stdout_pipe = child.stdout.take().expect("stdout is piped");
        let stderr_pipe = child.stderr.take().expect("stderr is piped");
        let max = self.max_output_bytes;
        let run = async {
            let (out, err, status) = tokio::join!(
                read_capped(stdout_pipe, max),
                read_capped(stderr_pipe, max),
                child.wait(),
            );
            let (stdout, cut_out) = out?;
            let (stderr, cut_err) = err?;
            let exit_code = status?.code().unwrap_or(-1);
            Ok::<_, Error>((stdout, stderr, cut_out || cut_err, exit_code))
        };

        let timeout = req.timeout.or(self.default_timeout);
        let (stdout, stderr, truncated, exit_code) = match timeout {
            Some(limit) => match tokio::time::timeout(limit, run).await {
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
            None => run.await?,
        };

        Ok(ExecResult {
            exit_code,
            stdout,
            stderr,
            truncated,
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
