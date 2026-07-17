/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;

use crate::spec::{ExecRequest, ExecResult};
use crate::{Error, Result};

/// A closure applied in the forked child (via `pre_exec`) to confine it before `exec`.
pub(crate) type PreExecHook = Box<dyn FnMut() -> std::io::Result<()> + Send + Sync + 'static>;

/// Shared subprocess runner used by the Local and OsSandbox backends.
pub(crate) struct Subprocess {
    pub root: PathBuf,
    pub env: Vec<(String, String)>,
    pub max_output_bytes: Option<usize>,
    pub default_timeout: Option<Duration>,
}

/// Read a stream, appending only up to `max` bytes but draining the rest so the child never
/// blocks on a full pipe. Returns the buffer and whether bytes were discarded.
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

impl Subprocess {
    pub(crate) async fn run(
        &self,
        req: ExecRequest,
        hook_factory: Option<&(dyn Fn() -> Result<PreExecHook> + Send + Sync)>,
    ) -> Result<ExecResult> {
        let (program, args) = req
            .command
            .split_first()
            .ok_or_else(|| Error::Exec("empty command".into()))?;

        let mut std_cmd = std::process::Command::new(program);
        std_cmd.args(args).current_dir(&self.root).env_clear();
        if let Some(path) = std::env::var_os("PATH") {
            std_cmd.env("PATH", path);
        }
        std_cmd
            .envs(self.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        if let Some(factory) = hook_factory {
            let hook = factory()?;
            // Safety: the hook performs only async-signal-safe syscalls (landlock/seccomp/prctl).
            unsafe {
                std::os::unix::process::CommandExt::pre_exec(&mut std_cmd, hook);
            }
        }

        let mut cmd = tokio::process::Command::from(std_cmd);
        cmd.kill_on_drop(true);

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
}
