/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bollard::Docker;
use bollard::container::{
    Config, CreateContainerOptions, DownloadFromContainerOptions, LogOutput,
    RemoveContainerOptions, UploadToContainerOptions,
};
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::image::CreateImageOptions;
use bollard::models::HostConfig;
use futures::{StreamExt, TryStreamExt};

use crate::spec::{ExecRequest, ExecResult, Network, SandboxSpec, truncate};
use crate::{Error, Result, Sandbox};

/// A container-isolated backend backed by an OCI runtime (Docker or Podman) via bollard.
pub(crate) struct ContainerSandbox {
    docker: Docker,
    id: String,
    workdir: String,
    max_output_bytes: Option<usize>,
    default_timeout: Option<Duration>,
}

impl From<bollard::errors::Error> for Error {
    fn from(err: bollard::errors::Error) -> Self {
        Error::Exec(err.to_string())
    }
}

impl ContainerSandbox {
    pub(crate) async fn spawn(spec: &SandboxSpec) -> Result<Self> {
        let image = spec.image.clone().ok_or(Error::MissingImage)?;
        let docker = Docker::connect_with_local_defaults()
            .map_err(|e| Error::Unavailable("container", e.to_string()))?;
        docker
            .ping()
            .await
            .map_err(|e| Error::Unavailable("container", e.to_string()))?;

        let mut pull = docker.create_image(
            Some(CreateImageOptions {
                from_image: image.clone(),
                ..Default::default()
            }),
            None,
            None,
        );
        while let Some(step) = pull.next().await {
            step?;
        }

        let workdir = spec.workdir.to_string_lossy().to_string();
        let env: Vec<String> = spec.env.iter().map(|(k, v)| format!("{k}={v}")).collect();
        // Hardening (cap_drop/readonly_rootfs/tmpfs/no-new-privileges) is applied here; rootless
        // execution is expected from a rootless daemon (e.g. rootless podman). RUNTIME-UNVERIFIED.
        let host_config = HostConfig {
            network_mode: Some(match spec.network {
                Network::None => "none".into(),
                Network::Bridge => "bridge".into(),
                Network::Host => "host".into(),
            }),
            memory: spec.limits.memory_bytes.map(|b| b as i64),
            nano_cpus: spec.limits.cpus.map(|c| (c as f64 * 1e9) as i64),
            cap_drop: Some(vec!["ALL".to_string()]),
            readonly_rootfs: Some(true),
            security_opt: Some(vec!["no-new-privileges".to_string()]),
            tmpfs: Some(HashMap::from([(workdir.clone(), String::new())])),
            ..Default::default()
        };
        let config = Config {
            image: Some(image),
            working_dir: Some(workdir.clone()),
            env: Some(env),
            cmd: Some(vec!["sleep".into(), "infinity".into()]),
            host_config: Some(host_config),
            ..Default::default()
        };
        let created = docker
            .create_container(None::<CreateContainerOptions<String>>, config)
            .await?;
        docker.start_container::<String>(&created.id, None).await?;

        let sandbox = ContainerSandbox {
            docker,
            id: created.id,
            workdir,
            max_output_bytes: spec.limits.max_output_bytes,
            default_timeout: spec.limits.timeout,
        };
        sandbox.ensure_workdir().await?;
        for seed in &spec.seed_files {
            sandbox.write_file(&seed.path, &seed.contents).await?;
        }
        Ok(sandbox)
    }

    async fn ensure_workdir(&self) -> Result<()> {
        self.exec(ExecRequest::new(vec![
            "mkdir".into(),
            "-p".into(),
            self.workdir.clone(),
        ]))
        .await
        .map(|_| ())
    }

    fn abs(&self, path: &Path) -> String {
        if path.is_absolute() {
            path.to_string_lossy().to_string()
        } else {
            format!("{}/{}", self.workdir, path.to_string_lossy())
        }
    }
}

#[async_trait]
impl Sandbox for ContainerSandbox {
    async fn exec(&self, req: ExecRequest) -> Result<ExecResult> {
        let timeout = req.timeout.or(self.default_timeout);
        let exec = self
            .docker
            .create_exec(
                &self.id,
                CreateExecOptions {
                    cmd: Some(req.command),
                    working_dir: Some(self.workdir.clone()),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    ..Default::default()
                },
            )
            .await?;

        let started = Instant::now();
        let drain = async {
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            if let StartExecResults::Attached { mut output, .. } =
                self.docker.start_exec(&exec.id, None).await?
            {
                while let Some(chunk) = output.next().await {
                    match chunk? {
                        LogOutput::StdOut { message } => stdout.extend_from_slice(&message),
                        LogOutput::StdErr { message } => stderr.extend_from_slice(&message),
                        other => stdout.extend_from_slice(&other.into_bytes()),
                    }
                }
            }
            let inspect = self.docker.inspect_exec(&exec.id).await?;
            let exit_code = inspect.exit_code.unwrap_or(-1) as i32;
            Ok::<_, Error>((stdout, stderr, exit_code))
        };

        // On timeout the exec'd process keeps running inside the container until teardown force-removes
        // the whole ephemeral container (the container analog of Local's kill_on_drop).
        let (stdout, stderr, exit_code) = match timeout {
            Some(limit) => match tokio::time::timeout(limit, drain).await {
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
            None => drain.await?,
        };

        let (stdout, cut_out) = truncate(stdout, self.max_output_bytes);
        let (stderr, cut_err) = truncate(stderr, self.max_output_bytes);
        Ok(ExecResult {
            exit_code,
            stdout,
            stderr,
            truncated: cut_out || cut_err,
            duration: started.elapsed(),
        })
    }

    async fn write_file(&self, path: &Path, contents: &[u8]) -> Result<()> {
        let abs = self.abs(path);
        let (dir, name) = match abs.rsplit_once('/') {
            Some((d, n)) if !d.is_empty() => (d.to_string(), n.to_string()),
            _ => ("/".to_string(), abs.clone()),
        };
        self.exec(ExecRequest::new(vec![
            "mkdir".into(),
            "-p".into(),
            dir.clone(),
        ]))
        .await?;

        let mut tar_buf = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut tar_buf);
            let mut header = tar::Header::new_gnu();
            header.set_size(contents.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, &name, contents)
                .map_err(|e| Error::Exec(e.to_string()))?;
            builder.finish().map_err(|e| Error::Exec(e.to_string()))?;
        }
        self.docker
            .upload_to_container(
                &self.id,
                Some(UploadToContainerOptions {
                    path: dir,
                    ..Default::default()
                }),
                tar_buf.into(),
            )
            .await?;
        Ok(())
    }

    async fn read_file(&self, path: &Path) -> Result<Vec<u8>> {
        let abs = self.abs(path);
        let stream = self
            .docker
            .download_from_container(&self.id, Some(DownloadFromContainerOptions { path: abs }));
        let bytes: Vec<u8> = stream
            .map_ok(|b| b.to_vec())
            .try_concat()
            .await
            .map_err(|e| Error::Exec(e.to_string()))?;
        let mut archive = tar::Archive::new(&bytes[..]);
        let mut entry = archive
            .entries()
            .map_err(|e| Error::Exec(e.to_string()))?
            .next()
            .ok_or_else(|| Error::Exec("empty archive".into()))?
            .map_err(|e| Error::Exec(e.to_string()))?;
        let mut out = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut out)?;
        Ok(out)
    }

    async fn teardown(self: Box<Self>) -> Result<()> {
        self.docker
            .remove_container(
                &self.id,
                Some(RemoveContainerOptions {
                    force: true,
                    v: true,
                    ..Default::default()
                }),
            )
            .await?;
        Ok(())
    }
}

impl Drop for ContainerSandbox {
    fn drop(&mut self) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let docker = self.docker.clone();
            let id = self.id.clone();
            handle.spawn(async move {
                let _ = docker
                    .remove_container(
                        &id,
                        Some(RemoveContainerOptions {
                            force: true,
                            v: true,
                            ..Default::default()
                        }),
                    )
                    .await;
            });
        }
    }
}
