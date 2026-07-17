/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use tempfile::TempDir;

use crate::exec_common::Subprocess;
use crate::spec::{ExecRequest, ExecResult, SandboxSpec};
use crate::{Result, Sandbox};

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

    fn subprocess(&self) -> Subprocess {
        Subprocess {
            root: self.root.path().to_path_buf(),
            env: self.env.clone(),
            max_output_bytes: self.max_output_bytes,
            default_timeout: self.default_timeout,
        }
    }
}

fn resolve(root: &Path, path: &Path) -> PathBuf {
    root.join(path.strip_prefix("/").unwrap_or(path))
}

#[async_trait]
impl Sandbox for LocalSandbox {
    async fn exec(&self, req: ExecRequest) -> Result<ExecResult> {
        self.subprocess().run(req, None).await
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
