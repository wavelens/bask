/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! bask-sandbox: a pluggable `Sandbox` trait with Local and Container backends.

mod error;
mod spec;

#[cfg(feature = "container")]
mod container;
mod exec_common;
mod local;
#[cfg(all(feature = "os-sandbox", target_os = "linux"))]
mod os_sandbox;

use std::path::Path;

use async_trait::async_trait;

pub use error::{Error, Result};
pub use spec::{ExecRequest, ExecResult, Isolation, Limits, Network, SandboxSpec, SeedFile};

/// An isolated environment that runs commands and moves files in and out.
#[async_trait]
pub trait Sandbox: Send + Sync {
    async fn exec(&self, req: ExecRequest) -> Result<ExecResult>;
    async fn write_file(&self, path: &Path, contents: &[u8]) -> Result<()>;
    async fn read_file(&self, path: &Path) -> Result<Vec<u8>>;
    async fn teardown(self: Box<Self>) -> Result<()>;
}

/// Spawn a sandbox for `spec`, selecting a backend from its declared isolation.
pub async fn spawn(spec: &SandboxSpec) -> Result<Box<dyn Sandbox>> {
    match spec.isolation {
        Isolation::Local => Ok(Box::new(local::LocalSandbox::spawn(spec).await?)),
        Isolation::OsSandbox => {
            #[cfg(all(feature = "os-sandbox", target_os = "linux"))]
            {
                Ok(Box::new(os_sandbox::OsSandbox::spawn(spec).await?))
            }
            #[cfg(not(all(feature = "os-sandbox", target_os = "linux")))]
            {
                Err(Error::Unavailable(
                    "os-sandbox",
                    "OsSandbox is only supported on Linux with the 'os-sandbox' feature"
                        .to_string(),
                ))
            }
        }
        Isolation::Container => {
            #[cfg(feature = "container")]
            {
                Ok(Box::new(container::ContainerSandbox::spawn(spec).await?))
            }
            #[cfg(not(feature = "container"))]
            {
                Err(Error::Unavailable(
                    "container",
                    "build bask-sandbox with the 'container' feature".to_string(),
                ))
            }
        }
    }
}
