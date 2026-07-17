/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::path::Path;

use async_trait::async_trait;

use crate::spec::{ExecRequest, ExecResult, SandboxSpec};
use crate::{Result, Sandbox};

pub(crate) struct ContainerSandbox;

impl ContainerSandbox {
    pub(crate) async fn spawn(_spec: &SandboxSpec) -> Result<Self> {
        unimplemented!("container backend lands in Task 5")
    }
}

#[async_trait]
impl Sandbox for ContainerSandbox {
    async fn exec(&self, _req: ExecRequest) -> Result<ExecResult> {
        unimplemented!("container backend lands in Task 5")
    }
    async fn write_file(&self, _path: &Path, _contents: &[u8]) -> Result<()> {
        unimplemented!("container backend lands in Task 5")
    }
    async fn read_file(&self, _path: &Path) -> Result<Vec<u8>> {
        unimplemented!("container backend lands in Task 5")
    }
    async fn teardown(self: Box<Self>) -> Result<()> {
        unimplemented!("container backend lands in Task 5")
    }
}
