/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::path::Path;

use async_trait::async_trait;

use crate::spec::{ExecRequest, ExecResult, SandboxSpec};
use crate::{Result, Sandbox};

pub(crate) struct LocalSandbox;

impl LocalSandbox {
    pub(crate) async fn spawn(_spec: &SandboxSpec) -> Result<Self> {
        Ok(LocalSandbox)
    }
}

#[async_trait]
impl Sandbox for LocalSandbox {
    async fn exec(&self, _req: ExecRequest) -> Result<ExecResult> {
        unimplemented!("local exec lands in Task 2")
    }
    async fn write_file(&self, _path: &Path, _contents: &[u8]) -> Result<()> {
        unimplemented!("local write_file lands in Task 2")
    }
    async fn read_file(&self, _path: &Path) -> Result<Vec<u8>> {
        unimplemented!("local read_file lands in Task 2")
    }
    async fn teardown(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
