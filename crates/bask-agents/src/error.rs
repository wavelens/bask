/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

/// Errors from constructing an agent. The runtime call and emit paths use `anyhow`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("task {task} declares emit target {target}, which is not a registered AgentTask")]
    UnregisteredTarget {
        task: &'static str,
        target: &'static str,
    },
    #[error("failed to build tool {name}: {message}")]
    Tool { name: &'static str, message: String },
}

pub type Result<T> = std::result::Result<T, Error>;
