/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! bask-agents: LLM agent workers that emit tasks along a source task's EmitPolicy DAG.

mod agent;
mod client;
mod config;
mod error;
mod registry;
mod render;
#[cfg(feature = "sandbox")]
mod tools;

pub use inventory;

pub use agent::{Agent, AgentBuilder, ToolChoice};
pub use bask_agents_macros::AgentTask;
pub use config::Agents;
pub use error::{Error, Result};
pub use registry::{AgentTask, AgentTaskInfo};
pub use render::render_task;

#[cfg(feature = "sandbox")]
pub use bask_sandbox::{Isolation, Limits, Network, SandboxSpec, SeedFile};
