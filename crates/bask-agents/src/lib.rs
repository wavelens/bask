/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! bask-agents: LLM agent workers that emit tasks along a source task's EmitPolicy DAG.

mod registry;

pub use bask_agents_macros::AgentTask;
pub use inventory;
pub use registry::{AgentTask, AgentTaskInfo};
