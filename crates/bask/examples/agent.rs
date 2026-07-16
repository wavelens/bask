/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Constructing an agent through the `bask::agents` umbrella path. No network call is
//! made; this only resolves the tools an agent offers from its source task's EmitPolicy.

use bask::EmitPolicy;
use bask::agents::{AgentTask, Agents};

#[derive(serde::Serialize, EmitPolicy)]
#[emits(Summary)]
struct Document {
    path: String,
}

#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema, AgentTask)]
struct Summary {
    text: String,
}

fn main() -> anyhow::Result<()> {
    let agents = Agents::new().api_key("unused");
    let agent = agents
        .worker::<Document>()
        .instruction("Summarize the document below.")
        .build()?;
    println!("tools: {:?}", agent.tool_names());
    Ok(())
}
