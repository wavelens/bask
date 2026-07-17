/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! An agent that runs a shell command in a sandbox, reacts to its output, then emits a Report.
//! `SandboxSpec::default()` now uses `OsSandbox` (Landlock + seccomp on Linux); `Local` is an
//! explicit opt-out for trusted use and `Container` is a stronger opt-in.
//! Set OPENAI_API_KEY (and optionally OPENAI_BASE_URL) and run:
//! `cargo run -p bask --features sandbox --example agent_sandbox`.

use bask::agents::{Agents, SandboxSpec, ToolChoice};
use bask_core::prelude::async_trait;
use bask_core::{Context, EmitPolicy, Engine, Worker};

#[derive(serde::Serialize, EmitPolicy)]
#[emits(Report)]
struct Investigate {
    question: String,
}

#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema, bask::agents::AgentTask)]
struct Report {
    finding: String,
}

struct PrintReport;
#[async_trait]
impl Worker for PrintReport {
    type Task = Report;
    async fn process(&self, report: &Report, _ctx: &Context) -> anyhow::Result<()> {
        println!("finding: {}", report.finding);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut agents = Agents::new()
        .api_key_from_env("OPENAI_API_KEY")
        .model("gpt-4o");
    if let Ok(base_url) = std::env::var("OPENAI_BASE_URL") {
        agents = agents.base_url(base_url);
    }

    let agent = agents
        .worker::<Investigate>()
        .system("You investigate by running shell commands, then report via the Report tool.")
        .instruction("Answer the question. Use run_command as needed, then call Report.")
        .tool_choice(ToolChoice::Auto)
        .sandbox(SandboxSpec::default())
        .max_steps(6)
        .build()?;

    let report = Engine::builder()
        .worker(agent)
        .worker(PrintReport)
        .seed(Investigate {
            question: "How many .rs files are under the current dir?".into(),
        })
        .run()
        .await?;

    println!(
        "processed {}, failed {}",
        report.stats.processed, report.stats.failed
    );
    Ok(())
}
