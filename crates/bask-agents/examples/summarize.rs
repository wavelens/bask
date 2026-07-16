/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! An end-to-end summarization pipeline. An `Agent<Document>` calls an OpenAI-compatible
//! model, which either emits a `Summary` or asks for more input via `NeedsInput`; the tool
//! set the model sees is exactly the `Document` EmitPolicy the engine also enforces. Set
//! `OPENAI_API_KEY` (and optionally `OPENAI_BASE_URL` for a local endpoint) and run:
//! `cargo run -p bask-agents --example summarize`.

use bask_core::prelude::async_trait;
use bask_core::{Context, EmitPolicy, Engine, Worker};

use bask_agents::{AgentTask, Agents, ToolChoice};

/// The task the agent consumes. Its EmitPolicy is the single source of truth for both the
/// tools offered to the model and the engine's runtime enforcement.
#[derive(serde::Serialize, EmitPolicy)]
#[emits(Summary, NeedsInput)]
struct Document {
    path: String,
    #[serde(rename = "Contents")]
    contents: String,
}

/// The structured result the model produces by calling the `Summary` tool.
#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema, AgentTask)]
struct Summary {
    text: String,
}

/// The model may ask for clarification instead of summarizing.
#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema, AgentTask)]
#[agent(description = "Request a clarification when the document is insufficient to summarize")]
struct NeedsInput {
    question: String,
}

struct PrintSummary;
#[async_trait]
impl Worker for PrintSummary {
    type Task = Summary;
    async fn process(&self, summary: &Summary, _ctx: &Context) -> anyhow::Result<()> {
        println!("summary: {}", summary.text);
        Ok(())
    }
}

struct AskUser;
#[async_trait]
impl Worker for AskUser {
    type Task = NeedsInput;
    async fn process(&self, needs: &NeedsInput, _ctx: &Context) -> anyhow::Result<()> {
        println!("model needs input: {}", needs.question);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut agents = Agents::new()
        .api_key_from_env("OPENAI_API_KEY")
        .model("gpt-4o-mini");
    if let Ok(base_url) = std::env::var("OPENAI_BASE_URL") {
        agents = agents.base_url(base_url);
    }

    let agent = agents
        .worker::<Document>()
        .system("You are a meticulous technical summarizer.")
        .instruction("Summarize the document below in two sentences by calling the Summary tool.")
        .tool_choice(ToolChoice::Required)
        .build()?;

    let report = Engine::builder()
        .worker(agent)
        .worker(PrintSummary)
        .worker(AskUser)
        .seed(Document {
            path: "README.md".into(),
            contents: "bask is an async task-queue pipeline engine with pluggable IO, \
                       columnar formats, and predefined tasks."
                .into(),
        })
        .run()
        .await?;

    println!(
        "processed {}, failed {}",
        report.stats.processed, report.stats.failed
    );
    Ok(())
}
