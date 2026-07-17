/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

#![cfg(feature = "sandbox")]

use bask_agents::{Agents, ToolChoice};
use bask_core::prelude::async_trait;
use bask_core::{Context, EmitPolicy, Engine, Worker};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

#[derive(serde::Serialize, EmitPolicy)]
#[emits(Done)]
struct Job {
    goal: String,
}

#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema, bask_agents::AgentTask)]
struct Done {
    output: String,
}

struct Collect;
#[async_trait]
impl Worker for Collect {
    type Task = Done;
    async fn process(&self, done: &Done, _ctx: &Context) -> anyhow::Result<()> {
        assert!(done.output.contains("hello"));
        Ok(())
    }
}

fn run_command_turn() -> serde_json::Value {
    serde_json::json!({
        "id": "1", "object": "chat.completion", "created": 0, "model": "m",
        "choices": [{ "index": 0, "finish_reason": "tool_calls", "message": {
            "role": "assistant", "content": null,
            "tool_calls": [{ "id": "c1", "type": "function", "function": {
                "name": "run_command",
                "arguments": "{\"command\":[\"sh\",\"-c\",\"printf hello\"]}"
            }}]
        }}]
    })
}

fn emit_turn() -> serde_json::Value {
    serde_json::json!({
        "id": "2", "object": "chat.completion", "created": 0, "model": "m",
        "choices": [{ "index": 0, "finish_reason": "tool_calls", "message": {
            "role": "assistant", "content": null,
            "tool_calls": [{ "id": "c2", "type": "function", "function": {
                "name": "Done", "arguments": "{\"output\":\"hello\"}"
            }}]
        }}]
    })
}

#[tokio::test]
async fn agent_runs_command_then_emits() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(run_command_turn()))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(emit_turn()))
        .mount(&server)
        .await;

    let agent = Agents::new()
        .base_url(format!("{}/v1", server.uri()))
        .api_key("test")
        .model("m")
        .worker::<Job>()
        .instruction("run the goal then report")
        .tool_choice(ToolChoice::Auto)
        .sandbox(bask_sandbox::SandboxSpec {
            isolation: bask_sandbox::Isolation::Local,
            ..bask_sandbox::SandboxSpec::default()
        })
        .max_steps(4)
        .build()
        .unwrap();

    let report = Engine::builder()
        .worker(agent)
        .worker(Collect)
        .seed(Job {
            goal: "say hello".into(),
        })
        .run()
        .await
        .unwrap();
    assert_eq!(report.stats.failed, 0);
    assert!(report.stats.processed >= 2);
}

#[test]
fn default_sandbox_isolation_is_os_sandbox() {
    assert_eq!(
        bask_sandbox::SandboxSpec::default().isolation,
        bask_sandbox::Isolation::OsSandbox
    );
}
