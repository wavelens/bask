/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::sync::{Arc, Mutex};

use bask_core::prelude::async_trait;
use bask_core::{Context, EmitPolicy, Engine, RetryPolicy, Worker};

use bask_agents::{AgentTask, Agents, ToolChoice};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[derive(serde::Serialize, EmitPolicy)]
#[emits(Summary)]
struct Document {
    path: String,
}

#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema, AgentTask)]
struct Summary {
    text: String,
}

struct Collect(Arc<Mutex<Vec<String>>>);
#[async_trait]
impl Worker for Collect {
    type Task = Summary;
    async fn process(&self, summary: &Summary, _ctx: &Context) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(summary.text.clone());
        Ok(())
    }
}

fn tool_call_body(text: &str) -> serde_json::Value {
    json!({
        "id": "1", "object": "chat.completion", "created": 0, "model": "gpt-4o",
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        "choices": [{
            "index": 0, "finish_reason": "tool_calls",
            "message": {
                "role": "assistant", "content": null,
                "tool_calls": [{
                    "id": "c1", "type": "function",
                    "function": {"name": "Summary", "arguments": format!("{{\"text\":\"{text}\"}}")}
                }]
            }
        }]
    })
}

#[test]
fn resolves_tools_from_emit_policy() {
    let agent = Agents::new()
        .api_key("x")
        .worker::<Document>()
        .instruction("Summarize.")
        .build()
        .unwrap();
    assert_eq!(agent.tool_names(), vec!["Summary"]);
}

#[derive(serde::Serialize, EmitPolicy)]
#[emits(Unregistered)]
struct BadDoc;
struct Unregistered;

#[test]
fn unregistered_target_is_rejected() {
    let err = Agents::new()
        .api_key("x")
        .worker::<BadDoc>()
        .instruction("x")
        .build()
        .err()
        .unwrap();
    assert!(matches!(err, bask_agents::Error::UnregisteredTarget { .. }));
}

#[tokio::test]
async fn agent_emits_tool_call_as_task() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(tool_call_body("done")))
        .mount(&server)
        .await;

    let collected = Arc::new(Mutex::new(Vec::new()));
    let agent = Agents::new()
        .base_url(server.uri())
        .api_key("test")
        .worker::<Document>()
        .instruction("Summarize.")
        .tool_choice(ToolChoice::Auto)
        .build()
        .unwrap();

    let report = Engine::builder()
        .worker(agent)
        .worker(Collect(collected.clone()))
        .seed(Document { path: "a.md".into() })
        .run()
        .await
        .unwrap();

    assert_eq!(report.stats.failed, 0);
    assert_eq!(report.stats.processed, 2);
    assert_eq!(collected.lock().unwrap().as_slice(), &["done".to_string()]);
}

#[tokio::test]
async fn agent_invokes_on_text_for_plain_response() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "1", "object": "chat.completion", "created": 0, "model": "gpt-4o",
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            "choices": [{
                "index": 0, "finish_reason": "stop",
                "message": {"role": "assistant", "content": "a plain answer"}
            }]
        })))
        .mount(&server)
        .await;

    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_cb = seen.clone();
    let agent = Agents::new()
        .base_url(server.uri())
        .api_key("test")
        .worker::<Document>()
        .instruction("Summarize.")
        .on_text(move |text| {
            seen_cb.lock().unwrap().push(text.to_string());
            Ok(())
        })
        .build()
        .unwrap();

    let report = Engine::builder()
        .worker(agent)
        .seed(Document { path: "a.md".into() })
        .run()
        .await
        .unwrap();

    assert_eq!(report.stats.failed, 0);
    assert_eq!(seen.lock().unwrap().as_slice(), &["a plain answer".to_string()]);
}

#[tokio::test]
async fn auth_error_fails_terminally() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({
            "error": {"message": "bad key", "type": "invalid_request_error", "code": "invalid_api_key"}
        })))
        .mount(&server)
        .await;

    let agent = Agents::new()
        .base_url(server.uri())
        .api_key("bad")
        .worker::<Document>()
        .instruction("Summarize.")
        .build()
        .unwrap();

    let report = Engine::builder()
        .retry(RetryPolicy::new().max_attempts(5))
        .worker(agent)
        .seed(Document { path: "a.md".into() })
        .run()
        .await
        .unwrap();

    assert_eq!(report.stats.failed, 1);
    assert_eq!(report.failures[0].attempts, 1);
}

#[tokio::test]
async fn server_error_is_retried() {
    let server = MockServer::start().await;
    // async-openai's client retries 5xx internally up to 3 times per call (4 attempts);
    // fail one more than that so the error actually reaches bask-core's RetryPolicy.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(4)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(tool_call_body("ok")))
        .with_priority(2)
        .mount(&server)
        .await;

    let collected = Arc::new(Mutex::new(Vec::new()));
    let agent = Agents::new()
        .base_url(server.uri())
        .api_key("test")
        .worker::<Document>()
        .instruction("Summarize.")
        .build()
        .unwrap();

    let report = Engine::builder()
        .retry(RetryPolicy::new().max_attempts(3))
        .worker(agent)
        .worker(Collect(collected.clone()))
        .seed(Document { path: "a.md".into() })
        .run()
        .await
        .unwrap();

    assert_eq!(report.stats.failed, 0);
    assert_eq!(report.stats.retried, 1);
    assert_eq!(collected.lock().unwrap().as_slice(), &["ok".to_string()]);
}
