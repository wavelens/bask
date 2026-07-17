/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use async_openai::types::chat::{ChatCompletionTool, ChatCompletionTools, FunctionObjectArgs};
use bask_sandbox::{ExecRequest, Sandbox};
use serde_json::json;

pub(crate) const BUILTIN_NAMES: [&str; 3] = ["run_command", "write_file", "read_file"];

pub(crate) fn is_builtin(name: &str) -> bool {
    BUILTIN_NAMES.contains(&name)
}

/// The three sandbox tools offered to the model when a sandbox is attached.
pub(crate) fn builtin_tools() -> anyhow::Result<Vec<ChatCompletionTools>> {
    let specs = [
        (
            "run_command",
            "Run a command (argv) in the sandbox and return exit code, stdout, stderr.",
            json!({
                "type": "object",
                "properties": {
                    "command": { "type": "array", "items": { "type": "string" } },
                    "timeout_secs": { "type": "number" }
                },
                "required": ["command"]
            }),
        ),
        (
            "write_file",
            "Write a UTF-8 file into the sandbox at the given path.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "contents": { "type": "string" }
                },
                "required": ["path", "contents"]
            }),
        ),
        (
            "read_file",
            "Read a UTF-8 file from the sandbox at the given path.",
            json!({
                "type": "object",
                "properties": { "path": { "type": "string" } },
                "required": ["path"]
            }),
        ),
    ];

    let mut tools = Vec::new();
    for (name, description, schema) in specs {
        let function = FunctionObjectArgs::default()
            .name(name)
            .description(description)
            .parameters(schema)
            .build()?;
        tools.push(ChatCompletionTools::Function(ChatCompletionTool {
            function,
        }));
    }
    Ok(tools)
}

/// Execute one built-in tool call against the sandbox and return a JSON result string
/// suitable for a tool-result message.
pub(crate) async fn execute(
    sandbox: &dyn Sandbox,
    name: &str,
    args: serde_json::Value,
) -> anyhow::Result<String> {
    match name {
        "run_command" => {
            let command: Vec<String> = serde_json::from_value(
                args.get("command")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            )?;
            let mut req = ExecRequest::new(command);
            if let Some(secs) = args.get("timeout_secs").and_then(|v| v.as_f64()) {
                if let Ok(duration) = std::time::Duration::try_from_secs_f64(secs) {
                    req.timeout = Some(duration);
                }
            }
            let result = sandbox.exec(req).await?;
            Ok(json!({
                "exit_code": result.exit_code,
                "stdout": String::from_utf8_lossy(&result.stdout),
                "stderr": String::from_utf8_lossy(&result.stderr),
                "truncated": result.truncated
            })
            .to_string())
        }
        "write_file" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let contents = args
                .get("contents")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            sandbox
                .write_file(std::path::Path::new(path), contents.as_bytes())
                .await?;
            Ok(json!({ "ok": true }).to_string())
        }
        "read_file" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let bytes = sandbox.read_file(std::path::Path::new(path)).await?;
            Ok(json!({ "contents": String::from_utf8_lossy(&bytes) }).to_string())
        }
        other => Err(anyhow::anyhow!("unknown built-in tool {other}")),
    }
}

#[cfg(all(test, feature = "sandbox"))]
mod tests {
    use super::*;
    use bask_sandbox::SandboxSpec;

    #[tokio::test]
    async fn run_command_ignores_invalid_timeout_secs() {
        let sandbox = bask_sandbox::spawn(&SandboxSpec::default()).await.unwrap();

        let result = execute(
            &*sandbox,
            "run_command",
            json!({"command": ["true"], "timeout_secs": -1.0}),
        )
        .await;
        assert!(
            result.is_ok(),
            "negative timeout_secs must not panic: {result:?}"
        );

        let result = execute(
            &*sandbox,
            "run_command",
            json!({"command": ["true"], "timeout_secs": 1e300}),
        )
        .await;
        assert!(
            result.is_ok(),
            "overflowing timeout_secs must not panic: {result:?}"
        );

        sandbox.teardown().await.unwrap();
    }
}
