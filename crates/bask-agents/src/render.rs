/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use serde::Serialize;

/// Render a task as `Key: value` lines from its Serialize form, honoring #[serde(rename)].
/// Strings render bare; other values render as compact JSON. A non-object payload renders as a
/// single compact-JSON line.
pub fn render_task<T: Serialize>(task: &T) -> String {
    match serde_json::to_value(task) {
        Ok(serde_json::Value::Object(map)) => map
            .iter()
            .map(|(key, value)| format!("{key}: {}", render_value(value)))
            .collect::<Vec<_>>()
            .join("\n"),
        Ok(value) => render_value(&value),
        Err(err) => format!("<unserializable: {err}>"),
    }
}

fn render_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct Doc {
        path: String,
        #[serde(rename = "SizeBytes")]
        size: u64,
    }

    #[test]
    fn renders_key_value_and_honors_rename_in_declaration_order() {
        let out = render_task(&Doc {
            path: "test.md".into(),
            size: 20480,
        });
        assert_eq!(out, "path: test.md\nSizeBytes: 20480");
    }

    #[derive(Serialize)]
    struct Nested {
        tags: Vec<String>,
    }

    #[test]
    fn renders_nested_value_as_compact_json() {
        let out = render_task(&Nested {
            tags: vec!["a".into(), "b".into()],
        });
        assert_eq!(out, r#"tags: ["a","b"]"#);
    }

    #[test]
    fn renders_non_object_payload_as_compact_json() {
        assert_eq!(render_task(&vec![1, 2, 3]), "[1,2,3]");
    }
}
