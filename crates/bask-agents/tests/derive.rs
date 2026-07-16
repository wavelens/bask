/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use bask_agents::AgentTask;

#[derive(serde::Deserialize, schemars::JsonSchema, AgentTask)]
struct Reply {
    text: String,
}

#[derive(serde::Deserialize, schemars::JsonSchema, AgentTask)]
#[agent(name = "custom_reply", description = "A short reply")]
struct Custom {
    text: String,
}

#[test]
fn derive_defaults_name_to_type() {
    assert_eq!(<Reply as AgentTask>::NAME, "Reply");
    assert_eq!(<Reply as AgentTask>::description(), None);
}

#[test]
fn derive_honors_attributes() {
    assert_eq!(<Custom as AgentTask>::NAME, "custom_reply");
    assert_eq!(<Custom as AgentTask>::description(), Some("A short reply"));
}
