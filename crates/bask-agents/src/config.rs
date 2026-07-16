/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use serde::Serialize;

use bask_core::EmitPolicy;

use crate::agent::AgentBuilder;

/// Engine-wide agent defaults. Clone-cheap; each `worker` seeds a builder with these.
#[derive(Clone)]
pub struct Agents {
    pub(crate) base_url: Option<String>,
    pub(crate) model: String,
    pub(crate) api_key: Option<String>,
}

impl Agents {
    pub fn new() -> Self {
        Agents {
            base_url: None,
            model: "gpt-4o".to_string(),
            api_key: None,
        }
    }
    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = Some(url.into());
        self
    }
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }
    /// Read the API key from an environment variable at call time (unset if absent).
    pub fn api_key_from_env(mut self, var: &str) -> Self {
        self.api_key = std::env::var(var).ok();
        self
    }
    /// Start building an agent for source task `Src`; its tools come from `Src`'s EmitPolicy.
    pub fn worker<Src: EmitPolicy + Serialize>(&self) -> AgentBuilder<Src> {
        AgentBuilder::new(self.clone())
    }
}

impl Default for Agents {
    fn default() -> Self {
        Self::new()
    }
}
