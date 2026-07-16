/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::marker::PhantomData;
use std::sync::Arc;

use async_openai::Client;
use async_openai::config::OpenAIConfig;
use async_openai::types::chat::{
    ChatCompletionMessageToolCalls, ChatCompletionRequestMessage,
    ChatCompletionRequestSystemMessageArgs, ChatCompletionRequestUserMessageArgs,
    ChatCompletionTool, ChatCompletionToolChoiceOption, ChatCompletionTools,
    CreateChatCompletionRequest, CreateChatCompletionRequestArgs, FunctionObjectArgs,
    ToolChoiceOptions,
};
use async_trait::async_trait;
use serde::Serialize;

use bask_core::{Allow, Context, EmitPolicy, Worker};

use crate::client;
use crate::config::Agents;
use crate::error::{Error, Result};
use crate::registry::{EmitFn, registry};
use crate::render::render_task;

/// Whether the model may, must, or must not call a tool.
#[derive(Clone, Copy, Default)]
pub enum ToolChoice {
    #[default]
    Auto,
    Required,
    None,
}

impl ToolChoice {
    fn to_openai(self) -> ChatCompletionToolChoiceOption {
        let mode = match self {
            ToolChoice::Auto => ToolChoiceOptions::Auto,
            ToolChoice::Required => ToolChoiceOptions::Required,
            ToolChoice::None => ToolChoiceOptions::None,
        };
        ChatCompletionToolChoiceOption::Mode(mode)
    }
}

/// A synchronous callback for the model's plain-text response (no structured tool call).
type OnText = Arc<dyn Fn(&str) -> anyhow::Result<()> + Send + Sync>;

struct ResolvedTool {
    name: &'static str,
    emit: EmitFn,
}

/// A worker that consults a model and emits the resulting tasks. Build with [`Agents::worker`].
pub struct Agent<Src> {
    client: Client<OpenAIConfig>,
    model: String,
    system: Option<String>,
    instruction: String,
    tool_choice: ToolChoice,
    temperature: Option<f32>,
    max_tokens: Option<u32>,
    tools: Vec<ResolvedTool>,
    openai_tools: Vec<ChatCompletionTools>,
    on_text: Option<OnText>,
    _src: PhantomData<fn() -> Src>,
}

impl<Src> Agent<Src> {
    /// The tool names this agent offers the model (its source task's EmitPolicy targets).
    pub fn tool_names(&self) -> Vec<&'static str> {
        self.tools.iter().map(|tool| tool.name).collect()
    }
}

/// Builds an [`Agent`]; `.build()` resolves tools from `Src`'s EmitPolicy and constructs the client.
pub struct AgentBuilder<Src> {
    defaults: Agents,
    model: Option<String>,
    base_url: Option<String>,
    api_key: Option<String>,
    system: Option<String>,
    instruction: String,
    tool_choice: ToolChoice,
    temperature: Option<f32>,
    max_tokens: Option<u32>,
    on_text: Option<OnText>,
    _src: PhantomData<fn() -> Src>,
}

impl<Src: EmitPolicy + Serialize> AgentBuilder<Src> {
    pub(crate) fn new(defaults: Agents) -> Self {
        AgentBuilder {
            defaults,
            model: None,
            base_url: None,
            api_key: None,
            system: None,
            instruction: String::new(),
            tool_choice: ToolChoice::default(),
            temperature: None,
            max_tokens: None,
            on_text: None,
            _src: PhantomData,
        }
    }

    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }
    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = Some(url.into());
        self
    }
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }
    pub fn system(mut self, prompt: impl Into<String>) -> Self {
        self.system = Some(prompt.into());
        self
    }
    pub fn instruction(mut self, instruction: impl Into<String>) -> Self {
        self.instruction = instruction.into();
        self
    }
    pub fn tool_choice(mut self, choice: ToolChoice) -> Self {
        self.tool_choice = choice;
        self
    }
    pub fn temperature(mut self, temperature: f32) -> Self {
        self.temperature = Some(temperature);
        self
    }
    pub fn max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = Some(max_tokens);
        self
    }
    pub fn on_text<F>(mut self, callback: F) -> Self
    where
        F: Fn(&str) -> anyhow::Result<()> + Send + Sync + 'static,
    {
        self.on_text = Some(Arc::new(callback));
        self
    }

    /// Resolve tools from `Src`'s EmitPolicy and build the agent, failing if a declared target
    /// is not a registered `AgentTask`.
    pub fn build(self) -> Result<Agent<Src>> {
        let mut allow = Allow::default();
        <Src as EmitPolicy>::declare(&mut allow);
        let reg = registry();

        let mut tools = Vec::new();
        let mut openai_tools = Vec::new();
        for (type_id, target_name) in allow.targets().iter().copied() {
            let entry = reg.get(&type_id).ok_or_else(|| Error::UnregisteredTarget {
                task: std::any::type_name::<Src>(),
                target: target_name,
            })?;
            let mut function = FunctionObjectArgs::default();
            function.name(entry.name).parameters(entry.schema.clone());
            if let Some(description) = entry.description {
                function.description(description);
            }
            let function = function.build().map_err(|err| Error::Tool {
                name: entry.name,
                message: err.to_string(),
            })?;
            openai_tools.push(ChatCompletionTools::Function(ChatCompletionTool { function }));
            tools.push(ResolvedTool { name: entry.name, emit: entry.emit });
        }

        let model = self.model.unwrap_or_else(|| self.defaults.model.clone());
        let base_url = self.base_url.or_else(|| self.defaults.base_url.clone());
        let api_key = self.api_key.or_else(|| self.defaults.api_key.clone());
        let client = client::build_client(base_url.as_deref(), api_key.as_deref());

        Ok(Agent {
            client,
            model,
            system: self.system,
            instruction: self.instruction,
            tool_choice: self.tool_choice,
            temperature: self.temperature,
            max_tokens: self.max_tokens,
            tools,
            openai_tools,
            on_text: self.on_text,
            _src: PhantomData,
        })
    }
}

impl<Src: EmitPolicy + Serialize> Agent<Src> {
    fn build_request(&self, user: String) -> anyhow::Result<CreateChatCompletionRequest> {
        let mut messages: Vec<ChatCompletionRequestMessage> = Vec::new();
        if let Some(system) = &self.system {
            messages.push(
                ChatCompletionRequestSystemMessageArgs::default()
                    .content(system.clone())
                    .build()?
                    .into(),
            );
        }
        messages.push(
            ChatCompletionRequestUserMessageArgs::default()
                .content(user)
                .build()?
                .into(),
        );

        let mut request = CreateChatCompletionRequestArgs::default();
        request.model(self.model.clone()).messages(messages);
        if !self.openai_tools.is_empty() {
            request.tools(self.openai_tools.clone());
            request.tool_choice(self.tool_choice.to_openai());
        }
        if let Some(temperature) = self.temperature {
            request.temperature(temperature);
        }
        if let Some(max_tokens) = self.max_tokens {
            request.max_completion_tokens(max_tokens);
        }
        Ok(request.build()?)
    }
}

#[async_trait]
impl<Src: EmitPolicy + Serialize> Worker for Agent<Src> {
    type Task = Src;

    async fn process(&self, task: &Src, ctx: &Context) -> anyhow::Result<()> {
        let user = format!("{}\n\nTask:\n{}", self.instruction, render_task(task));
        let request = self.build_request(user)?;
        let response = client::complete(&self.client, request).await?;

        let Some(choice) = response.choices.into_iter().next() else {
            return Ok(());
        };

        if let Some(calls) = choice.message.tool_calls {
            for call in calls {
                let ChatCompletionMessageToolCalls::Function(function) = call else {
                    continue;
                };
                let tool = self
                    .tools
                    .iter()
                    .find(|tool| tool.name == function.function.name)
                    .ok_or_else(|| {
                        anyhow::anyhow!("model called unknown tool {}", function.function.name)
                    })?;
                let args: serde_json::Value = serde_json::from_str(&function.function.arguments)?;
                (tool.emit)(ctx, args).await?;
            }
        }

        if let Some(text) = choice.message.content
            && !text.trim().is_empty()
            && let Some(on_text) = &self.on_text
        {
            on_text(&text)?;
        }
        Ok(())
    }
}
