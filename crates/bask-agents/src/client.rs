/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use async_openai::Client;
use async_openai::config::OpenAIConfig;
use async_openai::error::OpenAIError;
use async_openai::types::chat::{CreateChatCompletionRequest, CreateChatCompletionResponse};
use bask_core::RetryExt;

/// Build a client for an OpenAI-compatible endpoint. A `None` base_url or api_key falls back
/// to async-openai's defaults (the public API and the `OPENAI_API_KEY` env var).
pub(crate) fn build_client(base_url: Option<&str>, api_key: Option<&str>) -> Client<OpenAIConfig> {
    let mut config = OpenAIConfig::new();
    if let Some(key) = api_key {
        config = config.with_api_key(key);
    }
    if let Some(base) = base_url {
        config = config.with_api_base(base);
    }
    Client::with_config(config)
}

/// Issue one chat completion. Transport errors, 429, and 5xx are retryable; every other API
/// error is terminal (marked fatal so it skips retry under any policy).
/// async-openai's client also retries 429/5xx a few times internally, beneath this
/// classification, so bask-core's RetryPolicy governs only what survives that layer.
pub(crate) async fn complete(
    client: &Client<OpenAIConfig>,
    request: CreateChatCompletionRequest,
) -> anyhow::Result<CreateChatCompletionResponse> {
    match client.chat().create(request).await {
        Ok(response) => Ok(response),
        Err(err) if is_retryable(&err) => Err(anyhow::Error::new(err)),
        Err(err) => Err::<CreateChatCompletionResponse, _>(anyhow::Error::new(err)).fatal(),
    }
}

fn is_retryable(err: &OpenAIError) -> bool {
    match err {
        OpenAIError::Reqwest(_) => true,
        OpenAIError::ApiError(response) => retryable_status(response.status_code.as_u16()),
        _ => false,
    }
}

fn retryable_status(status: u16) -> bool {
    status == 429 || (500..600).contains(&status)
}

#[cfg(test)]
mod tests {
    use super::retryable_status;

    #[test]
    fn classifies_statuses() {
        assert!(retryable_status(429));
        assert!(retryable_status(500));
        assert!(retryable_status(503));
        assert!(!retryable_status(400));
        assert!(!retryable_status(401));
        assert!(!retryable_status(404));
    }
}
