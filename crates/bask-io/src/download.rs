/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! HTTP(S) download source. One [`HttpSource`] fetches one URL; concurrency, retry and
//! backpressure come from the engine dispatching many [`Read`](super::Read) tasks. An
//! optional cache directory makes fetches resumable: a URL already on disk is served
//! from there instead of the network (a lightweight stand-in for the #7 content store).

use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;

use super::{Keyed, ReadOptions, Source, SourceRegistry, Target};

pub struct HttpSource {
    client: reqwest::Client,
    url: String,
    cache: Option<PathBuf>,
    done: bool,
}

impl HttpSource {
    pub fn open(url: impl Into<String>) -> Self {
        HttpSource {
            client: reqwest::Client::new(),
            url: url.into(),
            cache: None,
            done: false,
        }
    }

    /// Serve from, and populate, a local cache directory so re-runs skip re-fetching.
    pub fn with_cache(mut self, dir: impl Into<PathBuf>) -> Self {
        self.cache = Some(dir.into());
        self
    }

    fn cache_path(&self) -> Option<PathBuf> {
        self.cache
            .as_ref()
            .map(|dir| dir.join(cache_name(&self.url)))
    }
}

#[async_trait]
impl Source<Bytes> for HttpSource {
    async fn next(&mut self) -> anyhow::Result<Option<Keyed<Bytes>>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let key = self.url.clone();

        if let Some(path) = self.cache_path()
            && tokio::fs::try_exists(&path).await.unwrap_or(false)
        {
            let bytes = tokio::fs::read(&path).await?;
            return Ok(Some(Keyed::new(key, Bytes::from(bytes))));
        }

        let response = self
            .client
            .get(&self.url)
            .send()
            .await?
            .error_for_status()?;
        let bytes = response.bytes().await?;

        if let Some(path) = self.cache_path() {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await.ok();
            }
            tokio::fs::write(&path, &bytes).await.ok();
        }
        Ok(Some(Keyed::new(key, bytes)))
    }
}

fn cache_name(url: &str) -> String {
    let mut name: String = url
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    name.truncate(180);
    name
}

pub fn register_source_builtins(registry: &mut SourceRegistry<Bytes>) {
    registry.register_scheme(&["http", "https"], |t: &Target, _o: &ReadOptions| {
        Ok(Box::new(HttpSource::open(t.raw.clone())) as Box<dyn Source<Bytes>>)
    });
}
