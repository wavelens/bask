/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Object-store plane over any [`object_store`] backend (S3, GCS, Azure, local). The
//! source lists a prefix and streams each object as a blob; the sink puts each blob
//! under a prefix. `s3://`, `gs://` and `az://` targets are wired into the blob
//! registry; any backend can also be used directly via [`with_store`](ObjectStoreSource::with_store).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, parse_url};
use url::Url;

use super::{Keyed, Sink, SinkRegistry, Source, SourceRegistry, Target};

/// Streams every object under a prefix. Keys are listed once, then fetched one at a time.
pub struct ObjectStoreSource {
    store: Arc<dyn ObjectStore>,
    prefix: StorePath,
    listed: Option<std::vec::IntoIter<StorePath>>,
}

impl ObjectStoreSource {
    pub fn with_store(store: Arc<dyn ObjectStore>, prefix: StorePath) -> Self {
        ObjectStoreSource {
            store,
            prefix,
            listed: None,
        }
    }

    async fn ensure_listed(&mut self) -> anyhow::Result<()> {
        if self.listed.is_none() {
            let mut stream = self.store.list(Some(&self.prefix));
            let mut paths = Vec::new();
            while let Some(meta) = stream.next().await {
                paths.push(meta?.location);
            }
            paths.sort();
            self.listed = Some(paths.into_iter());
        }
        Ok(())
    }
}

#[async_trait]
impl Source<Bytes> for ObjectStoreSource {
    async fn next(&mut self) -> anyhow::Result<Option<Keyed<Bytes>>> {
        self.ensure_listed().await?;
        let Some(path) = self.listed.as_mut().and_then(Iterator::next) else {
            return Ok(None);
        };
        let bytes = self.store.get(&path).await?.bytes().await?;
        Ok(Some(Keyed::new(path.to_string(), bytes)))
    }
}

/// Puts each blob at `prefix/<key>` in the backing store.
pub struct ObjectStoreSink {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ObjectStoreSink {
    pub fn with_store(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        ObjectStoreSink {
            store,
            prefix: prefix.into(),
        }
    }
}

#[async_trait]
impl Sink<Bytes> for ObjectStoreSink {
    async fn write(&mut self, item: &Keyed<Bytes>) -> anyhow::Result<()> {
        let key = item.key.trim_start_matches('/');
        let path = if self.prefix.is_empty() {
            StorePath::from(key)
        } else {
            StorePath::from(format!("{}/{}", self.prefix.trim_end_matches('/'), key))
        };
        self.store.put(&path, item.value.clone().into()).await?;
        Ok(())
    }

    async fn finish(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

fn store_for(target: &Target) -> anyhow::Result<(Arc<dyn ObjectStore>, StorePath)> {
    let url = Url::parse(&target.raw)?;
    let (store, prefix) = parse_url(&url)?;
    Ok((Arc::from(store), prefix))
}

pub fn register_source_builtins(registry: &mut SourceRegistry<Bytes>) {
    registry.register_scheme(&["s3", "gs", "az", "azure"], |t: &Target, _o| {
        let (store, prefix) = store_for(t)?;
        Ok(Box::new(ObjectStoreSource::with_store(store, prefix)) as Box<dyn Source<Bytes>>)
    });
}

pub fn register_sink_builtins(registry: &mut SinkRegistry<Bytes>) {
    registry.register_scheme(&["s3", "gs", "az", "azure"], |t: &Target, _o| {
        let (store, prefix) = store_for(t)?;
        Ok(
            Box::new(ObjectStoreSink::with_store(store, prefix.to_string()))
                as Box<dyn Sink<Bytes>>,
        )
    });
}
