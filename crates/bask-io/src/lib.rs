/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Pluggable IO plane: [`Source`]s stream keyed items into a pipeline and [`Sink`]s
//! drain them out, both generic over the item type and decoupled from any one format.
//! A [`SourceRegistry`]/[`SinkRegistry`] selects an implementation by file extension or
//! URI scheme, so adding a format means implementing one trait and registering it.

use async_trait::async_trait;

mod blob;
mod registry;
mod worker;

#[cfg(feature = "dataset")]
mod dataset;

#[cfg(feature = "download")]
mod download;

#[cfg(feature = "object-store")]
mod objectstore;

pub use bytes::Bytes;

pub use blob::{FileBlobSink, FileBlobSource};
pub use registry::{SinkRegistry, SourceRegistry, Target};
pub use worker::{Read, SinkWorker, SourceWorker};

#[cfg(feature = "dataset")]
pub use dataset::FileDataset;

#[cfg(feature = "download")]
pub use download::HttpSource;

#[cfg(feature = "object-store")]
pub use objectstore::{ObjectStoreSink, ObjectStoreSource};

/// A stable per-item identity, used for logging and, later, dedup and resume (#7).
pub type Key = std::sync::Arc<str>;

/// An item paired with its stable [`Key`]; the unit that flows through the IO plane.
#[derive(Clone)]
pub struct Keyed<T> {
    pub key: Key,
    pub value: T,
}

impl<T> Keyed<T> {
    pub fn new(key: impl Into<Key>, value: T) -> Self {
        Keyed {
            key: key.into(),
            value,
        }
    }
}

/// A blob of bytes with a key: the item type of the byte-oriented IO plane.
pub type Blob = Keyed<Bytes>;

/// How a source chunks its input; formats that read row groups honour `chunk_rows`.
#[derive(Clone, Copy)]
pub struct ReadOptions {
    pub chunk_rows: usize,
}

impl Default for ReadOptions {
    fn default() -> Self {
        ReadOptions { chunk_rows: 8192 }
    }
}

/// How a sink rotates its output; `None` fields mean a single unrotated target.
#[derive(Clone, Copy, Default)]
pub struct WriteOptions {
    pub rotate_rows: Option<usize>,
}

/// Lazily yields keyed items; only one item need be resident at a time.
#[async_trait]
pub trait Source<Item>: Send {
    async fn next(&mut self) -> anyhow::Result<Option<Keyed<Item>>>;
}

/// Consumes keyed items with chunked/rotating output; [`finish`](Sink::finish) flushes.
#[async_trait]
pub trait Sink<Item>: Send {
    async fn write(&mut self, item: &Keyed<Item>) -> anyhow::Result<()>;
    async fn finish(&mut self) -> anyhow::Result<()>;
}

impl SourceRegistry<Bytes> {
    /// A blob source registry with every built-in the active features allow: `file`
    /// (single file or directory tree), plus `http`/`https` and object-store schemes.
    pub fn blobs() -> Self {
        let mut registry = Self::new();
        blob::register_source_builtins(&mut registry);
        #[cfg(feature = "download")]
        download::register_source_builtins(&mut registry);
        #[cfg(feature = "object-store")]
        objectstore::register_source_builtins(&mut registry);
        registry
    }
}

impl SinkRegistry<Bytes> {
    /// A blob sink registry with every built-in the active features allow: `file`
    /// (directory of blobs), plus object-store schemes.
    pub fn blobs() -> Self {
        let mut registry = Self::new();
        blob::register_sink_builtins(&mut registry);
        #[cfg(feature = "object-store")]
        objectstore::register_sink_builtins(&mut registry);
        registry
    }
}
