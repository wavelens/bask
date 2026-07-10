/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! The byte-oriented plane: read a file or a directory tree of blobs, write each blob
//! back out under its key. Images, audio and arbitrary payloads travel as [`Bytes`].

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;

use super::{Keyed, ReadOptions, Sink, SinkRegistry, Source, SourceRegistry, WriteOptions};

/// Yields the bytes of a single file, or of every file under a directory tree, keyed by
/// path relative to the opened root.
pub struct FileBlobSource {
    root: PathBuf,
    files: std::vec::IntoIter<PathBuf>,
}

impl FileBlobSource {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let (root, mut files) = if path.is_dir() {
            let mut collected = Vec::new();
            collect_files(path, &mut collected)?;
            (path.to_path_buf(), collected)
        } else {
            let parent = path.parent().unwrap_or_else(|| Path::new("")).to_path_buf();
            (parent, vec![path.to_path_buf()])
        };
        files.sort();
        Ok(FileBlobSource {
            root,
            files: files.into_iter(),
        })
    }
}

#[async_trait]
impl Source<Bytes> for FileBlobSource {
    async fn next(&mut self) -> anyhow::Result<Option<Keyed<Bytes>>> {
        let Some(path) = self.files.next() else {
            return Ok(None);
        };
        let key = path
            .strip_prefix(&self.root)
            .unwrap_or(&path)
            .to_string_lossy()
            .into_owned();
        let bytes = tokio::fs::read(&path).await?;
        Ok(Some(Keyed::new(key, Bytes::from(bytes))))
    }
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_files(&path, out)?;
        } else {
            out.push(path);
        }
    }
    Ok(())
}

/// Writes each blob to `root/<key>`, creating parent directories. Keys are treated as
/// relative paths; leading separators and `..` components are stripped for safety.
pub struct FileBlobSink {
    root: PathBuf,
}

impl FileBlobSink {
    pub fn open(root: &Path) -> Self {
        FileBlobSink {
            root: root.to_path_buf(),
        }
    }
}

#[async_trait]
impl Sink<Bytes> for FileBlobSink {
    async fn write(&mut self, item: &Keyed<Bytes>) -> anyhow::Result<()> {
        let dest = self.root.join(sanitize(&item.key));
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&dest, &item.value).await?;
        Ok(())
    }

    async fn finish(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

fn sanitize(key: &str) -> PathBuf {
    Path::new(key)
        .components()
        .filter(|c| matches!(c, std::path::Component::Normal(_)))
        .collect()
}

/// Registers the `file` scheme (single file or directory tree) as a blob source.
pub fn register_source_builtins(registry: &mut SourceRegistry<Bytes>) {
    registry.register_scheme(&["file"], |t, _opts: &ReadOptions| {
        Ok(Box::new(FileBlobSource::open(t.path())?) as Box<dyn Source<Bytes>>)
    });
}

/// Registers the `file` scheme (directory of blobs) as a blob sink.
pub fn register_sink_builtins(registry: &mut SinkRegistry<Bytes>) {
    registry.register_scheme(&["file"], |t, _opts: &WriteOptions| {
        Ok(Box::new(FileBlobSink::open(t.path())) as Box<dyn Sink<Bytes>>)
    });
}
