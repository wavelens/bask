/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use super::{ReadOptions, Sink, Source, WriteOptions};

/// A parsed IO target: a URI scheme plus its remainder. A bare path is `file`.
pub struct Target {
    pub scheme: String,
    pub rest: String,
    pub raw: String,
}

impl Target {
    pub fn parse(raw: &str) -> Self {
        match raw.split_once("://") {
            Some((scheme, rest)) => Target {
                scheme: scheme.to_ascii_lowercase(),
                rest: rest.to_string(),
                raw: raw.to_string(),
            },
            None => Target {
                scheme: "file".to_string(),
                rest: raw.to_string(),
                raw: raw.to_string(),
            },
        }
    }

    /// Lower-cased file extension of the path portion, if any.
    pub fn extension(&self) -> Option<String> {
        Path::new(&self.rest)
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_ascii_lowercase())
    }

    /// The path portion, meaningful for the `file` scheme.
    pub fn path(&self) -> &Path {
        Path::new(&self.rest)
    }
}

type SourceFactory<Item> =
    dyn Fn(&Target, &ReadOptions) -> anyhow::Result<Box<dyn Source<Item>>> + Send + Sync;
type SinkFactory<Item> =
    dyn Fn(&Target, &WriteOptions) -> anyhow::Result<Box<dyn Sink<Item>>> + Send + Sync;

/// Resolves a target string to a [`Source`] by extension or URI scheme. Register a new
/// format with [`register_ext`](SourceRegistry::register_ext) or
/// [`register_scheme`](SourceRegistry::register_scheme); no core change is needed.
pub struct SourceRegistry<Item> {
    by_ext: HashMap<String, Arc<SourceFactory<Item>>>,
    by_scheme: HashMap<String, Arc<SourceFactory<Item>>>,
}

impl<Item> Default for SourceRegistry<Item> {
    fn default() -> Self {
        SourceRegistry {
            by_ext: HashMap::new(),
            by_scheme: HashMap::new(),
        }
    }
}

impl<Item: 'static> SourceRegistry<Item> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_ext<F>(&mut self, exts: &[&str], factory: F) -> &mut Self
    where
        F: Fn(&Target, &ReadOptions) -> anyhow::Result<Box<dyn Source<Item>>>
            + Send
            + Sync
            + 'static,
    {
        let factory: Arc<SourceFactory<Item>> = Arc::new(factory);
        for ext in exts {
            self.by_ext
                .insert(ext.to_ascii_lowercase(), factory.clone());
        }
        self
    }

    pub fn register_scheme<F>(&mut self, schemes: &[&str], factory: F) -> &mut Self
    where
        F: Fn(&Target, &ReadOptions) -> anyhow::Result<Box<dyn Source<Item>>>
            + Send
            + Sync
            + 'static,
    {
        let factory: Arc<SourceFactory<Item>> = Arc::new(factory);
        for scheme in schemes {
            self.by_scheme
                .insert(scheme.to_ascii_lowercase(), factory.clone());
        }
        self
    }

    /// Open a source for `target`; non-`file` schemes resolve by scheme, files by
    /// extension and then by a `file`-scheme fallback (e.g. a directory source).
    pub fn open(
        &self,
        target: &str,
        options: &ReadOptions,
    ) -> anyhow::Result<Box<dyn Source<Item>>> {
        let t = Target::parse(target);
        if t.scheme != "file" {
            return match self.by_scheme.get(&t.scheme) {
                Some(f) => f(&t, options),
                None => anyhow::bail!("no source registered for scheme {:?}", t.scheme),
            };
        }
        if let Some(ext) = t.extension()
            && let Some(f) = self.by_ext.get(&ext)
        {
            return f(&t, options);
        }
        match self.by_scheme.get("file") {
            Some(f) => f(&t, options),
            None => anyhow::bail!("no source registered for {target:?}"),
        }
    }
}

/// Resolves a target string to a [`Sink`]; the write-side twin of [`SourceRegistry`].
pub struct SinkRegistry<Item> {
    by_ext: HashMap<String, Arc<SinkFactory<Item>>>,
    by_scheme: HashMap<String, Arc<SinkFactory<Item>>>,
}

impl<Item> Default for SinkRegistry<Item> {
    fn default() -> Self {
        SinkRegistry {
            by_ext: HashMap::new(),
            by_scheme: HashMap::new(),
        }
    }
}

impl<Item: 'static> SinkRegistry<Item> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_ext<F>(&mut self, exts: &[&str], factory: F) -> &mut Self
    where
        F: Fn(&Target, &WriteOptions) -> anyhow::Result<Box<dyn Sink<Item>>>
            + Send
            + Sync
            + 'static,
    {
        let factory: Arc<SinkFactory<Item>> = Arc::new(factory);
        for ext in exts {
            self.by_ext
                .insert(ext.to_ascii_lowercase(), factory.clone());
        }
        self
    }

    pub fn register_scheme<F>(&mut self, schemes: &[&str], factory: F) -> &mut Self
    where
        F: Fn(&Target, &WriteOptions) -> anyhow::Result<Box<dyn Sink<Item>>>
            + Send
            + Sync
            + 'static,
    {
        let factory: Arc<SinkFactory<Item>> = Arc::new(factory);
        for scheme in schemes {
            self.by_scheme
                .insert(scheme.to_ascii_lowercase(), factory.clone());
        }
        self
    }

    pub fn open(
        &self,
        target: &str,
        options: &WriteOptions,
    ) -> anyhow::Result<Box<dyn Sink<Item>>> {
        let t = Target::parse(target);
        if t.scheme != "file" {
            return match self.by_scheme.get(&t.scheme) {
                Some(f) => f(&t, options),
                None => anyhow::bail!("no sink registered for scheme {:?}", t.scheme),
            };
        }
        if let Some(ext) = t.extension()
            && let Some(f) = self.by_ext.get(&ext)
        {
            return f(&t, options);
        }
        match self.by_scheme.get("file") {
            Some(f) => f(&t, options),
            None => anyhow::bail!("no sink registered for {target:?}"),
        }
    }
}
