/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! The record plane: any columnar [`Format`] lifted into the generic IO traits, so
//! Arrow is one adapter rather than the substrate. Registers arrow, parquet, csv and
//! jsonl, and offers a row-count-rotating sink for chunked output.

use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use super::{Keyed, Sink, SinkRegistry, Source, SourceRegistry, Target, WriteOptions};
use crate::formats::{
    ArrowFormat, ChunkReader, ChunkWriter, CsvFormat, Format, JsonlFormat, ParquetFormat,
};

/// Lifts any [`Format`]'s chunk reader into a [`Source`], offloading each blocking read
/// to the runtime's blocking pool and keying chunks by `path#index`.
pub struct FormatSource {
    reader: Option<Box<dyn ChunkReader>>,
    key: String,
    index: usize,
}

impl FormatSource {
    pub fn open(format: Box<dyn Format>, path: &Path, chunk_rows: usize) -> anyhow::Result<Self> {
        Ok(FormatSource {
            reader: Some(format.open_reader(path, chunk_rows)?),
            key: path.to_string_lossy().into_owned(),
            index: 0,
        })
    }
}

#[async_trait]
impl Source<RecordBatch> for FormatSource {
    async fn next(&mut self) -> anyhow::Result<Option<Keyed<RecordBatch>>> {
        let Some(reader) = self.reader.take() else {
            return Ok(None);
        };
        let (reader, chunk) = tokio::task::spawn_blocking(move || {
            let mut reader = reader;
            let chunk = reader.next_chunk();
            (reader, chunk)
        })
        .await?;
        match chunk? {
            Some(batch) => {
                self.reader = Some(reader);
                let key = format!("{}#{}", self.key, self.index);
                self.index += 1;
                Ok(Some(Keyed::new(key, batch)))
            }
            None => Ok(None),
        }
    }
}

/// Lifts any [`Format`]'s chunk writer into a [`Sink`], opening the writer lazily from
/// the first batch's schema.
pub struct FormatSink {
    format: Box<dyn Format>,
    path: PathBuf,
    writer: Option<Box<dyn ChunkWriter>>,
}

impl FormatSink {
    pub fn open(format: Box<dyn Format>, path: &Path) -> Self {
        FormatSink {
            format,
            path: path.to_path_buf(),
            writer: None,
        }
    }
}

#[async_trait]
impl Sink<RecordBatch> for FormatSink {
    async fn write(&mut self, item: &Keyed<RecordBatch>) -> anyhow::Result<()> {
        let batch = item.value.clone();
        if self.writer.is_none() {
            self.writer = Some(self.format.open_writer(&self.path, batch.schema())?);
        }
        let writer = self.writer.take().expect("writer opened above");
        let writer =
            tokio::task::spawn_blocking(move || -> anyhow::Result<Box<dyn ChunkWriter>> {
                let mut writer = writer;
                writer.write(&batch)?;
                Ok(writer)
            })
            .await??;
        self.writer = Some(writer);
        Ok(())
    }

    async fn finish(&mut self) -> anyhow::Result<()> {
        if let Some(writer) = self.writer.take() {
            tokio::task::spawn_blocking(move || writer.finish()).await??;
        }
        Ok(())
    }
}

/// A record sink that rolls to a fresh numbered part file (`stem-00000.ext`) once the
/// current part reaches `rotate_rows`, finalizing each part as it closes.
pub struct RotatingRecordSink {
    format: Box<dyn Format>,
    parent: PathBuf,
    stem: String,
    ext: String,
    rotate_rows: usize,
    part: usize,
    rows_in_part: usize,
    writer: Option<Box<dyn ChunkWriter>>,
}

impl RotatingRecordSink {
    pub fn open(format: Box<dyn Format>, base: &Path, rotate_rows: usize) -> anyhow::Result<Self> {
        let parent = base
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let stem = base
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("part")
            .to_string();
        let ext = base
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("out")
            .to_string();
        Ok(RotatingRecordSink {
            format,
            parent,
            stem,
            ext,
            rotate_rows: rotate_rows.max(1),
            part: 0,
            rows_in_part: 0,
            writer: None,
        })
    }

    fn part_path(&self) -> PathBuf {
        self.parent
            .join(format!("{}-{:05}.{}", self.stem, self.part, self.ext))
    }
}

#[async_trait]
impl Sink<RecordBatch> for RotatingRecordSink {
    async fn write(&mut self, item: &Keyed<RecordBatch>) -> anyhow::Result<()> {
        if self.writer.is_some() && self.rows_in_part >= self.rotate_rows {
            self.finish().await?;
            self.part += 1;
            self.rows_in_part = 0;
        }
        let batch = item.value.clone();
        if self.writer.is_none() {
            self.writer = Some(self.format.open_writer(&self.part_path(), batch.schema())?);
        }
        let rows = batch.num_rows();
        let writer = self.writer.take().expect("writer opened above");
        let writer =
            tokio::task::spawn_blocking(move || -> anyhow::Result<Box<dyn ChunkWriter>> {
                let mut writer = writer;
                writer.write(&batch)?;
                Ok(writer)
            })
            .await??;
        self.writer = Some(writer);
        self.rows_in_part += rows;
        Ok(())
    }

    async fn finish(&mut self) -> anyhow::Result<()> {
        if let Some(writer) = self.writer.take() {
            tokio::task::spawn_blocking(move || writer.finish()).await??;
        }
        Ok(())
    }
}

fn record_sink(
    format: Box<dyn Format>,
    target: &Target,
    options: &WriteOptions,
) -> anyhow::Result<Box<dyn Sink<RecordBatch>>> {
    match options.rotate_rows {
        Some(rows) => Ok(Box::new(RotatingRecordSink::open(
            format,
            target.path(),
            rows,
        )?)),
        None => Ok(Box::new(FormatSink::open(format, target.path()))),
    }
}

impl SourceRegistry<RecordBatch> {
    /// A record source registry for the built-in columnar formats: arrow, parquet, csv, jsonl.
    pub fn formats() -> Self {
        let mut registry = Self::new();
        registry
            .register_ext(&["parquet"], |t, o| {
                Ok(Box::new(FormatSource::open(
                    Box::new(ParquetFormat),
                    t.path(),
                    o.chunk_rows,
                )?) as Box<dyn Source<RecordBatch>>)
            })
            .register_ext(&["arrow", "ipc"], |t, o| {
                Ok(Box::new(FormatSource::open(
                    Box::new(ArrowFormat),
                    t.path(),
                    o.chunk_rows,
                )?) as Box<dyn Source<RecordBatch>>)
            })
            .register_ext(&["csv"], |t, o| {
                Ok(Box::new(FormatSource::open(
                    Box::new(CsvFormat),
                    t.path(),
                    o.chunk_rows,
                )?) as Box<dyn Source<RecordBatch>>)
            })
            .register_ext(&["json", "jsonl", "ndjson"], |t, o| {
                Ok(Box::new(FormatSource::open(
                    Box::new(JsonlFormat),
                    t.path(),
                    o.chunk_rows,
                )?) as Box<dyn Source<RecordBatch>>)
            });
        registry
    }
}

impl SinkRegistry<RecordBatch> {
    /// A record sink registry for the built-in columnar formats: arrow, parquet, csv, jsonl.
    /// Pass [`WriteOptions::rotate_rows`] to roll output into numbered part files.
    pub fn formats() -> Self {
        let mut registry = Self::new();
        registry
            .register_ext(&["parquet"], |t, o| {
                record_sink(Box::new(ParquetFormat), t, o)
            })
            .register_ext(&["arrow", "ipc"], |t, o| {
                record_sink(Box::new(ArrowFormat), t, o)
            })
            .register_ext(&["csv"], |t, o| record_sink(Box::new(CsvFormat), t, o))
            .register_ext(&["json", "jsonl", "ndjson"], |t, o| {
                record_sink(Box::new(JsonlFormat), t, o)
            });
        #[cfg(feature = "postgres")]
        super::postgres::register_sink_builtins(&mut registry);
        registry
    }
}
