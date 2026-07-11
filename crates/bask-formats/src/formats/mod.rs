/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Chunked, streaming I/O for columnar file formats. A file is read as a sequence
//! of Arrow [`Chunk`]s (RecordBatches), so data larger than RAM flows through a
//! chunk at a time. Add a format by implementing [`Format`] in its own file.

use std::path::Path;

use ::arrow::datatypes::SchemaRef;
use ::arrow::record_batch::RecordBatch;

pub mod arrow;
pub mod csv;
pub mod jsonl;
pub mod parquet;
mod rebatch;

pub use self::arrow::ArrowFormat;
pub use self::csv::CsvFormat;
pub use self::jsonl::JsonlFormat;
pub use self::parquet::{ParquetFormat, read_parquet_bytes, to_parquet_bytes};

/// One chunk of a stream: a columnar Arrow RecordBatch of a bounded number of rows.
pub type Chunk = RecordBatch;

/// Reads a file lazily as chunks; only one chunk need be resident at a time.
pub trait ChunkReader: Send {
    fn schema(&self) -> SchemaRef;
    fn next_chunk(&mut self) -> anyhow::Result<Option<Chunk>>;
}

/// Appends chunks to an output file.
pub trait ChunkWriter: Send {
    fn write(&mut self, chunk: &Chunk) -> anyhow::Result<()>;
    fn finish(self: Box<Self>) -> anyhow::Result<()>;
}

/// A columnar file format. Implement this to support a new format.
pub trait Format: Send + Sync + 'static {
    /// File extensions this format handles, e.g. `["arrow", "ipc"]` or `["parquet"]`.
    fn extensions(&self) -> &'static [&'static str];
    /// Open a reader that yields chunks of about `chunk_rows` rows each.
    fn open_reader(&self, path: &Path, chunk_rows: usize) -> anyhow::Result<Box<dyn ChunkReader>>;
    /// Open a writer for `path` with the given `schema`.
    fn open_writer(&self, path: &Path, schema: SchemaRef) -> anyhow::Result<Box<dyn ChunkWriter>>;
}

/// Pick a built-in format by the path's file extension.
pub fn for_path(path: &Path) -> anyhow::Result<Box<dyn Format>> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default();
    let formats: [Box<dyn Format>; 4] = [
        Box::new(ArrowFormat),
        Box::new(ParquetFormat),
        Box::new(CsvFormat),
        Box::new(JsonlFormat),
    ];
    for format in formats {
        if format.extensions().contains(&ext) {
            return Ok(format);
        }
    }
    anyhow::bail!("no format registered for extension {ext:?}")
}
