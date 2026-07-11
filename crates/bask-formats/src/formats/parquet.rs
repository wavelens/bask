/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Parquet format (`.parquet`), read and written in chunks.

use std::fs::File;
use std::path::Path;

use arrow::array::RecordBatchReader;
use arrow::datatypes::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use super::{Chunk, ChunkReader, ChunkWriter, Format};

pub struct ParquetFormat;

impl Format for ParquetFormat {
    fn extensions(&self) -> &'static [&'static str] {
        &["parquet"]
    }

    fn open_reader(&self, path: &Path, chunk_rows: usize) -> anyhow::Result<Box<dyn ChunkReader>> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
            .with_batch_size(chunk_rows)
            .build()?;
        Ok(Box::new(ParquetChunkReader { reader }))
    }

    fn open_writer(&self, path: &Path, schema: SchemaRef) -> anyhow::Result<Box<dyn ChunkWriter>> {
        let writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
        Ok(Box::new(ParquetChunkWriter {
            writer: Some(writer),
        }))
    }
}

/// Serialize a batch to a self-contained Parquet buffer (one row group); the inverse of
/// [`read_parquet_bytes`]. A dataset-backed checkpoint encodes its shard with this.
pub fn to_parquet_bytes(batch: &Chunk) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    Ok(buf)
}

/// Read every batch from a Parquet buffer, e.g. a shard read back from a [`Dataset`].
///
/// [`Dataset`]: bask_core::Dataset
pub fn read_parquet_bytes(bytes: &[u8]) -> anyhow::Result<Vec<Chunk>> {
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))?.build()?;
    Ok(reader.collect::<Result<Vec<_>, _>>()?)
}

struct ParquetChunkReader {
    reader: ParquetRecordBatchReader,
}

impl ChunkReader for ParquetChunkReader {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
    fn next_chunk(&mut self) -> anyhow::Result<Option<Chunk>> {
        match self.reader.next() {
            Some(batch) => Ok(Some(batch?)),
            None => Ok(None),
        }
    }
}

struct ParquetChunkWriter {
    writer: Option<ArrowWriter<File>>,
}

impl ChunkWriter for ParquetChunkWriter {
    fn write(&mut self, chunk: &Chunk) -> anyhow::Result<()> {
        self.writer.as_mut().expect("writer open").write(chunk)?;
        Ok(())
    }
    fn finish(mut self: Box<Self>) -> anyhow::Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.close()?;
        }
        Ok(())
    }
}
