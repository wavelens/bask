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
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;

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
        Ok(Box::new(ParquetChunkWriter { writer: Some(writer) }))
    }
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
