/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! CSV format (`.csv`), read in chunks with an inferred schema and written with a header.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use arrow::csv::reader::Format as CsvReaderFormat;
use arrow::csv::{ReaderBuilder, Writer, WriterBuilder};
use arrow::datatypes::SchemaRef;

use super::rebatch::ReBatch;
use super::{Chunk, ChunkReader, ChunkWriter, Format};

pub struct CsvFormat;

impl Format for CsvFormat {
    fn extensions(&self) -> &'static [&'static str] {
        &["csv"]
    }

    fn open_reader(&self, path: &Path, chunk_rows: usize) -> anyhow::Result<Box<dyn ChunkReader>> {
        let (schema, _) = CsvReaderFormat::default()
            .with_header(true)
            .infer_schema(BufReader::new(File::open(path)?), Some(1024))?;
        let schema: SchemaRef = Arc::new(schema);
        let reader = ReaderBuilder::new(schema.clone())
            .with_header(true)
            .with_batch_size(chunk_rows)
            .build(BufReader::new(File::open(path)?))?;
        Ok(Box::new(ReBatch::new(reader, schema, chunk_rows)))
    }

    fn open_writer(&self, path: &Path, _schema: SchemaRef) -> anyhow::Result<Box<dyn ChunkWriter>> {
        let writer = WriterBuilder::new()
            .with_header(true)
            .build(File::create(path)?);
        Ok(Box::new(CsvChunkWriter {
            writer: Some(writer),
        }))
    }
}

struct CsvChunkWriter {
    writer: Option<Writer<File>>,
}

impl ChunkWriter for CsvChunkWriter {
    fn write(&mut self, chunk: &Chunk) -> anyhow::Result<()> {
        self.writer.as_mut().expect("writer open").write(chunk)?;
        Ok(())
    }
    fn finish(mut self: Box<Self>) -> anyhow::Result<()> {
        self.writer.take();
        Ok(())
    }
}
