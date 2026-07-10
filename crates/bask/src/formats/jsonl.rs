/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Newline-delimited JSON (`.jsonl`, `.ndjson`, `.json`), read in chunks with an
//! inferred schema and written one object per line.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::json::reader::infer_json_schema_from_seekable;
use arrow::json::{LineDelimitedWriter, ReaderBuilder};

use super::rebatch::ReBatch;
use super::{Chunk, ChunkReader, ChunkWriter, Format};

pub struct JsonlFormat;

impl Format for JsonlFormat {
    fn extensions(&self) -> &'static [&'static str] {
        &["jsonl", "ndjson", "json"]
    }

    fn open_reader(&self, path: &Path, chunk_rows: usize) -> anyhow::Result<Box<dyn ChunkReader>> {
        let (schema, _) =
            infer_json_schema_from_seekable(BufReader::new(File::open(path)?), Some(1024))?;
        let schema: SchemaRef = Arc::new(schema);
        let reader = ReaderBuilder::new(schema.clone())
            .with_batch_size(chunk_rows)
            .build(BufReader::new(File::open(path)?))?;
        Ok(Box::new(ReBatch::new(reader, schema, chunk_rows)))
    }

    fn open_writer(&self, path: &Path, _schema: SchemaRef) -> anyhow::Result<Box<dyn ChunkWriter>> {
        Ok(Box::new(JsonlChunkWriter {
            writer: Some(LineDelimitedWriter::new(File::create(path)?)),
        }))
    }
}

struct JsonlChunkWriter {
    writer: Option<LineDelimitedWriter<File>>,
}

impl ChunkWriter for JsonlChunkWriter {
    fn write(&mut self, chunk: &Chunk) -> anyhow::Result<()> {
        self.writer.as_mut().expect("writer open").write(chunk)?;
        Ok(())
    }
    fn finish(mut self: Box<Self>) -> anyhow::Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.finish()?;
        }
        Ok(())
    }
}
