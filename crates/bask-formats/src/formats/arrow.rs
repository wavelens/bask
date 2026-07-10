/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Arrow IPC file format (`.arrow`, `.ipc`), read and written in chunks.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;

use super::rebatch::ReBatch;
use super::{Chunk, ChunkReader, ChunkWriter, Format};

pub struct ArrowFormat;

impl Format for ArrowFormat {
    fn extensions(&self) -> &'static [&'static str] {
        &["arrow", "ipc"]
    }

    fn open_reader(&self, path: &Path, chunk_rows: usize) -> anyhow::Result<Box<dyn ChunkReader>> {
        let reader = FileReader::try_new(BufReader::new(File::open(path)?), None)?;
        let schema = reader.schema();
        Ok(Box::new(ReBatch::new(reader, schema, chunk_rows)))
    }

    fn open_writer(&self, path: &Path, schema: SchemaRef) -> anyhow::Result<Box<dyn ChunkWriter>> {
        let writer = FileWriter::try_new(File::create(path)?, &schema)?;
        Ok(Box::new(ArrowIpcWriter {
            writer: Some(writer),
        }))
    }
}

struct ArrowIpcWriter {
    writer: Option<FileWriter<File>>,
}

impl ChunkWriter for ArrowIpcWriter {
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
