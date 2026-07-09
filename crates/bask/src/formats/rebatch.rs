/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Re-chunks an arbitrary RecordBatch iterator into fixed-size chunks, so formats
//! whose on-disk batch sizes differ from the requested `chunk_rows` still stream
//! uniform chunks.

use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use super::{Chunk, ChunkReader};

pub(crate) struct ReBatch<I> {
    inner: I,
    schema: SchemaRef,
    target: usize,
    buffer: Vec<RecordBatch>,
    buffered_rows: usize,
    exhausted: bool,
}

impl<I> ReBatch<I> {
    pub fn new(inner: I, schema: SchemaRef, chunk_rows: usize) -> Self {
        ReBatch {
            inner,
            schema,
            target: chunk_rows.max(1),
            buffer: Vec::new(),
            buffered_rows: 0,
            exhausted: false,
        }
    }
}

impl<I> ChunkReader for ReBatch<I>
where
    I: Iterator<Item = Result<RecordBatch, ArrowError>> + Send,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_chunk(&mut self) -> anyhow::Result<Option<Chunk>> {
        while !self.exhausted && self.buffered_rows < self.target {
            match self.inner.next() {
                Some(batch) => {
                    let batch = batch?;
                    if batch.num_rows() > 0 {
                        self.buffered_rows += batch.num_rows();
                        self.buffer.push(batch);
                    }
                }
                None => self.exhausted = true,
            }
        }
        if self.buffered_rows == 0 {
            return Ok(None);
        }

        let want = self.target.min(self.buffered_rows);
        let mut collected: Vec<RecordBatch> = Vec::new();
        let mut taken = 0;
        while taken < want {
            let batch = self.buffer.remove(0);
            let need = want - taken;
            if batch.num_rows() <= need {
                taken += batch.num_rows();
                collected.push(batch);
            } else {
                collected.push(batch.slice(0, need));
                self.buffer.insert(0, batch.slice(need, batch.num_rows() - need));
                taken += need;
            }
        }
        self.buffered_rows -= want;
        Ok(Some(concat_batches(&self.schema, &collected)?))
    }
}
