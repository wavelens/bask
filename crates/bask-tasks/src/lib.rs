/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Predefined building blocks that compose on the bask engine: a [`Chunker`] worker that
//! splits a record batch into fixed-row pieces, and a [`RowBatch`] router that aggregates
//! a batch stream back into groups of at least `ROWS` rows. Both are parameterized by a
//! const row count, so `Chunker::<8192>` and `RowBatch::<8192>` are distinct pipeline
//! stages. Most users reach these via `bask::tasks`.

use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bask_core::prelude::*;

/// A record batch to split into fixed-row [`Piece`]s; seed or emit one to feed a [`Chunker`].
pub struct Whole(pub RecordBatch);

/// One fixed-row piece a [`Chunker`] emits (the last piece of a batch may be smaller).
pub struct Piece(pub RecordBatch);

/// A group of at least `ROWS` rows a [`RowBatch`] router emits (the trailing group may be
/// smaller). Concatenated into a single batch when the shard's batches share a schema.
pub struct Batch(pub RecordBatch);

/// Splits `batch` into consecutive zero-copy slices of at most `rows` rows each. The
/// building block behind [`Chunker`]; front-ends (e.g. the Python bindings) call it to
/// reuse the stage without the worker wrapper.
pub fn chunk(batch: &RecordBatch, rows: usize) -> Vec<RecordBatch> {
    let rows = rows.max(1);
    let total = batch.num_rows();
    (0..total)
        .step_by(rows)
        .map(|offset| batch.slice(offset, rows.min(total - offset)))
        .collect()
}

/// Splits each [`Whole`] into zero-copy [`Piece`]s of at most `ROWS` rows.
pub struct Chunker<const ROWS: usize>;

#[async_trait]
impl<const ROWS: usize> Worker for Chunker<ROWS> {
    type Task = Whole;
    async fn process(&self, whole: &Whole, ctx: &Context) -> anyhow::Result<()> {
        for piece in chunk(&whole.0, ROWS) {
            ctx.emit(Piece(piece)).await?;
        }
        Ok(())
    }
}

/// Sharded accumulator for [`RowBatch`]: buffers batches until they reach the row target.
#[derive(Default)]
pub struct RowBatchState {
    schema: Option<SchemaRef>,
    buffer: Vec<RecordBatch>,
    rows: usize,
    groups: usize,
}

impl RowBatchState {
    fn drain(&mut self, out: &mut Emit) {
        if self.buffer.is_empty() {
            return;
        }
        let batches = std::mem::take(&mut self.buffer);
        self.rows = 0;
        self.groups += 1;
        match self.schema.as_ref().map(|s| concat_batches(s, &batches)) {
            Some(Ok(batch)) => out.emit(Batch(batch)),
            _ => batches.into_iter().for_each(|b| out.emit(Batch(b))),
        }
    }
}

/// Aggregates a stream of record batches into [`Batch`]es of at least `ROWS` rows, routing
/// each full group downstream and flushing the trailing partial group at end-of-run. The
/// terminal output is the number of groups emitted.
pub struct RowBatch<const ROWS: usize>;

impl<const ROWS: usize> Router for RowBatch<ROWS> {
    type Input = RecordBatch;
    type State = RowBatchState;
    type Output = usize;

    fn route(state: &mut Self::State, batch: RecordBatch, out: &mut Emit) {
        if batch.num_rows() == 0 {
            return;
        }
        state.schema.get_or_insert_with(|| batch.schema());
        state.rows += batch.num_rows();
        state.buffer.push(batch);
        if state.rows >= ROWS.max(1) {
            state.drain(out);
        }
    }

    fn merge(left: &mut Self::State, right: Self::State) {
        left.groups += right.groups;
        left.rows += right.rows;
        left.buffer.extend(right.buffer);
        if left.schema.is_none() {
            left.schema = right.schema;
        }
    }

    fn flush(state: &mut Self::State, out: &mut Emit) {
        state.drain(out);
    }

    fn finalize(state: Self::State) -> Self::Output {
        state.groups
    }
}
