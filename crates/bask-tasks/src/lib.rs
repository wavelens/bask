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

/// A runtime row-count aggregator: push batches and get a full group back once the target
/// is reached, then [`flush`](RowAggregator::flush) the remainder at end of stream. Shared
/// by the [`RowBatch`] router and dynamic front-ends (the Python `row_batch` stage), so the
/// aggregation lives in Rust either way.
pub struct RowAggregator {
    target: usize,
    schema: Option<SchemaRef>,
    buffer: Vec<RecordBatch>,
    rows: usize,
}

impl RowAggregator {
    pub fn new(target: usize) -> Self {
        RowAggregator {
            target: target.max(1),
            schema: None,
            buffer: Vec::new(),
            rows: 0,
        }
    }

    /// Push a batch; returns a full group once the target is reached (as one concatenated
    /// batch, or the raw batches if their schemas disagree), else nothing.
    pub fn push(&mut self, batch: RecordBatch) -> Vec<RecordBatch> {
        if batch.num_rows() == 0 {
            return Vec::new();
        }
        self.schema.get_or_insert_with(|| batch.schema());
        self.rows += batch.num_rows();
        self.buffer.push(batch);
        if self.rows >= self.target {
            self.take()
        } else {
            Vec::new()
        }
    }

    /// Drain any buffered rows into a final group.
    pub fn flush(&mut self) -> Vec<RecordBatch> {
        self.take()
    }

    fn take(&mut self) -> Vec<RecordBatch> {
        if self.buffer.is_empty() {
            return Vec::new();
        }
        let batches = std::mem::take(&mut self.buffer);
        self.rows = 0;
        match self.schema.as_ref().map(|s| concat_batches(s, &batches)) {
            Some(Ok(batch)) => vec![batch],
            _ => batches,
        }
    }
}

/// Sharded state for [`RowBatch`]: a lazily-sized aggregator plus the group count.
#[derive(Default)]
pub struct RowBatchState {
    agg: Option<RowAggregator>,
    groups: usize,
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
        let group = state
            .agg
            .get_or_insert_with(|| RowAggregator::new(ROWS))
            .push(batch);
        if !group.is_empty() {
            state.groups += 1;
            group.into_iter().for_each(|b| out.emit(Batch(b)));
        }
    }

    fn merge(left: &mut Self::State, right: Self::State) {
        left.groups += right.groups;
    }

    fn flush(state: &mut Self::State, out: &mut Emit) {
        if let Some(group) = state.agg.as_mut().map(RowAggregator::flush)
            && !group.is_empty()
        {
            state.groups += 1;
            group.into_iter().for_each(|b| out.emit(Batch(b)));
        }
    }

    fn finalize(state: Self::State) -> Self::Output {
        state.groups
    }
}
