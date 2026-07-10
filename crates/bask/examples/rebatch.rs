/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Predefined tasks from `bask::tasks`: a large record batch is split into fixed-row
//! `Piece`s by a `Chunker`, then a `RowBatch` router re-aggregates them into groups of at
//! least 100 rows, flushing the trailing group. The pipeline only wires the stages.
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::record_batch::RecordBatch;
use bask::prelude::*;
use bask::tasks::{Batch, Chunker, Piece, RowBatch, Whole};

fn batch(rows: i64) -> RecordBatch {
    let col = Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())) as ArrayRef;
    RecordBatch::try_from_iter([("n", col)]).unwrap()
}

/// Routes each 32-row piece into the row-count aggregator.
struct Feed;
#[async_trait]
impl Worker for Feed {
    type Task = Piece;
    async fn process(&self, piece: &Piece, ctx: &Context) -> anyhow::Result<()> {
        ctx.route::<RowBatch<100>>(piece.0.clone()).await?;
        Ok(())
    }
}

/// Consumes each re-aggregated group.
struct Show;
#[async_trait]
impl Worker for Show {
    type Task = Batch;
    async fn process(&self, group: &Batch, _ctx: &Context) -> anyhow::Result<()> {
        println!("group of {} rows", group.0.num_rows());
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let report = Engine::builder()
        .worker(Chunker::<32>)
        .worker(Feed)
        .worker(Show)
        .router::<RowBatch<100>>()
        .concurrency(1)
        .seed(Whole(batch(250)))
        .run()
        .await?;

    println!(
        "groups emitted: {}",
        report.output::<RowBatch<100>>().unwrap()
    );
    Ok(())
}
