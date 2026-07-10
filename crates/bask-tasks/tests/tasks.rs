/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use arrow::array::{ArrayRef, Int64Array};
use arrow::record_batch::RecordBatch;
use bask_core::prelude::*;
use bask_tasks::{Batch, Chunker, Piece, RowBatch, Whole};

fn batch(rows: i64) -> RecordBatch {
    let col = Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())) as ArrayRef;
    RecordBatch::try_from_iter([("n", col)]).unwrap()
}

struct CountPieces(Arc<AtomicUsize>, Arc<AtomicUsize>);
#[async_trait]
impl Worker for CountPieces {
    type Task = Piece;
    async fn process(&self, piece: &Piece, _ctx: &Context) -> anyhow::Result<()> {
        self.0.fetch_add(1, SeqCst);
        self.1.fetch_add(piece.0.num_rows(), SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn chunker_splits_a_batch_into_fixed_pieces() {
    let pieces = Arc::new(AtomicUsize::new(0));
    let rows = Arc::new(AtomicUsize::new(0));
    let report = Engine::builder()
        .worker(Chunker::<3>)
        .worker(CountPieces(pieces.clone(), rows.clone()))
        .concurrency(1)
        .seed(Whole(batch(10)))
        .run()
        .await
        .unwrap();

    assert_eq!(report.stats.failed, 0);
    assert_eq!(pieces.load(SeqCst), 4, "10 rows in pieces of 3 = 4 pieces");
    assert_eq!(rows.load(SeqCst), 10);
}

struct Feed;
#[async_trait]
impl Worker for Feed {
    type Task = Piece;
    async fn process(&self, piece: &Piece, ctx: &Context) -> anyhow::Result<()> {
        ctx.route::<RowBatch<4>>(piece.0.clone()).await?;
        Ok(())
    }
}

struct SumRows(Arc<AtomicUsize>);
#[async_trait]
impl Worker for SumRows {
    type Task = Batch;
    async fn process(&self, group: &Batch, _ctx: &Context) -> anyhow::Result<()> {
        self.0.fetch_add(group.0.num_rows(), SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn rowbatch_reaggregates_pieces_and_flushes_the_remainder() {
    let rows = Arc::new(AtomicUsize::new(0));
    let report = Engine::builder()
        .worker(Chunker::<3>)
        .worker(Feed)
        .worker(SumRows(rows.clone()))
        .router::<RowBatch<4>>()
        .concurrency(1)
        .seed(Whole(batch(10)))
        .run()
        .await
        .unwrap();

    assert_eq!(report.stats.failed, 0);
    assert_eq!(rows.load(SeqCst), 10, "every row flows through the groups");
    assert_eq!(
        report.output::<RowBatch<4>>().copied(),
        Some(2),
        "10 rows in groups of >=4 = 2 groups"
    );
}
