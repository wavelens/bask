/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Convert a CSV file to Parquet through the IO plane: a `SourceWorker` streams the CSV
//! as record batches and a `SinkWorker` writes them out, with the format on each side
//! chosen from its extension by the registry.
//!
//! Run with: cargo run --example io_convert --features formats

use arrow::record_batch::RecordBatch;
use bask::io::{Read, SinkWorker, SourceWorker};
use bask::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::temp_dir();
    let csv = dir.join("bask_io_convert.csv");
    let parquet = dir.join("bask_io_convert.parquet");
    std::fs::write(&csv, "id,name\n1,alice\n2,bob\n3,carol\n")?;

    let sinks = bask::formats::record_sinks();
    let report = Engine::builder()
        .worker(SourceWorker::new(bask::formats::record_sources()))
        .worker_cfg(
            SinkWorker::open(&sinks, parquet.to_str().unwrap())?,
            WorkerCfg::new().label("parquet-out").concurrency(1),
        )
        .seed(Read::<RecordBatch>::new(csv.to_str().unwrap()))
        .run()
        .await?;

    let mut reader = bask::formats::for_path(&parquet)?.open_reader(&parquet, 8192)?;
    let mut rows = 0;
    while let Some(chunk) = reader.next_chunk()? {
        rows += chunk.num_rows();
    }
    std::fs::remove_file(&csv).ok();
    std::fs::remove_file(&parquet).ok();

    println!(
        "csv -> parquet: {rows} rows across {} tasks, {} failures",
        report.stats.processed,
        report.failures.len()
    );
    Ok(())
}
