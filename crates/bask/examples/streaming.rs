/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Streaming a columnar file through a pipeline. Writes a Parquet file, then a
//! reader worker streams it in chunks (one RecordBatch at a time) and a summer
//! worker reduces each chunk, so the whole file never needs to be resident.
//!
//! Run with: cargo run --example streaming --features formats
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bask::formats::{for_path, Chunk, Format, ParquetFormat};
use bask::prelude::*;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
}

fn batch(start: i64, len: i64) -> RecordBatch {
    let column = Int64Array::from((start..start + len).collect::<Vec<_>>());
    RecordBatch::try_new(schema(), vec![Arc::new(column) as ArrayRef]).unwrap()
}

struct ReadFile {
    path: PathBuf,
    chunk_rows: usize,
}
struct Batch(Chunk);

struct Reader;
#[async_trait]
impl Worker for Reader {
    type Task = ReadFile;
    async fn process(&self, task: &ReadFile, ctx: &Context) -> anyhow::Result<()> {
        let mut reader = for_path(&task.path)?.open_reader(&task.path, task.chunk_rows)?;
        while let Some(chunk) = reader.next_chunk()? {
            ctx.emit(Batch(chunk)).await?;
        }
        Ok(())
    }
}

struct Summer;
#[async_trait]
impl Worker for Summer {
    type Task = Batch;
    async fn process(&self, batch: &Batch, ctx: &Context) -> anyhow::Result<()> {
        let column =
            batch.0.column(0).as_any().downcast_ref::<Int64Array>().expect("int64 column");
        let sum: i64 = column.iter().flatten().sum();
        ctx.aggregate::<Total>((batch.0.num_rows() as u64, sum));
        Ok(())
    }
}

struct Total;
impl Aggregator for Total {
    type Input = (u64, i64);
    type State = (u64, i64);
    type Output = (u64, i64);
    fn fold(state: &mut Self::State, input: Self::Input) {
        state.0 += input.0;
        state.1 += input.1;
    }
    fn merge(left: &mut Self::State, right: Self::State) {
        left.0 += right.0;
        left.1 += right.1;
    }
    fn finalize(state: Self::State) -> Self::Output {
        state
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Write 10k rows to a Parquet file in ten batches.
    let path = std::env::temp_dir().join("bask_streaming.parquet");
    let mut writer = ParquetFormat.open_writer(&path, schema())?;
    for i in 0..10 {
        writer.write(&batch(i * 1000, 1000))?;
    }
    writer.finish()?;

    // Stream it back through the pipeline in 1024-row chunks.
    let report = Engine::builder()
        .worker(Reader)
        .worker(Summer)
        .aggregator::<Total>()
        .seed(ReadFile { path: path.clone(), chunk_rows: 1024 })
        .run()
        .await?;

    std::fs::remove_file(&path).ok();
    let (rows, sum) = report.output::<Total>().copied().unwrap();
    println!("streamed {rows} rows, sum of n = {sum}");
    Ok(())
}
