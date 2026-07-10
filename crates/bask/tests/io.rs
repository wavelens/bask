/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
#![cfg(feature = "formats")]

use std::path::PathBuf;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bask::io::{
    Bytes, Keyed, Read, ReadOptions, SinkRegistry, SinkWorker, Source, SourceRegistry,
    SourceWorker, Target, WriteOptions,
};
use bask::prelude::*;

fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(name);
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_file(&path);
    path
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_to_parquet_through_the_engine() {
    let csv = scratch("bask_io_csv_to_parquet.csv");
    let parquet = scratch("bask_io_csv_to_parquet.parquet");
    std::fs::write(&csv, "id,name\n10,alice\n20,bob\n30,carol\n").unwrap();

    let sinks = SinkRegistry::<RecordBatch>::formats();
    let report = Engine::builder()
        .worker(SourceWorker::new(SourceRegistry::<RecordBatch>::formats()))
        .worker_cfg(
            SinkWorker::open(&sinks, parquet.to_str().unwrap()).unwrap(),
            WorkerCfg::new().concurrency(1),
        )
        .seed(Read::<RecordBatch>::new(csv.to_str().unwrap()))
        .run()
        .await
        .unwrap();

    assert!(
        report.failures.is_empty(),
        "failures: {:?}",
        report.failures
    );

    let mut reader = bask::formats::for_path(&parquet)
        .unwrap()
        .open_reader(&parquet, 8192)
        .unwrap();
    let (mut rows, mut sum) = (0usize, 0i64);
    while let Some(batch) = reader.next_chunk().unwrap() {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        rows += batch.num_rows();
        sum += ids.iter().flatten().sum::<i64>();
    }
    assert_eq!(rows, 3);
    assert_eq!(sum, 60);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blob_directory_copies_through_the_engine() {
    let src = scratch("bask_io_blobs_in");
    let dst = scratch("bask_io_blobs_out");
    std::fs::create_dir_all(src.join("nested")).unwrap();
    std::fs::write(src.join("a.txt"), b"alpha").unwrap();
    std::fs::write(src.join("nested/b.bin"), b"beta").unwrap();

    let sinks = SinkRegistry::<Bytes>::blobs();
    let report = Engine::builder()
        .worker(SourceWorker::new(SourceRegistry::<Bytes>::blobs()))
        .worker_cfg(
            SinkWorker::open(&sinks, dst.to_str().unwrap()).unwrap(),
            WorkerCfg::new().concurrency(1),
        )
        .seed(Read::<Bytes>::new(src.to_str().unwrap()))
        .run()
        .await
        .unwrap();

    assert!(
        report.failures.is_empty(),
        "failures: {:?}",
        report.failures
    );
    assert_eq!(report.stats.processed, 3); // 1 directory read + 2 blob writes
    assert_eq!(std::fs::read(dst.join("a.txt")).unwrap(), b"alpha");
    assert_eq!(std::fs::read(dst.join("nested/b.bin")).unwrap(), b"beta");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rotating_record_sink_splits_into_parts() {
    let base = scratch("bask_io_rotate.arrow");
    let sinks = SinkRegistry::<RecordBatch>::formats();
    let mut sink = sinks
        .open(
            base.to_str().unwrap(),
            &WriteOptions {
                rotate_rows: Some(2),
            },
        )
        .unwrap();

    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("n", arrow::datatypes::DataType::Int64, false),
    ]));
    for i in 0..3 {
        let col = Int64Array::from(vec![i * 2, i * 2 + 1]);
        let batch = RecordBatch::try_new(schema.clone(), vec![std::sync::Arc::new(col)]).unwrap();
        sink.write(&Keyed::new(format!("batch-{i}"), batch))
            .await
            .unwrap();
    }
    sink.finish().await.unwrap();

    let parts: Vec<_> = (0..3)
        .map(|p| base.with_file_name(format!("bask_io_rotate-{p:05}.arrow")))
        .collect();
    assert!(parts.iter().all(|p| p.exists()), "expected rotated parts");
    for p in &parts {
        std::fs::remove_file(p).ok();
    }
}

struct OneBlob(Option<Keyed<Bytes>>);
#[async_trait]
impl Source<Bytes> for OneBlob {
    async fn next(&mut self) -> anyhow::Result<Option<Keyed<Bytes>>> {
        Ok(self.0.take())
    }
}

#[tokio::test]
async fn registry_resolves_by_scheme_and_extends_without_core_changes() {
    let mut registry = SourceRegistry::<Bytes>::blobs();
    registry.register_scheme(&["mem"], |t: &Target, _o: &ReadOptions| {
        let payload = Bytes::from(t.rest.clone().into_bytes());
        Ok(Box::new(OneBlob(Some(Keyed::new(t.raw.clone(), payload)))) as Box<dyn Source<Bytes>>)
    });

    let mut src = registry
        .open("mem://payload", &ReadOptions::default())
        .unwrap();
    let item = src.next().await.unwrap().unwrap();
    assert_eq!(&item.value[..], b"payload");
    assert!(src.next().await.unwrap().is_none());

    assert!(registry.open("nope://x", &ReadOptions::default()).is_err());
}
