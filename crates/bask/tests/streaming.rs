/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
#![cfg(feature = "formats")]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bask::formats::{ArrowFormat, Format, ParquetFormat};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
}

fn batch(start: i64, len: i64) -> RecordBatch {
    let column = Int64Array::from((start..start + len).collect::<Vec<_>>());
    RecordBatch::try_new(schema(), vec![Arc::new(column) as ArrayRef]).unwrap()
}

fn roundtrip_rechunks(format: impl Format, ext: &str) {
    let path = std::env::temp_dir().join(format!("bask_roundtrip.{ext}"));

    let mut writer = format.open_writer(&path, schema()).unwrap();
    writer.write(&batch(0, 2500)).unwrap();
    writer.write(&batch(2500, 2500)).unwrap();
    writer.finish().unwrap();

    let mut reader = format.open_reader(&path, 1024).unwrap();
    let (mut rows, mut sum, mut chunks) = (0usize, 0i64, 0usize);
    while let Some(chunk) = reader.next_chunk().unwrap() {
        chunks += 1;
        assert!(chunk.num_rows() <= 1024, "chunk of {} exceeds chunk_rows", chunk.num_rows());
        let column = chunk.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        rows += chunk.num_rows();
        sum += column.iter().flatten().sum::<i64>();
    }
    std::fs::remove_file(&path).ok();

    assert_eq!(rows, 5000);
    assert_eq!(sum, (0..5000).sum::<i64>());
    assert!(chunks >= 5, "expected 5000 rows to re-chunk into >= 5 chunks, got {chunks}");
}

#[test]
fn parquet_roundtrip_chunks() {
    roundtrip_rechunks(ParquetFormat, "parquet");
}

#[test]
fn arrow_roundtrip_chunks() {
    roundtrip_rechunks(ArrowFormat, "arrow");
}
