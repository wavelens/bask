/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
#![cfg(feature = "postgres")]

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bask::formats::PostgresSink;
use bask::io::{Keyed, Sink};

/// Requires a reachable Postgres. Set `BASK_TEST_POSTGRES` to a connection string with a
/// writable `bask_copy_test` table, then run `cargo test --features postgres -- --ignored`.
#[tokio::test]
#[ignore]
async fn postgres_copy_in_loads_a_batch() {
    let Ok(conn) = std::env::var("BASK_TEST_POSTGRES") else {
        eprintln!("BASK_TEST_POSTGRES unset; skipping");
        return;
    };

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();

    let mut sink = PostgresSink::new(conn, "bask_copy_test");
    sink.write(&Keyed::new("batch-0", batch)).await.unwrap();
    sink.finish().await.unwrap();
}
