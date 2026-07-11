// SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
//
// SPDX-License-Identifier: MIT OR Apache-2.0
#![cfg(feature = "dataset")]
//! A save followed by a resave that re-derives the same source rows supersedes the earlier
//! shards, garbage-collects their files, and survives reopening the dataset.

use std::path::Path;

use bask_core::{Committed, Coverage, Dataset, Store};
use bask_io::FileDataset;

fn coverage(rows: &[u64]) -> Coverage {
    let mut cov = Coverage::empty();
    for &row in rows {
        cov.insert(row);
    }
    cov
}

fn put(dataset: &FileDataset, name: &str, key: &str, payload: &[u8], rows: &[u64]) {
    dataset
        .put(&Committed {
            name: name.to_string(),
            key: key.to_string(),
            payload: Some(payload.to_vec()),
            coverage: coverage(rows),
        })
        .unwrap();
}

fn shard_files(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in std::fs::read_dir(&path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "parquet") {
                out.push(path.to_string_lossy().into_owned());
            }
        }
    }
    out
}

#[test]
fn resave_supersedes_and_compacts() {
    let dir = std::env::temp_dir().join(format!("bask-dataset-supersede-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    {
        let dataset = FileDataset::open(&dir).unwrap();
        put(&dataset, "Saved", "a", b"chunk-a-v1", &[0, 1]);
        put(&dataset, "Saved", "b", b"chunk-b-v1", &[2, 3]);
        let live: Vec<_> = dataset
            .read()
            .unwrap()
            .into_iter()
            .map(|s| s.payload)
            .collect();
        assert_eq!(live.len(), 2);
        assert_eq!(shard_files(&dir).len(), 2, "both first-save shards on disk");

        // Edit + resave: the same source rows, new content -> the first shards are superseded.
        put(&dataset, "Resaved", "a", b"chunk-a-v2", &[0, 1]);
        put(&dataset, "Resaved", "b", b"chunk-b-v2", &[2, 3]);
        let mut live: Vec<_> = dataset
            .read()
            .unwrap()
            .into_iter()
            .map(|s| s.payload)
            .collect();
        live.sort();
        assert_eq!(live, vec![b"chunk-a-v2".to_vec(), b"chunk-b-v2".to_vec()]);
        assert_eq!(
            shard_files(&dir).len(),
            2,
            "superseded files garbage-collected"
        );
    }

    // Reopen as a fresh handle: the compacted snapshot persists.
    let reopened = FileDataset::open(&dir).unwrap();
    let mut live: Vec<_> = reopened
        .read()
        .unwrap()
        .into_iter()
        .map(|s| s.payload)
        .collect();
    live.sort();
    assert_eq!(live, vec![b"chunk-a-v2".to_vec(), b"chunk-b-v2".to_vec()]);
    assert!(reopened.covered().unwrap().contains(3), "coverage survives");

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn partial_resave_keeps_untouched_shards() {
    let dir = std::env::temp_dir().join(format!("bask-dataset-partial-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let dataset = FileDataset::open(&dir).unwrap();

    put(&dataset, "Saved", "a", b"a-v1", &[0, 1]);
    put(&dataset, "Saved", "b", b"b-v1", &[2, 3]);
    // Only rows {0,1} are re-derived, so shard "b" stays live.
    put(&dataset, "Resaved", "a", b"a-v2", &[0, 1]);

    let mut live: Vec<_> = dataset
        .read()
        .unwrap()
        .into_iter()
        .map(|s| s.payload)
        .collect();
    live.sort();
    assert_eq!(live, vec![b"a-v2".to_vec(), b"b-v1".to_vec()]);
    assert_eq!(shard_files(&dir).len(), 2);

    std::fs::remove_dir_all(&dir).unwrap();
}
