/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
#![cfg(feature = "object-store")]

use std::collections::BTreeMap;
use std::sync::Arc;

use bask::io::{Bytes, Keyed, ObjectStoreSink, ObjectStoreSource, Sink, Source};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;

#[tokio::test]
async fn object_store_sink_then_source_roundtrip() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let mut sink = ObjectStoreSink::with_store(store.clone(), "out");
    sink.write(&Keyed::new("a.txt", Bytes::from_static(b"alpha")))
        .await
        .unwrap();
    sink.write(&Keyed::new("sub/b.txt", Bytes::from_static(b"beta")))
        .await
        .unwrap();
    sink.finish().await.unwrap();

    let mut source = ObjectStoreSource::with_store(store.clone(), StorePath::from("out"));
    let mut got = BTreeMap::new();
    while let Some(item) = source.next().await.unwrap() {
        got.insert(item.key.to_string(), item.value.to_vec());
    }

    assert_eq!(got.len(), 2);
    assert_eq!(got.get("out/a.txt").unwrap(), b"alpha");
    assert_eq!(got.get("out/sub/b.txt").unwrap(), b"beta");
}
