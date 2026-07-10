/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
#![cfg(feature = "download")]

use std::io::{Read as _, Write as _};
use std::net::TcpListener;
use std::path::{Path, PathBuf};

use bask::io::{
    Bytes, HttpSource, Read, SinkRegistry, SinkWorker, Source, SourceRegistry, SourceWorker,
};
use bask::prelude::*;

/// A throwaway HTTP/1.1 server that answers every request with `body`.
fn serve(body: &'static [u8]) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/file", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf);
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
        }
    });
    url
}

fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(name);
    let _ = std::fs::remove_dir_all(&path);
    path
}

fn find_one_file(dir: &Path) -> PathBuf {
    fn walk(dir: &Path) -> Option<PathBuf> {
        let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
            .ok()?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .collect();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                if let Some(found) = walk(&path) {
                    return Some(found);
                }
            } else {
                return Some(path);
            }
        }
        None
    }
    walk(dir).unwrap_or_else(|| panic!("no file written under {dir:?}"))
}

#[tokio::test]
async fn http_source_fetches_bytes() {
    let url = serve(b"hello world");
    let mut src = HttpSource::open(url);
    let item = src.next().await.unwrap().unwrap();
    assert_eq!(&item.value[..], b"hello world");
    assert!(src.next().await.unwrap().is_none());
}

#[tokio::test]
async fn http_source_resumes_from_cache() {
    let url = serve(b"from-network");
    let cache = scratch("bask_dl_cache");

    let mut first = HttpSource::open(url.clone()).with_cache(&cache);
    assert_eq!(
        &first.next().await.unwrap().unwrap().value[..],
        b"from-network"
    );

    // Tamper with the cached copy: a second fetch must read it, not the network.
    let cached = find_one_file(&cache);
    std::fs::write(&cached, b"from-disk").unwrap();

    let mut second = HttpSource::open(url).with_cache(&cache);
    assert_eq!(
        &second.next().await.unwrap().unwrap().value[..],
        b"from-disk"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn download_to_directory_through_the_engine() {
    let url = serve(b"engine-body");
    let out = scratch("bask_dl_out");

    let sinks = SinkRegistry::<Bytes>::blobs();
    let report = Engine::builder()
        .worker(SourceWorker::new(SourceRegistry::<Bytes>::blobs()))
        .worker_cfg(
            SinkWorker::open(&sinks, out.to_str().unwrap()).unwrap(),
            WorkerCfg::new().concurrency(1),
        )
        .seed(Read::<Bytes>::new(url))
        .run()
        .await
        .unwrap();

    assert!(
        report.failures.is_empty(),
        "failures: {:?}",
        report.failures
    );
    assert_eq!(report.stats.processed, 2); // 1 fetch + 1 write
    assert_eq!(std::fs::read(find_one_file(&out)).unwrap(), b"engine-body");
}
