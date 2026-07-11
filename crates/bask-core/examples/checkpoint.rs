/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Opt-in durability: a source reads rows, a worker "fetches" each, and the result is a
//! `Saved` checkpoint. The two runs share one store, so the second skips every already
//! fetched row and the source is not re-read.

use std::sync::Arc;

use bask_core::prelude::*;
use bask_core::{Checkpoint, MemStore};
use serde::{Deserialize, Serialize};

struct Feed;
struct Row(u64);

#[derive(Serialize, Deserialize, Checkpoint)]
struct Saved {
    #[key]
    id: u64,
    body: String,
}

struct Reader;
#[async_trait]
impl Worker for Reader {
    type Task = Feed;
    async fn process(&self, _feed: &Feed, ctx: &Context) -> anyhow::Result<()> {
        for id in 0..8 {
            ctx.emit_keyed(id, Row(id)).await?;
        }
        Ok(())
    }
}

struct Fetch;
#[async_trait]
impl Worker for Fetch {
    type Task = Row;
    async fn process(&self, row: &Row, ctx: &Context) -> anyhow::Result<()> {
        ctx.emit(Saved {
            id: row.0,
            body: format!("row-{}", row.0),
        })
        .await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = Arc::new(MemStore::default());

    for run in 1..=2 {
        let report = Engine::builder()
            .worker(Reader)
            .worker(Fetch)
            .store(store.clone())
            .source("feed", Feed)
            .run()
            .await?;
        println!(
            "run {run}: processed {}, skipped {}",
            report.stats.processed, report.stats.skipped
        );
    }
    Ok(())
}
