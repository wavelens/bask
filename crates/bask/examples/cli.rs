// SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
//
// SPDX-License-Identifier: MIT OR Apache-2.0
//! A bask script that is also a program. `cargo run --example cli` runs the pipeline with
//! live progress; `... --example cli -- list-tasks` lists its checkpoints; `... -- --tasks=Saved`
//! runs only up to that boundary. The whole CLI is `bask::cli::run` -- this file just wires
//! the pipeline and forwards argv.

use bask::Checkpoint;
use bask::prelude::*;
use serde::{Deserialize, Serialize};

struct Feed;
struct Line(u64);

#[derive(Serialize, Deserialize, Checkpoint)]
struct Saved {
    #[key]
    id: String,
    value: u64,
}

#[derive(Serialize, Deserialize, Checkpoint)]
struct Resaved {
    #[key]
    id: String,
    value: u64,
}

struct Reader;
#[async_trait]
impl Worker for Reader {
    type Task = Feed;
    async fn process(&self, _feed: &Feed, ctx: &Context) -> anyhow::Result<()> {
        for i in 0..20 {
            ctx.emit_keyed(i, Line(i)).await?;
        }
        Ok(())
    }
}

// Convert each row into a Saved checkpoint keyed by its ordinal.
struct Convert;
#[async_trait]
impl Worker for Convert {
    type Task = Line;
    async fn process(&self, line: &Line, ctx: &Context) -> anyhow::Result<()> {
        ctx.emit(Saved {
            id: format!("row-{}", line.0),
            value: line.0,
        })
        .await?;
        Ok(())
    }
}

// Edit each Saved and resave it; Resaved has no worker, so it is a terminal checkpoint.
struct Edit;
#[async_trait]
impl Worker for Edit {
    type Task = Saved;
    async fn process(&self, saved: &Saved, ctx: &Context) -> anyhow::Result<()> {
        ctx.emit(Resaved {
            id: saved.id.clone(),
            value: saved.value * 10,
        })
        .await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let engine = Engine::builder()
        .worker(Reader)
        .worker(Convert)
        .worker(Edit)
        .source("feed", Feed);
    std::process::exit(bask::cli::run(engine, std::env::args()).await);
}
