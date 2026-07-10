/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Live dashboard: a fan-out crawl -> render pipeline with two worker types and
//! two instances each. Run in a terminal to watch queue depth and per-type
//! concurrency update in place.
use std::time::Duration;

use bask::prelude::*;

const MAX_DEPTH: u32 = 3;
const FANOUT: u32 = 4;

struct Page {
    id: u32,
    depth: u32,
}

struct Render;

struct Crawler;
#[async_trait]
impl Worker for Crawler {
    type Task = Page;
    async fn process(&self, page: &Page, ctx: &Context) -> anyhow::Result<()> {
        tokio::time::sleep(Duration::from_millis(30)).await; // simulate fetching
        ctx.emit(Render).await?;
        if page.depth < MAX_DEPTH {
            for i in 0..FANOUT {
                let id = page.id.wrapping_mul(FANOUT).wrapping_add(i);
                ctx.emit(Page {
                    id,
                    depth: page.depth + 1,
                })
                .await?;
            }
        }
        Ok(())
    }
}

struct Renderer;
#[async_trait]
impl Worker for Renderer {
    type Task = Render;
    async fn process(&self, _render: &Render, ctx: &Context) -> anyhow::Result<()> {
        tokio::time::sleep(Duration::from_millis(50)).await; // simulate rendering
        ctx.route::<Rendered>(1).await?;
        Ok(())
    }
}

struct Rendered;
impl Router for Rendered {
    type Input = u64;
    type State = u64;
    type Output = u64;
    fn route(state: &mut u64, input: u64, _out: &mut Emit) {
        *state += input;
    }

    fn merge(left: &mut u64, right: u64) {
        *left += right;
    }

    fn finalize(state: u64) -> u64 {
        state
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let two = || WorkerCfg::new().concurrency(2);
    let report = Engine::builder()
        .worker_cfg(Crawler, two().label("crawler-1"))
        .worker_cfg(Crawler, two().label("crawler-2"))
        .worker_cfg(Renderer, two().label("renderer-1"))
        .worker_cfg(Renderer, two().label("renderer-2"))
        .router::<Rendered>()
        .concurrency(6)
        .monitor(LiveConsole::new())
        .sample_interval(Duration::from_millis(120))
        .seed(Page { id: 1, depth: 0 })
        .run()
        .await?;

    println!("rendered {} pages", report.output::<Rendered>().unwrap());
    Ok(())
}
