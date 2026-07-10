/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Resource-attribute-aware retry. Two GPU instances self-describe with a `gpu`
//! attribute and draw from a shared `gpu` pool; a job too large for the a100 fails with a
//! `DifferentAttr("gpu")` hint and is retried on the h100, while a job with no data fails
//! fatally and lands in the dead-letter sink.
use std::sync::{Arc, Mutex};

use bask::prelude::*;

struct TrainJob {
    id: u32,
    size_gb: u32,
}

/// Records the gpu each job was ultimately placed on.
struct Placed;
impl Router for Placed {
    type Input = (u32, &'static str);
    type State = Vec<(u32, &'static str)>;
    type Output = Vec<(u32, &'static str)>;
    fn route(state: &mut Self::State, hit: (u32, &'static str), _out: &mut Emit) {
        state.push(hit);
    }
    fn merge(left: &mut Self::State, right: Self::State) {
        left.extend(right);
    }
    fn finalize(mut state: Self::State) -> Self::Output {
        state.sort();
        state
    }
}

struct Trainer {
    kind: &'static str,
    vram_gb: u32,
}
#[async_trait]
impl Worker for Trainer {
    type Task = TrainJob;
    async fn process(&self, job: &TrainJob, ctx: &Context) -> anyhow::Result<()> {
        if job.size_gb == 0 {
            return Err(anyhow::anyhow!("job {} has no data", job.id)).fatal();
        }
        if job.size_gb > self.vram_gb {
            return Err(anyhow::anyhow!(
                "job {} needs {}GB, {} has {}GB",
                job.id,
                job.size_gb,
                self.kind,
                self.vram_gb
            ))
            .retry_on(RetryOn::DifferentAttr("gpu".into()));
        }
        ctx.route::<Placed>((job.id, self.kind)).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dead: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = {
        let dead = dead.clone();
        move |letter: DeadLetter| {
            if let Some(job) = letter.payload.downcast_ref::<TrainJob>() {
                dead.lock().unwrap().push(job.id);
            }
            println!(
                "dead-letter: {} ({} attempts)",
                letter.error, letter.attempts
            );
        }
    };

    let mut builder = Engine::builder()
        .resource("gpu", 2)
        .worker_cfg(
            Trainer {
                kind: "a100",
                vram_gb: 40,
            },
            WorkerCfg::new()
                .label("a100")
                .attr("gpu", "a100")
                .requires("gpu"),
        )
        .worker_cfg(
            Trainer {
                kind: "h100",
                vram_gb: 80,
            },
            WorkerCfg::new()
                .label("h100")
                .attr("gpu", "h100")
                .requires("gpu"),
        )
        .router::<Placed>()
        .retry(RetryPolicy::new().max_attempts(2).jitter(0.2))
        .dead_letter(sink)
        .concurrency(1);

    let jobs = [(1, 20), (2, 60), (3, 0), (4, 75), (5, 35)];
    for (id, size_gb) in jobs {
        builder = builder.seed(TrainJob { id, size_gb });
    }
    let report = builder.run().await?;

    println!("\nplacements:");
    for (id, gpu) in report.output::<Placed>().unwrap() {
        println!("  job {id} -> {gpu}");
    }
    println!("\ndead-lettered jobs: {:?}", dead.lock().unwrap());
    println!("stats: {:?}", report.stats);
    Ok(())
}
