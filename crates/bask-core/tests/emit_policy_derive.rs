/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use bask_core::EmitPolicy;
use bask_core::prelude::*;

#[derive(EmitPolicy)]
#[emits(Step)]
struct Job;

struct Step;
struct Forbidden;

struct EmitsForbidden;
#[async_trait]
impl Worker for EmitsForbidden {
    type Task = Job;
    async fn process(&self, _job: &Job, ctx: &Context) -> anyhow::Result<()> {
        ctx.emit(Forbidden).await?;
        Ok(())
    }
}

#[tokio::test]
async fn derived_policy_is_auto_collected() {
    let report = Engine::builder()
        .worker(EmitsForbidden)
        .seed(Job)
        .run()
        .await
        .unwrap();
    assert_eq!(report.stats.failed, 1);
    assert!(report.failures[0].error.contains("may not emit"));
}
