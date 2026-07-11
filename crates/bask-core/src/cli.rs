/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! The one Rust frontend a bask script runs behind: argument parsing, `list-tasks`, task
//! selection, and live rendering, all defined here so the Rust binary and the Python
//! entrypoint mirror it for free. A front-end supplies the [`EngineBuilder`] and forwards
//! `argv`; everything else -- flags, `list-tasks`/`--json` output, the live monitor, and the
//! exit code -- comes from this module, so a change here needs no change in the binding.

use std::sync::Arc;

use crate::checkpoint::Dataset;
use crate::engine::{EngineBuilder, TaskInfo};
use crate::monitor::{JsonConsole, LiveConsole};
use crate::sqlite::SqliteStore;

/// Opens a [`Dataset`] from a `--dataset` path; supplied by the front-end that owns the
/// concrete type (bask-io's `FileDataset`), since bask-core cannot construct one itself.
type DatasetOpener = Box<dyn Fn(&str) -> anyhow::Result<Arc<dyn Dataset>>>;

/// The bask CLI frontend. Build it, optionally teach it how to open a `--dataset`, then
/// [`run`](Cli::run) it with the process arguments.
#[derive(Default)]
pub struct Cli {
    dataset_opener: Option<DatasetOpener>,
}

impl Cli {
    pub fn new() -> Self {
        Cli::default()
    }

    /// Register how `--dataset DIR` maps to a [`Dataset`]; without it the flag is rejected.
    pub fn dataset_opener(
        mut self,
        opener: impl Fn(&str) -> anyhow::Result<Arc<dyn Dataset>> + 'static,
    ) -> Self {
        self.dataset_opener = Some(Box::new(opener));
        self
    }

    /// Parse `args` (including argv[0]), apply the run-level flags to `builder`, then either
    /// run the pipeline with live progress or print `list-tasks`. Prints any error to stderr
    /// and returns a process exit code, so the front-end is a bare `exit(run(..).await)`:
    /// 0 on success, 1 if a task failed, 2 on a usage error.
    pub async fn run(self, builder: EngineBuilder, args: impl IntoIterator<Item = String>) -> i32 {
        match self.try_run(builder, args).await {
            Ok(code) => code,
            Err(err) => {
                eprintln!("bask: {err:#}");
                2
            }
        }
    }

    async fn try_run(
        self,
        mut builder: EngineBuilder,
        args: impl IntoIterator<Item = String>,
    ) -> anyhow::Result<i32> {
        let opts = Opts::parse(args)?;
        if opts.help {
            print_help();
            return Ok(0);
        }
        if let Some(j) = opts.concurrency {
            builder = builder.concurrency(j);
        }
        if let Some(path) = &opts.store {
            builder = builder.store(SqliteStore::open(path));
        }
        if let Some(dir) = &opts.dataset {
            let opener = self
                .dataset_opener
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("--dataset is not supported by this entrypoint"))?;
            builder = builder.dataset_arc(opener(dir)?);
        }

        if opts.list_tasks {
            print_tasks(&builder.build().tasks()?);
            return Ok(0);
        }

        if let Some(task) = &opts.task {
            builder = builder.select_tasks([task.clone()]);
        }
        builder = match (opts.json, opts.no_live) {
            (true, _) => builder.monitor(JsonConsole::new()),
            (false, true) => builder.monitor(LiveConsole::plain()),
            (false, false) => builder.monitor(LiveConsole::new()),
        };
        let engine = builder.build();
        if let Some(task) = &opts.task
            && !engine.checkpoint_names().contains(&task.as_str())
        {
            anyhow::bail!(
                "unknown task {task:?}; run `list-tasks` (available: {})",
                engine.checkpoint_names().join(", ")
            );
        }
        let report = engine.run().await?;
        Ok(i32::from(report.stats.failed > 0))
    }
}

/// One entrypoint call: parse `args`, drive `builder`, return an exit code. Front-ends needing
/// `--dataset` build a [`Cli`] with [`dataset_opener`](Cli::dataset_opener) instead.
pub async fn run(builder: EngineBuilder, args: impl IntoIterator<Item = String>) -> i32 {
    Cli::new().run(builder, args).await
}

#[derive(Default)]
struct Opts {
    list_tasks: bool,
    task: Option<String>,
    concurrency: Option<usize>,
    store: Option<String>,
    dataset: Option<String>,
    no_live: bool,
    json: bool,
    help: bool,
}

impl Opts {
    fn parse(args: impl IntoIterator<Item = String>) -> anyhow::Result<Self> {
        let mut opts = Opts::default();
        let mut it = args.into_iter().skip(1);
        while let Some(arg) = it.next() {
            let mut value = |flag: &str| {
                it.next()
                    .ok_or_else(|| anyhow::anyhow!("{flag} expects a value"))
            };
            match arg.as_str() {
                "list-tasks" => opts.list_tasks = true,
                "--no-live" => opts.no_live = true,
                "--json" => opts.json = true,
                "-h" | "--help" => opts.help = true,
                "-j" | "--concurrency" => opts.concurrency = Some(value(&arg)?.parse()?),
                "--store" => opts.store = Some(value(&arg)?),
                "--dataset" => opts.dataset = Some(value(&arg)?),
                "--tasks" => opts.task = Some(value(&arg)?),
                other => {
                    if let Some(v) = other.strip_prefix("--tasks=") {
                        opts.task = Some(v.to_string());
                    } else if let Some(v) = other.strip_prefix("--store=") {
                        opts.store = Some(v.to_string());
                    } else if let Some(v) = other.strip_prefix("--dataset=") {
                        opts.dataset = Some(v.to_string());
                    } else if let Some(v) = other.strip_prefix("--concurrency=") {
                        opts.concurrency = Some(v.parse()?);
                    } else if let Some(v) = other.strip_prefix("-j") {
                        opts.concurrency = Some(v.parse()?);
                    } else {
                        anyhow::bail!("unknown argument {other:?} (try --help)");
                    }
                }
            }
        }
        Ok(opts)
    }
}

fn print_help() {
    println!(
        "bask pipeline runner\n\n\
         usage: <program> [command] [options]\n\n\
         commands:\n  \
           (none)               run the pipeline with live progress\n  \
           list-tasks           list the checkpoint tasks and their stored status\n\n\
         options:\n  \
           --tasks=NAME          run only up to the named checkpoint (a terminal boundary)\n  \
           --store=PATH          checkpoint store path (default ./bask.sqlite)\n  \
           --dataset=DIR         materialize checkpoints into a dataset directory\n  \
           -j, --concurrency=N   worker concurrency\n  \
           --no-live             appended progress frames instead of an in-place dashboard\n  \
           --json                emit newline-delimited JSON snapshots\n  \
           -h, --help            show this help"
    );
}

fn print_tasks(tasks: &[TaskInfo]) {
    if tasks.is_empty() {
        println!("no checkpoint tasks registered");
        return;
    }
    println!("tasks (checkpoints):");
    for task in tasks {
        let consumer = task.worker_type.map_or("terminal", short);
        println!(
            "  {:<20} {:>4} stored  {:>4} done   (worker: {consumer})",
            task.name, task.stored, task.done
        );
    }
}

fn short(type_name: &str) -> &str {
    type_name.rsplit("::").next().unwrap_or(type_name)
}

#[cfg(test)]
mod tests {
    use super::Opts;

    fn parse(args: &[&str]) -> Opts {
        Opts::parse(std::iter::once("prog".to_string()).chain(args.iter().map(|s| s.to_string())))
            .unwrap()
    }

    #[test]
    fn parses_flags_in_both_forms() {
        let o = parse(&["--tasks=Convert", "--store=s.db", "-j", "8", "--json"]);
        assert_eq!(o.task.as_deref(), Some("Convert"));
        assert_eq!(o.store.as_deref(), Some("s.db"));
        assert_eq!(o.concurrency, Some(8));
        assert!(o.json && !o.no_live && !o.list_tasks);

        let o = parse(&[
            "list-tasks",
            "--concurrency=2",
            "--dataset",
            "out",
            "--no-live",
        ]);
        assert!(o.list_tasks && o.no_live);
        assert_eq!(o.concurrency, Some(2));
        assert_eq!(o.dataset.as_deref(), Some("out"));
    }

    #[test]
    fn rejects_unknown_argument_and_missing_value() {
        assert!(Opts::parse(["prog".to_string(), "--nope".to_string()]).is_err());
        assert!(Opts::parse(["prog".to_string(), "--tasks".to_string()]).is_err());
    }
}
