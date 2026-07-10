/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Live observation of a run. Implement [`Monitor`] for custom sinks; [`LiveConsole`]
//! is a ready-made in-place terminal dashboard.
use std::io::{IsTerminal, Write};
use std::time::Instant;

use crate::metrics::Snapshot;
use crate::report::RunReport;

/// Sampled at a fixed interval by the engine while a run is in progress.
pub trait Monitor: Send {
    fn sample(&mut self, snapshot: &Snapshot);
    fn finish(&mut self, _report: &RunReport) {}
}

/// Rewrites its block in place each sample; falls back to plain appended frames
/// when stdout is not a terminal (pipes, logs).
pub struct LiveConsole {
    last_lines: usize,
    tty: bool,
    start: Option<Instant>,
}

impl LiveConsole {
    pub fn new() -> Self {
        Self {
            last_lines: 0,
            tty: std::io::stdout().is_terminal(),
            start: None,
        }
    }

    fn frame(&mut self, s: &Snapshot) -> Vec<String> {
        let elapsed = self.start.get_or_insert_with(Instant::now).elapsed();
        let mut lines = vec![
            format!(
                "bask · {:>5.1}s · in-flight {} · queue {} · processed {} · retried {} · failed {}",
                elapsed.as_secs_f64(),
                s.in_flight,
                s.queued,
                s.processed,
                s.retried,
                s.failed
            ),
            format!(
                "  {:<16}{:>9}{:>9}{:>9}",
                "worker", "active", "queued", "done"
            ),
        ];
        for w in &s.workers {
            lines.push(format!(
                "  {:<16}{:>9}{:>9}{:>9}",
                short(w.worker_type),
                format!("{}/{}", w.active, w.capacity),
                w.queued,
                w.processed
            ));
        }
        lines
    }
}

impl Default for LiveConsole {
    fn default() -> Self {
        Self::new()
    }
}

impl Monitor for LiveConsole {
    fn sample(&mut self, snapshot: &Snapshot) {
        let lines = self.frame(snapshot);
        let mut out = std::io::stdout().lock();
        if self.tty {
            if self.last_lines > 0 {
                let _ = write!(out, "\x1b[{}A", self.last_lines);
            }
            for line in &lines {
                let _ = writeln!(out, "\x1b[2K{line}");
            }
        } else {
            for line in &lines {
                let _ = writeln!(out, "{line}");
            }
            let _ = writeln!(out);
        }
        let _ = out.flush();
        self.last_lines = lines.len();
    }

    fn finish(&mut self, _report: &RunReport) {
        if self.tty {
            println!();
        }
    }
}

fn short(type_name: &str) -> &str {
    type_name.rsplit("::").next().unwrap_or(type_name)
}
