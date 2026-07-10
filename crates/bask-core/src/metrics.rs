/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Point-in-time engine load, handed to a [`Monitor`](crate::Monitor) each sample.

pub struct Snapshot {
    pub in_flight: usize,
    pub queued: usize,
    pub processed: u64,
    pub retried: u64,
    pub failed: u64,
    pub workers: Vec<WorkerStat>,
}

pub struct WorkerStat {
    pub worker_type: &'static str,
    pub instances: usize,
    pub active: usize,
    pub capacity: usize,
    pub queued: usize,
    pub processed: u64,
}
