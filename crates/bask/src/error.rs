/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("engine has stopped; cannot emit")]
    Stopped,
    #[error("worker lifecycle hook failed: {0}")]
    Worker(#[source] anyhow::Error),
}
