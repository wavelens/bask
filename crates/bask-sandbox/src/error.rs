/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

/// Errors from spawning or driving a sandbox.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("isolation backend {0} is unavailable: {1}")]
    Unavailable(&'static str, String),
    #[error("container backend requires an image but none was set")]
    MissingImage,
    #[error("sandbox io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("sandbox exec failed: {0}")]
    Exec(String),
}

pub type Result<T> = std::result::Result<T, Error>;
