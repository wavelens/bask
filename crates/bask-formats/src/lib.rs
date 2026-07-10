/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Columnar file formats and the record IO plane for bask. A file is read and written
//! as a stream of Arrow [`Chunk`]s; implement [`Format`] to add one, and
//! [`record_sources`]/[`record_sinks`] lift the built-ins into the generic
//! [`bask_io`] source/sink traits. Most users depend on `bask` and reach these via
//! `bask::formats`.

pub mod formats;
pub mod records;

#[cfg(feature = "postgres")]
pub mod postgres;

pub use formats::{
    ArrowFormat, Chunk, ChunkReader, ChunkWriter, CsvFormat, Format, JsonlFormat, ParquetFormat,
    for_path,
};
pub use records::{FormatSink, FormatSource, RotatingRecordSink, record_sinks, record_sources};

#[cfg(feature = "postgres")]
pub use postgres::PostgresSink;
