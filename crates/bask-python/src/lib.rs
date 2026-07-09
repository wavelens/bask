/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use pyo3::prelude::*;

#[pymodule]
fn _bask(_m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
