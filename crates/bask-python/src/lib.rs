use pyo3::prelude::*;

#[pymodule]
fn _bask(_m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
