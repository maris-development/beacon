//! DataFusion sinks that materialize Arrow [`RecordBatch`]es into NetCDF files.
//!
//! The module provides two [`DataSink`] implementations:
//!
//! * [`NetCDFSink`] — writes each incoming batch directly via the high-level
//!   [`ArrowRecordBatchWriter`], producing a *flat* NetCDF file with a single
//!   unlimited `obs` dimension. Suitable for classic table-shaped / ragged data.
//!
//! * [`NetCDFNdSink`] — reshapes tabular rows into N-dimensional slabs using
//!   previously collected unique dimension values (via
//!   [`UniqueValuesHandleCollection`]), producing a *gridded* NetCDF file whose
//!   variables are indexed by named dimensions (e.g. `lat × lon × time`).
//!
//! # Write mode selection
//!
//! The write mode is controlled by [`NetcdfOptions::write_dimensions`]:
//!
//! | `write_dimensions`           | Sink used       | NetCDF layout           |
//! |------------------------------|-----------------|-------------------------|
//! | `None` / empty               | [`NetCDFSink`]  | flat, unlimited `obs`   |
//! | `Some(["lat", "lon", …])`  | [`NetCDFNdSink`]| gridded, named dims     |
//!
//! Both sinks write a local scratch file (see `scratch`) and upload it to the
//! object store of the [`FileSinkConfig`](datafusion::datasource::physical_plan::FileSinkConfig).

mod flat;
mod grid;
mod scratch;
#[cfg(test)]
mod tests;

pub use flat::NetCDFSink;
pub use grid::NetCDFNdSink;
