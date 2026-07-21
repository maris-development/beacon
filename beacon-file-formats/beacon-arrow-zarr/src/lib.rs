//! `beacon-arrow-zarr` bridges Zarr v3 stores into Beacon's shared
//! `beacon-nd-array` engine.
//!
//! A zarr group is exposed as a Beacon
//! [`AnyDataset`](beacon_nd_array::dataset::AnyDataset) via lazy
//! [`NdArrayD`](beacon_nd_array::NdArrayD) backends ([`backend`]), with CF
//! conventions (fill values, `scale_factor`/`add_offset`, time units) applied
//! at read time. The [`datafusion`] module registers a `ZarrFormat` /
//! `ZarrFormatFactory` so zarr stores are queryable — including predicate
//! pushdown handled by the shared engine.

pub mod attributes;
pub mod backend;
pub mod compat;
pub mod data_types;
pub mod datafusion;
pub mod reader;
pub mod util;
pub mod writer;
