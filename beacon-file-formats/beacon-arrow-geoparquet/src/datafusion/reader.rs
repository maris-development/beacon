//! Low-level helpers for opening GeoParquet files and deriving their GeoArrow
//! output schema.
//!
//! These wrap the upstream async Parquet reader together with the `geoparquet`
//! crate's [`GeoParquetReaderBuilder`] extension so geometry columns described
//! in the file's `geo` metadata are decoded to their native GeoArrow type.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use geoarrow_schema::CoordType;
use geoparquet::reader::GeoParquetReaderBuilder;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;

/// Coordinate layout used when decoding geometry columns to native GeoArrow.
///
/// `Separated` stores each coordinate dimension in its own child array (struct
/// of `x`/`y` arrays), which round-trips Beacon's own GeoParquet writer output.
pub(crate) const COORD_TYPE: CoordType = CoordType::Separated;

/// The concrete async Parquet reader builder used throughout this crate.
pub(crate) type GeoStreamBuilder = ParquetRecordBatchStreamBuilder<ParquetObjectReader>;

/// Open a streaming Parquet reader builder for a GeoParquet object.
pub(crate) async fn stream_builder(
    object_store: Arc<dyn ObjectStore>,
    object: &ObjectMeta,
) -> Result<GeoStreamBuilder> {
    let reader = ParquetObjectReader::new(object_store, object.location.clone())
        .with_file_size(object.size);

    ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

/// Derive the output Arrow schema for a builder.
///
/// Geometry columns described in the file's GeoParquet metadata are decoded to
/// their native GeoArrow type. Files without a `geo` metadata key fall back to
/// the plain Arrow schema, so a regular Parquet file is still readable.
pub(crate) fn output_schema(builder: &GeoStreamBuilder) -> Result<SchemaRef> {
    match builder.geoparquet_metadata() {
        Some(geo_meta) => {
            let geo_meta = geo_meta.map_err(|e| DataFusionError::External(Box::new(e)))?;
            builder
                .geoarrow_schema(&geo_meta, true, COORD_TYPE)
                .map_err(|e| DataFusionError::External(Box::new(e)))
        }
        None => Ok(builder.schema().clone()),
    }
}

/// Fetch the GeoArrow output schema for a single object, used for schema
/// inference. Only the file metadata is read; no row groups are decoded.
pub(crate) async fn fetch_schema(
    object_store: Arc<dyn ObjectStore>,
    object: ObjectMeta,
) -> Result<SchemaRef> {
    let builder = stream_builder(object_store, &object).await?;
    output_schema(&builder)
}
