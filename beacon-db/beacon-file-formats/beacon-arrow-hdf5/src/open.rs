//! Opening one HDF5 object with the Rust reader, and schema inference on top.
//!
//! An open parses the metadata of one object. A repeated schema inference of the
//! same file is answered by the schema cache of
//! [`beacon_datafusion_ext::format_ext`], above this module, so nothing is held
//! here: one open reads one file.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_nd_array::dataset::AnyDataset;
use object_store::{ObjectMeta, ObjectStore};

/// Open an HDF5 dataset through the Rust reader.
pub async fn open_dataset(
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
) -> anyhow::Result<AnyDataset> {
    crate::reader::open_dataset(store.clone(), object.location.clone()).await
}

/// Fetch the Arrow schema for an HDF5 object by opening the dataset and
/// converting its fields to an Arrow [`SchemaRef`].
///
/// When `read_dimensions` is provided the dataset is projected to only include
/// variables that belong to those dimensions before deriving the Arrow schema.
/// When it is absent, a broadcast-compatible default dimension set is
/// auto-selected (see [`beacon_nd_array::dataset::resolve_read_dimensions`]) so
/// the schema matches what `SELECT *` can actually return.
pub async fn fetch_schema(
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
    read_dimensions: Option<Vec<String>>,
) -> datafusion::error::Result<SchemaRef> {
    let dataset = open_dataset(store, object).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to open HDF5 dataset {} for schema inference: {e}",
            object.location
        ))
    })?;

    let dataset = if let Some(dims) = beacon_nd_array::dataset::resolve_read_dimensions(
        &dataset,
        read_dimensions,
        Some("read_hdf5"),
    ) {
        let proj = beacon_nd_array::projection::DatasetProjection {
            dimension_projection: Some(dims),
            index_projection: None,
        };
        dataset.project(&proj).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to project HDF5 dataset with dimensions: {e}"
            ))
        })?
    } else {
        dataset
    };

    let schema =
        beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema(&dataset).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to derive Arrow schema from HDF5 dataset: {e}"
            ))
        })?;

    Ok(schema.into())
}
