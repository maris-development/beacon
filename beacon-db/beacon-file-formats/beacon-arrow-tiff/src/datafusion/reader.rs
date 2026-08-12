use std::sync::Arc;

use beacon_nd_array::{dataset::AnyDataset, projection::DatasetProjection};
use object_store::{ObjectMeta, ObjectStore};

/// Open a TIFF dataset for a given object metadata entry.
pub async fn open_dataset(
    object_store: Arc<dyn ObjectStore>,
    object: ObjectMeta,
) -> anyhow::Result<AnyDataset> {
    crate::reader::open_dataset(object_store, object.location.clone()).await
}

/// Fetch the Arrow schema for a TIFF object.
///
/// When `read_dimensions` is provided the dataset is projected to only include
/// variables that belong to those dimensions before deriving the Arrow schema.
/// When it is absent, a broadcast-compatible default dimension set is
/// auto-selected (see [`beacon_nd_array::dataset::resolve_read_dimensions`]) so
/// the schema matches what `SELECT *` can actually return.
pub async fn fetch_schema(
    object_store: Arc<dyn ObjectStore>,
    object: ObjectMeta,
    read_dimensions: Option<Vec<String>>,
) -> datafusion::error::Result<arrow::datatypes::SchemaRef> {
    let dataset = open_dataset(object_store, object).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to open TIFF dataset for schema inference: {e}"
        ))
    })?;

    let dataset = if let Some(dims) = beacon_nd_array::dataset::resolve_read_dimensions(
        &dataset,
        read_dimensions,
        Some("read_tiff"),
    ) {
        dataset
            .project(&DatasetProjection::new_with_dimension_projection(dims))
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to project TIFF dataset with dimensions: {e}"
                ))
            })?
    } else {
        dataset
    };

    let schema =
        beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema(&dataset).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to derive Arrow schema from TIFF dataset: {e}"
            ))
        })?;

    Ok(schema.into())
}
