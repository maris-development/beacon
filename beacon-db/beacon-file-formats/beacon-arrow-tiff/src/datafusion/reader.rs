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
pub async fn fetch_schema(
    object_store: Arc<dyn ObjectStore>,
    object: ObjectMeta,
) -> datafusion::error::Result<arrow::datatypes::SchemaRef> {
    let dataset = open_dataset(object_store, object).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to open TIFF dataset for schema inference: {e}"
        ))
    })?;

    let schema =
        beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema(&dataset).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to derive Arrow schema from TIFF dataset: {e}"
            ))
        })?;

    Ok(schema.into())
}
