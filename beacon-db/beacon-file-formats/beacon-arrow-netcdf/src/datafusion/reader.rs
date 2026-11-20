use std::sync::Arc;

use beacon_nd_array::dataset::AnyDataset;
use object_store::{ObjectMeta, ObjectStore};

/// Where the reader gets the bytes of one NetCDF object.
///
/// `oxcdf` range-reads the object through the store. No local copy is made, so
/// this also covers s3, gs and az. The format, the statistics and the opener
/// all build one of these, so they cannot disagree about a file.
#[derive(Debug, Clone)]
pub struct NetcdfInput {
    /// The store the object lives in.
    pub store: Arc<dyn ObjectStore>,
    /// The object, relative to that store. Its size spares the reader a
    /// `head` request.
    pub object: ObjectMeta,
}

impl NetcdfInput {
    /// The input for `object` in `store`.
    pub fn new(store: &Arc<dyn ObjectStore>, object: &ObjectMeta) -> Self {
        Self {
            store: store.clone(),
            object: object.clone(),
        }
    }

    /// The location to name in an error message.
    pub fn location(&self) -> String {
        self.object.location.to_string()
    }

    /// Open the dataset this input points at.
    pub async fn open(self) -> anyhow::Result<AnyDataset> {
        crate::oxcdf_reader::open_object(self.store, &self.object).await
    }
}

/// Fetch the Arrow schema for a NetCDF object by opening the dataset and
/// converting its fields to an Arrow [`SchemaRef`].
///
/// When `read_dimensions` is provided the dataset is projected to only
/// include variables that belong to those dimensions before deriving the
/// Arrow schema. When it is absent, a broadcast-compatible default dimension
/// set is auto-selected (see [`beacon_nd_array::dataset::resolve_read_dimensions`])
/// so the schema matches
/// what `SELECT *` can actually return.
///
/// With `skip_unbroadcastable`, a file the list does not fit gets an empty
/// schema, the same way the scan reads no row from it.
///
/// With `skip_invalid_files`, a file the reader cannot open gets an empty
/// schema and a warning, the same way the scan reads no row from it.
///
/// A repeated inference of the same file is answered by the schema cache of
/// [`beacon_datafusion_ext::format_ext`], above this function, so this one
/// always opens the file.
///
/// # Errors
///
/// Returns an error when the file cannot be opened and `skip_invalid_files`
/// is off, or when its dimensions or types do not convert to Arrow.
pub async fn fetch_schema(
    input: NetcdfInput,
    read_dimensions: Option<Vec<String>>,
    skip_unbroadcastable: bool,
    skip_invalid_files: bool,
) -> datafusion::error::Result<arrow::datatypes::SchemaRef> {
    // The input moves into `open`, so keep its location only when a skip needs it.
    let location = skip_invalid_files.then(|| input.location());
    let dataset = match (input.open().await, location) {
        (Ok(dataset), _) => dataset,
        (Err(e), Some(location)) => {
            tracing::warn!(file = %location, error = %e, "skipping an unreadable NetCDF file");
            return Ok(Arc::new(arrow::datatypes::Schema::empty()));
        }
        (Err(e), None) => {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "Failed to open NetCDF dataset for schema inference: {e}"
            )))
        }
    };

    // A skipped file gives the table no column.
    let Some(dataset) = beacon_nd_array::dataset::project_read_dimensions_or_skip(
        dataset,
        read_dimensions,
        skip_unbroadcastable,
        Some("read_netcdf"),
    )
    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?
    else {
        return Ok(Arc::new(arrow::datatypes::Schema::empty()));
    };

    let schema =
        beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema(&dataset).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to derive Arrow schema from NetCDF dataset: {e}"
            ))
        })?;

    Ok(schema.into())
}
