//! `view_dataset_statistics(path)` — table function that returns per-column
//! DataFusion statistics for a single NetCDF dataset file.
//!
//! # Cache behaviour
//!
//! Statistics are served from the session's [`FileStatisticsCache`] when a
//! valid entry exists (validated by file size **and** last-modified time).
//! A cache miss triggers a full computation via
//! [`statistics::generate_statistics`], and the result is inserted into the
//! cache so that subsequent queries against the same unchanged file are free.
//!
//! # Output schema
//!
//! | column       | type    | nullable | description                                 |
//! |--------------|---------|----------|---------------------------------------------|
//! | column_name  | Utf8    | no       | Field name in the dataset schema            |
//! | data_type    | Utf8    | no       | Arrow data-type string                      |
//! | min_value    | Utf8    | yes      | Minimum value, null when unknown            |
//! | max_value    | Utf8    | yes      | Maximum value, null when unknown            |
//! | is_exact     | Boolean | yes      | true = exact, false = inexact, null = absent|
//!
//! [`FileStatisticsCache`]: datafusion::execution::cache::cache_manager::FileStatisticsCache

use std::sync::Arc;

use arrow::{
    array::{BooleanArray, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use beacon_arrow_netcdf::datafusion::{reader, statistics};
use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use beacon_object_storage::{get_datasets_object_store, DatasetsStore};
use datafusion::{
    catalog::TableFunctionImpl,
    common::{plan_datafusion_err, plan_err, stats::Precision, Statistics},
    datasource::{
        file_format::{self, FileFormat},
        MemTable,
    },
    execution::cache::CacheAccessor,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};
use object_store::{ObjectMeta, ObjectStore};

use crate::file_formats::BeaconTableFunctionImpl;

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("column_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("min_value", DataType::Utf8, true),
        Field::new("max_value", DataType::Utf8, true),
        Field::new("is_exact", DataType::Boolean, true),
    ]))
}

pub struct ViewDatasetStatisticsFunc {
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Arc<SessionContext>,
}

impl ViewDatasetStatisticsFunc {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Arc<SessionContext>) -> Self {
        Self {
            runtime_handle,
            session_ctx,
        }
    }
}

impl std::fmt::Debug for ViewDatasetStatisticsFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ViewDatasetStatisticsFunc")
    }
}

impl BeaconTableFunctionImpl for ViewDatasetStatisticsFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "view_dataset_statistics".to_string()
    }

    fn description(&self) -> Option<String> {
        Some(
            "Returns per-column min/max statistics for a dataset file, \
             using (and populating) the file statistics cache."
                .to_string(),
        )
    }

    fn arguments(&self) -> Option<Vec<Field>> {
        Some(vec![Field::new("path", DataType::Utf8, false)])
    }
}

impl TableFunctionImpl for ViewDatasetStatisticsFunc {
    fn call(
        &self,
        args: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let path_str = match args.first() {
            Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => s.clone(),
            _ => return plan_err!("view_dataset_statistics requires a single Utf8 path argument"),
        };

        let session_ctx = self.session_ctx.clone();

        let (schema, stats) = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                let path = object_store::path::Path::parse(&path_str)
                    .map_err(|e| plan_datafusion_err!("Invalid path '{path_str}': {e}"))?;
                let store = get_datasets_object_store().await;

                // Head the object to get size + last_modified for cache validation.
                let meta = store
                    .head(&path)
                    .await
                    .map_err(|e| plan_datafusion_err!("Failed to stat '{path}': {e}"))?;
                let file_format = infer_file_format(session_ctx.clone(), &meta)?;

                // ── Cache lookup ─────────────────────────────────────────────
                let state = session_ctx.state();
                let cache = state.runtime_env().cache_manager.get_file_statistic_cache();
                let schema = file_schema(
                    session_ctx.clone(),
                    store.clone(),
                    file_format.clone(),
                    &meta,
                )
                .await?;

                if let Some(ref cache) = cache {
                    if let Some(cached) = cache.get_with_extra(&meta.location, &meta) {
                        return Ok::<_, datafusion::error::DataFusionError>((
                            schema.clone(),
                            cached.clone(),
                        ));
                    }
                }

                let stats = Arc::new(
                    file_stats(session_ctx, store, file_format, schema.clone(), &meta).await?,
                );

                if let Some(cache) = cache {
                    cache.put_with_extra(&meta.location, stats.clone(), &meta);
                }

                Ok((schema, stats))
            })
        })?;

        let batch = build_record_batch(&schema, &stats)?;
        let mem_table = MemTable::try_new(output_schema(), vec![vec![batch]])?;
        Ok(Arc::new(mem_table))
    }
}

/// Convert `Statistics` + schema into a single `RecordBatch` in `output_schema()` format.
fn build_record_batch(
    schema: &arrow::datatypes::Schema,
    stats: &Statistics,
) -> datafusion::error::Result<RecordBatch> {
    let fields = schema.fields();
    let col_stats = &stats.column_statistics;

    let n = fields.len().min(col_stats.len());
    let mut column_names: Vec<Option<&str>> = Vec::with_capacity(n);
    let mut data_types: Vec<Option<String>> = Vec::with_capacity(n);
    let mut min_values: Vec<Option<String>> = Vec::with_capacity(n);
    let mut max_values: Vec<Option<String>> = Vec::with_capacity(n);
    let mut is_exact: Vec<Option<bool>> = Vec::with_capacity(n);

    for (field, col_stat) in fields.iter().zip(col_stats.iter()) {
        column_names.push(Some(field.name().as_str()));
        data_types.push(Some(field.data_type().to_string()));

        let (min_str, exact) = precision_to_str_and_exact(&col_stat.min_value);
        let (max_str, _) = precision_to_str_and_exact(&col_stat.max_value);
        min_values.push(min_str);
        max_values.push(max_str);
        is_exact.push(exact);
    }

    RecordBatch::try_new(
        output_schema(),
        vec![
            Arc::new(StringArray::from(column_names)),
            Arc::new(StringArray::from(data_types)),
            Arc::new(StringArray::from(min_values)),
            Arc::new(StringArray::from(max_values)),
            Arc::new(BooleanArray::from(is_exact)),
        ],
    )
    .map_err(|e| plan_datafusion_err!("Failed to build statistics record batch: {e}"))
}

/// Stringify a `Precision<ScalarValue>` and return whether it is exact.
///
/// - `Exact(v)` → `(Some(v.to_string()), Some(true))`
/// - `Inexact(v)` → `(Some(v.to_string()), Some(false))`
/// - `Absent` → `(None, None)`
fn precision_to_str_and_exact(p: &Precision<ScalarValue>) -> (Option<String>, Option<bool>) {
    match p {
        Precision::Exact(v) => (Some(v.to_string()), Some(true)),
        Precision::Inexact(v) => (Some(v.to_string()), Some(false)),
        Precision::Absent => (None, None),
    }
}

fn infer_file_format(
    session_ctx: Arc<SessionContext>,
    meta: &ObjectMeta,
) -> datafusion::error::Result<Arc<dyn FileFormat>> {
    if let Some(ext) = meta.location.extension() {
        if ext == "json"
            && meta
                .location
                .filename()
                .unwrap_or_default()
                .starts_with("zarr")
        {
            // Zarr JSON "file" - So use the ZarrFileFormat
            // Note: this is a bit hacky, but it allows us to support Zarr metadata files without a separate function or argument.

            let state = session_ctx.state();
            let file_format_factory = state.get_file_format_factory("zarr").ok_or_else(|| {
                plan_datafusion_err!(
                    "Unsupported file format for Zarr JSON file '{}'",
                    meta.location
                )
            })?;
            Ok(file_format_factory.default())
        } else {
            // Get the file format from the session context
            let state = session_ctx.state();
            let file_format_factory = state.get_file_format_factory(ext).ok_or_else(|| {
                plan_datafusion_err!(
                    "Unsupported file format for extension '{ext}' in '{}'",
                    meta.location
                )
            })?;
            Ok(file_format_factory.default())
        }
    } else {
        tracing::warn!(
            "Cannot determine file format for '{}', missing extension.",
            meta.location
        );
        Err(plan_datafusion_err!(
            "Cannot determine file format for '{}', missing extension.",
            meta.location
        ))
    }
}

async fn file_schema(
    session_ctx: Arc<SessionContext>,
    store: Arc<dyn ObjectStore>,
    file_format: Arc<dyn FileFormat>,
    meta: &ObjectMeta,
) -> datafusion::error::Result<SchemaRef> {
    let state = session_ctx.state();
    let objects = vec![meta.clone()];
    file_format
        .infer_schema(&state, &store, &objects)
        .await
        .map_err(|e| plan_datafusion_err!("Failed to infer schema for '{}': {e}", meta.location))
}

async fn file_stats(
    session_ctx: Arc<SessionContext>,
    store: Arc<dyn ObjectStore>,
    file_format: Arc<dyn FileFormat>,
    table_schema: SchemaRef,
    meta: &ObjectMeta,
) -> datafusion::error::Result<Statistics> {
    let state = session_ctx.state();
    file_format
        .infer_stats(&state, &store, table_schema, meta)
        .await
        .map_err(|e| {
            plan_datafusion_err!("Failed to infer statistics for '{}': {e}", meta.location)
        })
}
