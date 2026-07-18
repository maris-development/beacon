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

use std::sync::{Arc, Weak};

use arrow::{
    array::{BooleanArray, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use beacon_arrow_netcdf::datafusion::{reader, statistics};
use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use datafusion::{
    catalog::TableFunctionImpl,
    common::{plan_datafusion_err, plan_err, Statistics},
    datasource::{
        file_format::{self, FileFormat},
        listing::ListingTable,
        MemTable,
    },
    execution::{cache::CacheAccessor, object_store::ObjectStoreUrl},
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

use crate::file_formats::BeaconTableFunctionImpl;
use super::helpers::{ColumnStatRow, column_stat_rows};

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
    session_ctx: Weak<SessionContext>,
    /// URL the datasets store is registered under, resolved from the session's
    /// object-store registry at call time.
    datasets_url: ObjectStoreUrl,
}

impl ViewDatasetStatisticsFunc {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_ctx: Weak<SessionContext>,
        datasets_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            runtime_handle,
            session_ctx,
            datasets_url,
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

        let session_ctx = self.session_ctx.upgrade().ok_or_else(|| {
            plan_datafusion_err!("session context has been dropped")
        })?;
        let datasets_url = self.datasets_url.clone();

        let (schema, stats) = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                let path = object_store::path::Path::parse(&path_str)
                    .map_err(|e| plan_datafusion_err!("Invalid path '{path_str}': {e}"))?;
                let store = session_ctx
                    .state()
                    .runtime_env()
                    .object_store(datasets_url)
                    .map_err(|e| {
                        plan_datafusion_err!("failed to resolve datasets object store: {e}")
                    })?;

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
                    if let Some(cached) = cache
                        .get(&meta.location)
                        .filter(|cached| cached.is_valid_for(&meta))
                    {
                        return Ok::<_, datafusion::error::DataFusionError>((
                            schema.clone(),
                            Arc::clone(&cached.statistics),
                        ));
                    }
                }

                let stats = Arc::new(
                    file_stats(session_ctx, store, file_format, schema.clone(), &meta).await?,
                );

                if let Some(cache) = cache {
                    cache.put(
                        &meta.location,
                        datafusion::execution::cache::cache_manager::CachedFileMetadata::new(
                            meta.clone(),
                            Arc::clone(&stats),
                            None,
                        ),
                    );
                }

                Ok((schema, stats))
            })
        })?;

        let batch = build_record_batch(&schema, &stats)?;
        let mem_table = MemTable::try_new(output_schema(), vec![vec![batch]])?;
        Ok(Arc::new(mem_table))
    }
}

fn build_record_batch(
    schema: &arrow::datatypes::Schema,
    stats: &Statistics,
) -> datafusion::error::Result<RecordBatch> {
    let rows = column_stat_rows(schema.fields(), &stats.column_statistics);
    RecordBatch::try_new(
        output_schema(),
        vec![
            Arc::new(StringArray::from_iter(rows.iter().map(|r| r.column_name.as_deref()))),
            Arc::new(StringArray::from_iter(rows.iter().map(|r| r.data_type.as_deref()))),
            Arc::new(StringArray::from_iter(rows.iter().map(|r| r.min_value.as_deref()))),
            Arc::new(StringArray::from_iter(rows.iter().map(|r| r.max_value.as_deref()))),
            Arc::new(BooleanArray::from(rows.iter().map(|r| r.is_exact).collect::<Vec<_>>())),
        ],
    )
    .map_err(|e| plan_datafusion_err!("Failed to build statistics record batch: {e}"))
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
