//! `view_external_table_statistics(table_name)` — table function that returns
//! per-file statistics for every file belonging to an external (listing) table.
//!
//! For each file the listing table would scan, the function looks it up in the
//! [`BeaconFileStatisticsCache`] and emits one row per column when a valid
//! cache entry exists, or a single null row when the file is not yet cached.
//!
//! # Output schema
//!
//! | column      | type    | nullable | description                                        |
//! |-------------|---------|----------|----------------------------------------------------|
//! | path        | Utf8    | no       | Object-store path of the file                      |
//! | file_size   | UInt64  | no       | File size in bytes                                 |
//! | cached      | Boolean | no       | true when a valid statistics entry exists in cache |
//! | column_name | Utf8    | yes      | Schema field name (null when not cached)           |
//! | data_type   | Utf8    | yes      | Arrow data-type string (null when not cached)      |
//! | min_value   | Utf8    | yes      | Minimum value, null when unknown or not cached     |
//! | max_value   | Utf8    | yes      | Maximum value, null when unknown or not cached     |
//! | is_exact    | Boolean | yes      | true=exact, false=inexact, null=absent/not cached  |
//!
//! [`BeaconFileStatisticsCache`]: beacon_datafusion_ext::stats_cache::BeaconFileStatisticsCache

use std::sync::{Arc, Weak};

use arrow::{
    array::{BooleanArray, BooleanBuilder, RecordBatch, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use beacon_datafusion_ext::{stats_cache::BeaconFileStatisticsCache, table_ext::ExternalTable};
use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    common::{plan_datafusion_err, plan_err},
    datasource::MemTable,
    execution::cache::CacheAccessor,
    logical_expr::{Signature, Volatility},
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};
use futures::TryStreamExt;
use object_store::ObjectMeta;
use tokio::runtime::Handle;

use super::helpers::column_stat_rows;
use crate::file_formats::BeaconTableFunctionImpl;

// ─── Output schema ───────────────────────────────────────────────────────────

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("file_size", DataType::UInt64, false),
        Field::new("cached", DataType::Boolean, false),
        Field::new("column_name", DataType::Utf8, true),
        Field::new("data_type", DataType::Utf8, true),
        Field::new("min_value", DataType::Utf8, true),
        Field::new("max_value", DataType::Utf8, true),
        Field::new("is_exact", DataType::Boolean, true),
    ]))
}

// ─── Function struct ─────────────────────────────────────────────────────────

pub struct ViewExternalTableStatisticsFunc {
    cache: Arc<BeaconFileStatisticsCache>,
    runtime_handle: Handle,
    session_ctx: Weak<SessionContext>,
}

impl ViewExternalTableStatisticsFunc {
    pub fn new(
        session_ctx: Weak<SessionContext>,
        cache: Arc<BeaconFileStatisticsCache>,
        runtime_handle: Handle,
    ) -> Self {
        Self {
            cache,
            runtime_handle,
            session_ctx,
        }
    }
}

impl std::fmt::Debug for ViewExternalTableStatisticsFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ViewExternalTableStatisticsFunc")
    }
}

impl BeaconTableFunctionImpl for ViewExternalTableStatisticsFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "view_external_table_statistics".to_string()
    }

    fn description(&self) -> Option<String> {
        Some(
            "Returns per-file statistics for every file in an external table, \
             showing which files have cached statistics and their per-column min/max values."
                .to_string(),
        )
    }

    fn arguments(&self) -> Option<Vec<Field>> {
        Some(vec![Field::new("table_name", DataType::Utf8, false)])
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![DataType::Utf8], Volatility::Volatile)
    }
}

// ─── TableFunctionImpl ───────────────────────────────────────────────────────

impl TableFunctionImpl for ViewExternalTableStatisticsFunc {
    fn call(
        &self,
        args: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let table_name = match args.first() {
            Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => s.clone(),
            _ => {
                return plan_err!(
                    "view_external_table_statistics requires a single Utf8 table name argument"
                )
            }
        };

        let session_ctx = self.session_ctx.upgrade().ok_or_else(|| {
            plan_datafusion_err!("session context has been dropped")
        })?;

        let (schema, files) = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                let provider = session_ctx
                    .table_provider(table_name.as_str())
                    .await
                    .map_err(|e| plan_datafusion_err!("Table '{table_name}' not found: {e}"))?;
                let state = session_ctx.state();

                let external = provider
                    .as_any()
                    .downcast_ref::<ExternalTable>()
                    .ok_or_else(|| {
                        plan_datafusion_err!("'{table_name}' is not an external table")
                    })?;

                let listing_table = external.inner();
                let schema = listing_table.schema();

                // List every file that belongs to this listing table. Each
                // `table_url` (produced by the listing factory when the table was
                // created) carries its own store URL, so the store is resolved from
                // the URL itself — no assumption about a single datasets store.
                // `table_paths()` carries the prefix and optional glob; `contains()`
                // handles glob matching so we don't need DataFusion's internal
                // `list_files_for_scan`.
                let mut files: Vec<ObjectMeta> = Vec::new();
                for table_url in listing_table.table_paths() {
                    let store = state
                        .runtime_env()
                        .object_store(table_url.object_store())
                        .map_err(|e| {
                            plan_datafusion_err!(
                                "failed to resolve object store for {table_url}: {e}"
                            )
                        })?;
                    let listed: Vec<ObjectMeta> = table_url
                        .list_all_files(&state, store.as_ref(), "")
                        .await?
                        .try_collect()
                        .await?;
                    files.extend(listed);
                }

                Ok::<_, datafusion::error::DataFusionError>((schema, files))
            })
        })?;

        let output_schema = output_schema();

        let mut paths: Vec<Option<String>> = Vec::new();
        let mut file_sizes: Vec<Option<u64>> = Vec::new();
        let mut cached_col: BooleanBuilder = BooleanBuilder::new();
        let mut column_names: Vec<Option<String>> = Vec::new();
        let mut data_types_col: Vec<Option<String>> = Vec::new();
        let mut min_values: Vec<Option<String>> = Vec::new();
        let mut max_values: Vec<Option<String>> = Vec::new();
        let mut is_exact: Vec<Option<bool>> = Vec::new();

        for meta in &files {
            let path_str = meta.location.to_string();

            let rows = self
                .cache
                .get_with_extra(&meta.location, meta)
                .map(|stats| column_stat_rows(schema.fields(), &stats.column_statistics));

            match rows {
                Some(rows) if !rows.is_empty() => {
                    for row in rows {
                        paths.push(Some(path_str.clone()));
                        file_sizes.push(Some(meta.size));
                        cached_col.append_value(true);
                        column_names.push(row.column_name);
                        data_types_col.push(row.data_type);
                        min_values.push(row.min_value);
                        max_values.push(row.max_value);
                        is_exact.push(row.is_exact);
                    }
                }
                cached => {
                    // Either not in cache (`None`) or cached with no column stats (`Some([])`).
                    paths.push(Some(path_str));
                    file_sizes.push(Some(meta.size));
                    cached_col.append_value(cached.is_some());
                    column_names.push(None);
                    data_types_col.push(None);
                    min_values.push(None);
                    max_values.push(None);
                    is_exact.push(None);
                }
            }
        }

        let batch = RecordBatch::try_new(
            output_schema.clone(),
            vec![
                Arc::new(StringArray::from(paths)),
                Arc::new(UInt64Array::from(file_sizes)),
                Arc::new(cached_col.finish()),
                Arc::new(StringArray::from(column_names)),
                Arc::new(StringArray::from(data_types_col)),
                Arc::new(StringArray::from(min_values)),
                Arc::new(StringArray::from(max_values)),
                Arc::new(BooleanArray::from(is_exact)),
            ],
        )
        .map_err(|e| plan_datafusion_err!("Failed to build record batch: {e}"))?;

        Ok(Arc::new(MemTable::try_new(
            output_schema,
            vec![vec![batch]],
        )?))
    }
}
