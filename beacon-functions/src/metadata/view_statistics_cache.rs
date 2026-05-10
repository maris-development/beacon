//! `view_statistics_cache()` — table function that streams every entry in the
//! [`BeaconFileStatisticsCache`] as a record batch.
//!
//! Each row represents one file × column pair whose statistics have been
//! computed and cached (by DataFusion's `ListingTable` machinery or by a prior
//! call to `view_dataset_statistics`).
//!
//! # Output schema
//!
//! | column      | type    | nullable | description                                       |
//! |-------------|---------|----------|---------------------------------------------------|
//! | path        | Utf8    | no       | Object-store path of the cached file              |
//! | file_size   | UInt64  | no       | File size in bytes at cache time                  |
//! | is_valid    | Boolean | no       | true when head() confirms size+last_modified match |
//! | column_name | Utf8    | yes      | Field name (null when stats carry no names)       |
//! | data_type   | Utf8    | yes      | Arrow data-type string (null when unknown)        |
//! | min_value   | Utf8    | yes      | Minimum value, null when unknown                  |
//! | max_value   | Utf8    | yes      | Maximum value, null when unknown                  |
//! | is_exact    | Boolean | yes      | true=exact, false=inexact, null=absent            |
//!
//! [`BeaconFileStatisticsCache`]: beacon_datafusion_ext::stats_cache::BeaconFileStatisticsCache

use std::sync::Arc;

use arrow::{
    array::{BooleanArray, BooleanBuilder, RecordBatch, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use beacon_datafusion_ext::stats_cache::BeaconFileStatisticsCache;
use beacon_object_storage::get_datasets_object_store;
use datafusion::{
    catalog::TableFunctionImpl,
    common::plan_datafusion_err,
    datasource::MemTable,
    logical_expr::{Signature, Volatility},
    prelude::{Expr, SessionContext},
};
use object_store::ObjectStore;
use tokio::runtime::Handle;

use crate::file_formats::BeaconTableFunctionImpl;
use super::helpers::column_stat_rows;

// ─── Output schema ──────────────────────────────────────────────────────────

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("file_size", DataType::UInt64, false),
        Field::new("is_valid", DataType::Boolean, false),
        Field::new("column_name", DataType::Utf8, true),
        Field::new("data_type", DataType::Utf8, true),
        Field::new("min_value", DataType::Utf8, true),
        Field::new("max_value", DataType::Utf8, true),
        Field::new("is_exact", DataType::Boolean, true),
    ]))
}

// ─── Function struct ────────────────────────────────────────────────────────

pub struct ViewStatisticsCacheFunc {
    cache: Arc<BeaconFileStatisticsCache>,
    runtime_handle: Handle,
    _session_ctx: Arc<SessionContext>,
}

impl ViewStatisticsCacheFunc {
    pub fn new(
        session_ctx: Arc<SessionContext>,
        cache: Arc<BeaconFileStatisticsCache>,
        runtime_handle: Handle,
    ) -> Self {
        Self {
            cache,
            runtime_handle,
            _session_ctx: session_ctx,
        }
    }
}

impl std::fmt::Debug for ViewStatisticsCacheFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ViewStatisticsCacheFunc")
    }
}

impl BeaconTableFunctionImpl for ViewStatisticsCacheFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "view_statistics_cache".to_string()
    }

    fn description(&self) -> Option<String> {
        Some(
            "Streams all entries from the file statistics cache, \
             showing per-column min/max for every cached file. \
             Each row includes an is_valid flag verified against the object store."
                .to_string(),
        )
    }

    fn arguments(&self) -> Option<Vec<Field>> {
        Some(vec![])
    }

    // Zero-argument function — override the signature so DataFusion accepts the call.
    fn signature(&self) -> Signature {
        Signature::exact(vec![], Volatility::Volatile)
    }
}

// ─── TableFunctionImpl ──────────────────────────────────────────────────────

impl TableFunctionImpl for ViewStatisticsCacheFunc {
    fn call(
        &self,
        _args: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let entries = self.cache.list_entries();
        let schema = output_schema();

        // Validate each cached entry against the live object store by calling head().
        // call() is sync, so we bridge into async via block_in_place.
        let validations: Vec<bool> =
            tokio::task::block_in_place(|| {
                self.runtime_handle.block_on(async {
                    let store = get_datasets_object_store().await;
                    let mut results = Vec::with_capacity(entries.len());
                    for (path, cached_meta, _) in &entries {
                        let is_valid = match store.head(path).await {
                            Ok(current) => {
                                current.size == cached_meta.size
                                    && current.last_modified == cached_meta.last_modified
                            }
                            Err(_) => false,
                        };
                        results.push(is_valid);
                    }
                    results
                })
            });

        // Collect owned path strings first so we can borrow them below.
        let path_strings: Vec<String> = entries
            .iter()
            .map(|(path, _, _)| path.to_string())
            .collect();

        // Flatten: one row per (file × column).
        let mut paths: Vec<Option<&str>> = Vec::new();
        let mut file_sizes: Vec<Option<u64>> = Vec::new();
        let mut is_valid_col: BooleanBuilder = BooleanBuilder::new();
        let mut column_names: Vec<Option<String>> = Vec::new();
        let mut data_types: Vec<Option<String>> = Vec::new();
        let mut min_values: Vec<Option<String>> = Vec::new();
        let mut max_values: Vec<Option<String>> = Vec::new();
        let mut is_exact: Vec<Option<bool>> = Vec::new();

        for (((_, meta, stats), path_str), valid) in entries
            .iter()
            .zip(path_strings.iter())
            .zip(validations.iter())
        {
            // Schema is unavailable here; pass &[] so column_name/data_type stay null.
            let rows = column_stat_rows(&[], &stats.column_statistics);

            if rows.is_empty() {
                // File is cached but has no per-column data — one null row to surface the file.
                paths.push(Some(path_str.as_str()));
                file_sizes.push(Some(meta.size));
                is_valid_col.append_value(*valid);
                column_names.push(None);
                data_types.push(None);
                min_values.push(None);
                max_values.push(None);
                is_exact.push(None);
            } else {
                for row in rows {
                    paths.push(Some(path_str.as_str()));
                    file_sizes.push(Some(meta.size));
                    is_valid_col.append_value(*valid);
                    column_names.push(row.column_name);
                    data_types.push(row.data_type);
                    min_values.push(row.min_value);
                    max_values.push(row.max_value);
                    is_exact.push(row.is_exact);
                }
            }
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(paths)),
                Arc::new(UInt64Array::from(file_sizes)),
                Arc::new(is_valid_col.finish()),
                Arc::new(StringArray::from(column_names)),
                Arc::new(StringArray::from(data_types)),
                Arc::new(StringArray::from(min_values)),
                Arc::new(StringArray::from(max_values)),
                Arc::new(BooleanArray::from(is_exact)),
            ],
        )
        .map_err(|e| {
            plan_datafusion_err!("Failed to build statistics cache record batch: {e}")
        })?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

