//! The scan of an [`ExternalTable`](crate::ExternalTable).
//!
//! The scan lists the files, drops the files that the statistics provider
//! rules out, and then lets the format plan the scan over the kept files.
//! The format therefore never opens a pruned file.

use std::{collections::HashSet, sync::Arc};

use beacon_datafusion_ext::type_widening::ArrowTypeWideningStrategy;
use datafusion::{
    arrow::datatypes::{Field, FieldRef, SchemaRef},
    catalog::{Session, TableProvider},
    common::{DFSchema, Result, Statistics, project_schema},
    datasource::{
        listing::{
            ListingOptions, ListingTable, ListingTableUrl, PartitionedFile,
            helpers::{expr_applicable_for_cols, pruned_partition_list},
        },
        physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder},
        source::DataSourceExec,
        table_schema::TableSchema,
    },
    execution::object_store::ObjectStoreUrl,
    logical_expr::utils::conjunction,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{ExecutionPlan, empty::EmptyExec, metrics::MetricBuilder},
    prelude::Expr,
};
use futures::{TryStreamExt, future::try_join_all};
use object_store::{ObjectMeta, ObjectStore, path::Path as ObjectPath};

use crate::statistics_cache::ExternalTableStatisticsProvider;

/// Plans a scan of `table` and prunes its files with `provider`.
///
/// `provider` gets `type_widening`, the strategy of the table.
///
/// Without a provider, the scan reads all listed files. A filter on partition
/// columns only prunes the listing, as in a DataFusion `ListingTable`.
pub(crate) async fn scan_with_statistics(
    session: &dyn Session,
    table: &ListingTable,
    provider: Option<&Arc<dyn ExternalTableStatisticsProvider>>,
    type_widening: Arc<dyn ArrowTypeWideningStrategy>,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let options = table.options();
    let table_schema = table.schema();
    let empty = || -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(project_schema(
            &table_schema,
            projection,
        )?)))
    };

    let Some(first_url) = table.table_paths().first() else {
        return empty();
    };
    let store_url = first_url.object_store();
    let store = session.runtime_env().object_store(first_url)?;

    let (partition_filters, data_filters) = split_filters(options, filters);
    let files = list_files(
        session,
        store.as_ref(),
        table.table_paths(),
        &partition_filters,
        options,
    )
    .await?;

    let considered = files.len();
    let (files, pruned) = match provider {
        Some(provider) if !data_filters.is_empty() => {
            prune_files(
                session,
                provider.as_ref(),
                type_widening,
                &store_url,
                &table_schema,
                &data_filters,
                files,
            )
            .await
        }
        _ => (files, 0),
    };
    if files.is_empty() {
        return empty();
    }

    let groups = FileGroup::new(files).split_files(options.target_partitions);
    let output_ordering = table.try_create_output_ordering(session.execution_props(), &groups)?;
    let config = FileScanConfigBuilder::new(store_url, file_source(options, &table_schema))
        .with_file_groups(groups)
        .with_constraints(table.constraints().cloned().unwrap_or_default())
        // The listing collects no per-file statistics, so the scan statistics are unknown.
        .with_statistics(Statistics::new_unknown(&table_schema))
        .with_projection_indices(projection.cloned())?
        .with_limit(limit)
        .with_output_ordering(output_ordering)
        .build();
    let plan = options.format.create_physical_plan(session, config).await?;

    if provider.is_some() {
        record_counters(&plan, considered, pruned);
    }
    Ok(plan)
}

/// Splits `filters` into the filters on partition columns only and the other filters.
fn split_filters(options: &ListingOptions, filters: &[Expr]) -> (Vec<Expr>, Vec<Expr>) {
    let partition_names: Vec<&str> = options
        .table_partition_cols
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    filters.iter().cloned().partition(|filter| {
        !partition_names.is_empty() && expr_applicable_for_cols(&partition_names, filter)
    })
}

/// Lists the files of all table paths. Partition filters skip whole partition folders.
async fn list_files(
    session: &dyn Session,
    store: &dyn ObjectStore,
    table_paths: &[ListingTableUrl],
    partition_filters: &[Expr],
    options: &ListingOptions,
) -> Result<Vec<PartitionedFile>> {
    let listings = try_join_all(table_paths.iter().map(|url| async move {
        pruned_partition_list(
            session,
            store,
            url,
            partition_filters,
            &options.file_extension,
            &options.table_partition_cols,
        )
        .await?
        .try_collect::<Vec<_>>()
        .await
    }))
    .await?;
    Ok(listings.into_iter().flatten().collect())
}

/// The file source of the format, over the file schema and the partition columns.
fn file_source(
    options: &ListingOptions,
    table_schema: &SchemaRef,
) -> Arc<dyn datafusion::datasource::physical_plan::FileSource> {
    let partition_fields: Vec<FieldRef> = options
        .table_partition_cols
        .iter()
        .map(|(name, data_type)| Arc::new(Field::new(name, data_type.clone(), false)))
        .collect();
    // The table schema is the file schema with the partition columns at the end.
    let file_field_count = table_schema.fields().len() - partition_fields.len();
    let file_schema = Arc::new(datafusion::arrow::datatypes::Schema::new_with_metadata(
        table_schema.fields()[..file_field_count].to_vec(),
        table_schema.metadata().clone(),
    ));
    options
        .format
        .file_source(TableSchema::new(file_schema, partition_fields))
}

/// Drops the files that the statistics of `provider` rule out for `filters`.
///
/// Returns the kept files and the number of dropped files. A file without
/// statistics stays. Each failure keeps all files: a prune must never lose rows.
pub(crate) async fn prune_files(
    session: &dyn Session,
    provider: &dyn ExternalTableStatisticsProvider,
    type_widening: Arc<dyn ArrowTypeWideningStrategy>,
    store_url: &ObjectStoreUrl,
    table_schema: &SchemaRef,
    filters: &[Expr],
    files: Vec<PartitionedFile>,
) -> (Vec<PartitionedFile>, usize) {
    if files.is_empty() {
        return (files, 0);
    }
    let Some(predicate) = pruning_predicate(session, table_schema, filters) else {
        return (files, 0);
    };

    let objects: Vec<ObjectMeta> = files.iter().map(|file| file.object_meta.clone()).collect();
    let fetched = provider
        .fetch_statistics(
            store_url.clone(),
            &objects,
            type_widening,
            Arc::clone(table_schema),
        )
        .await;
    let (returned, statistics) = match fetched {
        Ok(fetched) => fetched,
        Err(error) => {
            tracing::debug!(%error, "the statistics provider failed; keeping all files");
            return (files, 0);
        }
    };
    if statistics.num_containers() != returned.len() {
        tracing::debug!(
            containers = statistics.num_containers(),
            objects = returned.len(),
            "the statistics do not match the returned objects; keeping all files"
        );
        return (files, 0);
    }
    let keep = match predicate.prune(statistics.as_ref()) {
        Ok(keep) => keep,
        Err(error) => {
            tracing::debug!(%error, "the prune failed; keeping all files");
            return (files, 0);
        }
    };

    let ruled_out: HashSet<&ObjectPath> = returned
        .iter()
        .zip(&keep)
        .filter(|(_, keep)| !**keep)
        .map(|(object, _)| &object.location)
        .collect();
    if ruled_out.is_empty() {
        return (files, 0);
    }
    let before = files.len();
    let kept: Vec<PartitionedFile> = files
        .into_iter()
        .filter(|file| !ruled_out.contains(&file.object_meta.location))
        .collect();
    let dropped = before - kept.len();
    (kept, dropped)
}

/// The pruning predicate for `filters` over `schema`, or `None` when it cannot drop a file.
fn pruning_predicate(
    session: &dyn Session,
    schema: &SchemaRef,
    filters: &[Expr],
) -> Option<PruningPredicate> {
    let predicate = conjunction(filters.iter().cloned())?;
    let df_schema = DFSchema::try_from(schema.as_ref().clone()).ok()?;
    let predicate = session.create_physical_expr(predicate, &df_schema).ok()?;
    let pruning = PruningPredicate::try_new(predicate, Arc::clone(schema)).ok()?;
    (!pruning.always_true()).then_some(pruning)
}

/// Adds the prune counters to the file scan, so that `EXPLAIN ANALYZE` shows them.
///
/// A format can put nodes above its scan, so the walk follows single children down.
fn record_counters(plan: &Arc<dyn ExecutionPlan>, considered: usize, pruned: usize) {
    let mut node: &dyn ExecutionPlan = plan.as_ref();
    loop {
        if let Some(exec) = node.downcast_ref::<DataSourceExec>() {
            let Some(config) = exec.data_source().downcast_ref::<FileScanConfig>() else {
                return;
            };
            let metrics = datafusion::datasource::source::DataSource::metrics(config);
            MetricBuilder::new(&metrics)
                .global_counter("statistics_files_considered")
                .add(considered);
            MetricBuilder::new(&metrics)
                .global_counter("statistics_files_pruned")
                .add(pruned);
            return;
        }
        let children = node.children();
        let [child] = children[..] else { return };
        node = child.as_ref();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::anyhow;
    use datafusion::{
        arrow::{
            array::{ArrayRef, BooleanArray, Float64Array},
            datatypes::{DataType, Schema},
        },
        common::{Column, ScalarValue, pruning::PruningStatistics},
        datasource::{file_format::csv::CsvFormat, listing::ListingTableConfig},
        physical_plan::collect,
        prelude::{SessionContext, col, lit},
    };
    use futures::future::BoxFuture;

    use beacon_datafusion_ext::type_widening::{DefaultArrowTypeWidening, NumpyArrowTypeWidening};

    use super::*;
    use crate::statistics_cache::SharedPruningStatistics;

    /// Statistics for one column, `temp`.
    struct TempStatistics {
        min: ArrayRef,
        max: ArrayRef,
        containers: usize,
    }

    impl PruningStatistics for TempStatistics {
        fn min_values(&self, column: &Column) -> Option<ArrayRef> {
            (column.name == "temp").then(|| Arc::clone(&self.min))
        }

        fn max_values(&self, column: &Column) -> Option<ArrayRef> {
            (column.name == "temp").then(|| Arc::clone(&self.max))
        }

        fn num_containers(&self) -> usize {
            self.containers
        }

        fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
            None
        }

        fn row_counts(&self) -> Option<ArrayRef> {
            None
        }

        fn contained(
            &self,
            _column: &Column,
            _values: &HashSet<ScalarValue>,
        ) -> Option<BooleanArray> {
            None
        }
    }

    /// A provider with a `temp` range per file name.
    #[derive(Default)]
    struct TestProvider {
        ranges: HashMap<&'static str, (f64, f64)>,
        fail: bool,
        wrong_count: bool,
        /// The `Debug` form of the strategy of the last fetch.
        seen_type_widening: parking_lot::Mutex<Option<String>>,
    }

    impl TestProvider {
        fn with_ranges(ranges: &[(&'static str, f64, f64)]) -> Self {
            Self {
                ranges: ranges
                    .iter()
                    .map(|(name, min, max)| (*name, (*min, *max)))
                    .collect(),
                ..Self::default()
            }
        }
    }

    impl ExternalTableStatisticsProvider for TestProvider {
        fn is_stale(&self) -> BoxFuture<'static, bool> {
            Box::pin(async { false })
        }

        fn fetch_statistics(
            &self,
            _store_url: ObjectStoreUrl,
            objects: &[ObjectMeta],
            type_widening: Arc<dyn ArrowTypeWideningStrategy>,
            _schema: SchemaRef,
        ) -> BoxFuture<'static, anyhow::Result<(Vec<ObjectMeta>, SharedPruningStatistics)>>
        {
            *self.seen_type_widening.lock() = Some(format!("{type_widening:?}"));
            if self.fail {
                return Box::pin(async { Err(anyhow!("the store is down")) });
            }
            let returned: Vec<(ObjectMeta, (f64, f64))> = objects
                .iter()
                .filter_map(|object| {
                    let name = object.location.filename()?;
                    self.ranges.get(name).map(|range| (object.clone(), *range))
                })
                .collect();
            let min = Float64Array::from_iter_values(returned.iter().map(|(_, r)| r.0));
            let max = Float64Array::from_iter_values(returned.iter().map(|(_, r)| r.1));
            let containers = returned.len() + usize::from(self.wrong_count);
            let statistics: SharedPruningStatistics = Arc::new(TempStatistics {
                min: Arc::new(min),
                max: Arc::new(max),
                containers,
            });
            let objects = returned.into_iter().map(|(object, _)| object).collect();
            Box::pin(async move { Ok((objects, statistics)) })
        }

        fn refresh(&self) -> BoxFuture<'static, anyhow::Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn refresh_objects(
            &self,
            _store_url: ObjectStoreUrl,
            _store: Arc<dyn ObjectStore>,
            _objects: &[ObjectMeta],
        ) -> BoxFuture<'static, anyhow::Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("temp", DataType::Float64, true)]))
    }

    fn files(names: &[&str]) -> Vec<PartitionedFile> {
        names
            .iter()
            .map(|name| PartitionedFile::new(*name, 10))
            .collect()
    }

    fn names(files: &[PartitionedFile]) -> Vec<String> {
        files
            .iter()
            .map(|file| file.object_meta.location.to_string())
            .collect()
    }

    async fn prune(provider: &TestProvider, names: &[&str]) -> (Vec<PartitionedFile>, usize) {
        let session = SessionContext::new().state();
        prune_files(
            &session,
            provider,
            Arc::new(DefaultArrowTypeWidening::new()),
            &ObjectStoreUrl::local_filesystem(),
            &schema(),
            &[col("temp").gt(lit(10.0))],
            files(names),
        )
        .await
    }

    #[tokio::test]
    async fn drops_a_file_whose_range_cannot_match() {
        let provider = TestProvider::with_ranges(&[("a.csv", 0.0, 5.0), ("b.csv", 0.0, 20.0)]);
        let (kept, dropped) = prune(&provider, &["a.csv", "b.csv"]).await;
        assert_eq!(names(&kept), ["b.csv"]);
        assert_eq!(dropped, 1);
    }

    #[tokio::test]
    async fn keeps_a_file_without_statistics() {
        let provider = TestProvider::with_ranges(&[("a.csv", 0.0, 5.0)]);
        let (kept, dropped) = prune(&provider, &["a.csv", "c.csv"]).await;
        assert_eq!(names(&kept), ["c.csv"]);
        assert_eq!(dropped, 1);
    }

    #[tokio::test]
    async fn a_provider_error_keeps_all_files() {
        let provider = TestProvider {
            fail: true,
            ..TestProvider::with_ranges(&[("a.csv", 0.0, 5.0)])
        };
        let (kept, dropped) = prune(&provider, &["a.csv", "b.csv"]).await;
        assert_eq!(names(&kept), ["a.csv", "b.csv"]);
        assert_eq!(dropped, 0);
    }

    #[tokio::test]
    async fn a_container_count_mismatch_keeps_all_files() {
        let provider = TestProvider {
            wrong_count: true,
            ..TestProvider::with_ranges(&[("a.csv", 0.0, 5.0)])
        };
        let (kept, dropped) = prune(&provider, &["a.csv", "b.csv"]).await;
        assert_eq!(names(&kept), ["a.csv", "b.csv"]);
        assert_eq!(dropped, 0);
    }

    #[tokio::test]
    async fn a_predicate_on_an_unknown_shape_keeps_all_files() {
        let provider = TestProvider::with_ranges(&[("a.csv", 0.0, 5.0)]);
        let session = SessionContext::new().state();
        // A predicate without a column cannot drop a file.
        let (kept, dropped) = prune_files(
            &session,
            &provider,
            Arc::new(DefaultArrowTypeWidening::new()),
            &ObjectStoreUrl::local_filesystem(),
            &schema(),
            &[lit(true)],
            files(&["a.csv"]),
        )
        .await;
        assert_eq!(names(&kept), ["a.csv"]);
        assert_eq!(dropped, 0);
    }

    /// Two CSV files: `a.csv` holds 1 and 5, `b.csv` holds 20 and 30.
    fn csv_table(dir: &std::path::Path) -> ListingTable {
        std::fs::write(dir.join("a.csv"), "temp\n1.0\n5.0\n").unwrap();
        std::fs::write(dir.join("b.csv"), "temp\n20.0\n30.0\n").unwrap();
        let url = ListingTableUrl::parse(format!("{}/", dir.display())).unwrap();
        let options = ListingOptions::new(Arc::new(CsvFormat::default().with_has_header(true)))
            .with_file_extension(".csv");
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(schema());
        ListingTable::try_new(config).unwrap()
    }

    async fn scan_rows(
        provider: Option<&Arc<dyn ExternalTableStatisticsProvider>>,
        type_widening: Arc<dyn ArrowTypeWideningStrategy>,
    ) -> (usize, Option<usize>) {
        let dir = tempfile::tempdir().unwrap();
        let table = csv_table(dir.path());
        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = scan_with_statistics(
            &state,
            &table,
            provider,
            type_widening,
            None,
            &[col("temp").gt(lit(10.0))],
            None,
        )
        .await
        .unwrap();
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
        let rows = batches.iter().map(|batch| batch.num_rows()).sum();
        let pruned = plan
            .metrics()
            .and_then(|metrics| metrics.sum_by_name("statistics_files_pruned"))
            .map(|value| value.as_usize());
        (rows, pruned)
    }

    #[tokio::test]
    async fn the_scan_reads_only_the_kept_files() {
        let provider: Arc<dyn ExternalTableStatisticsProvider> = Arc::new(
            TestProvider::with_ranges(&[("a.csv", 1.0, 5.0), ("b.csv", 20.0, 30.0)]),
        );
        let (rows, pruned) = scan_rows(Some(&provider), Arc::new(DefaultArrowTypeWidening::new())).await;
        // The scan returns all rows of the kept file; a filter above it removes rows.
        assert_eq!(rows, 2);
        assert_eq!(pruned, Some(1));
    }

    #[tokio::test]
    async fn without_a_provider_the_scan_reads_all_files() {
        let (rows, pruned) = scan_rows(None, Arc::new(DefaultArrowTypeWidening::new())).await;
        assert_eq!(rows, 4);
        assert_eq!(pruned, None);
    }

    #[tokio::test]
    async fn the_provider_gets_the_table_strategy() {
        let provider = Arc::new(TestProvider::with_ranges(&[("a.csv", 1.0, 5.0)]));
        let dynamic: Arc<dyn ExternalTableStatisticsProvider> = provider.clone();
        let numpy: Arc<dyn ArrowTypeWideningStrategy> = Arc::new(NumpyArrowTypeWidening::new());
        scan_rows(Some(&dynamic), numpy).await;
        assert_eq!(
            provider.seen_type_widening.lock().as_deref(),
            Some("NumpyArrowTypeWidening { on_conflict: Fail }")
        );
    }
}
