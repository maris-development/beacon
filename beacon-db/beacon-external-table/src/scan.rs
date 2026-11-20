//! The scan of an [`ExternalTable`](crate::ExternalTable).
//!
//! The scan lists the files, drops the files that the statistics provider
//! rules out, and then lets the format plan the scan over the kept files.
//! The format therefore never opens a pruned file.

use std::{collections::HashSet, sync::Arc};

use beacon_datafusion_ext::type_widening::ArrowTypeWideningStrategy;
use datafusion::{
    arrow::datatypes::{Field, FieldRef, Schema, SchemaRef},
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
    logical_expr::utils::conjunction,
    physical_expr::utils::collect_columns,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{ExecutionPlan, empty::EmptyExec, metrics::MetricBuilder},
    prelude::Expr,
};
use futures::{TryStreamExt, future::try_join_all};
use object_store::{ObjectMeta, ObjectStore};

use crate::{
    error::ScanPhase, scan_errors::ScanErrorPolicy, scan_errors_source::with_scan_errors,
    statistics_cache::ExternalTableStatisticsProvider,
};

/// The statistics provider of a scan, with the name of the table it keeps the statistics under.
pub(crate) type ScanStatistics<'a> = (&'a dyn ExternalTableStatisticsProvider, &'a str);

/// Plans a scan of `table` and prunes its files with the provider of `statistics`.
///
/// The scan config gets the adapter of `type_widening`, the strategy of the
/// table. Only a format that reads the `PhysicalExprAdapterFactory` of its
/// config uses it, which is Parquet.
///
/// `errors` decides what a file that the scan cannot read does: it fails the
/// scan with the table, the file type and the file, or it ends and the scan
/// continues with the next file. A list or plan failure always fails the scan
/// with the table and the file type.
///
/// Without a provider, the scan reads all listed files. A filter on partition
/// columns only prunes the listing, as in a DataFusion `ListingTable`.
pub(crate) async fn scan_with_statistics(
    session: &dyn Session,
    table: &ListingTable,
    statistics: Option<ScanStatistics<'_>>,
    type_widening: Arc<dyn ArrowTypeWideningStrategy>,
    errors: ScanErrorPolicy,
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
    let list_error = |error| errors.context().error(ScanPhase::List, None, error);
    let store = session
        .runtime_env()
        .object_store(first_url)
        .map_err(list_error)?;

    let (partition_filters, data_filters) = split_filters(options, filters);
    let files = list_files(
        session,
        store.as_ref(),
        table.table_paths(),
        &partition_filters,
        options,
    )
    .await
    .map_err(list_error)?;

    let considered = files.len();
    let (files, pruned) = match statistics {
        Some((provider, name)) if !data_filters.is_empty() => {
            prune_files(session, provider, name, &table_schema, &data_filters, files).await
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
        .with_expr_adapter(Some(type_widening.physical_expr_adapter_factory()))
        .build();
    let plan = options
        .format
        .create_physical_plan(session, config)
        .await
        .map_err(|error| errors.context().error(ScanPhase::Plan, None, error))?;
    let plan = with_scan_errors(plan, errors);

    if statistics.is_some() {
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

/// Drops the files that the statistics of `provider` for table `name` rule out for `filters`.
///
/// Returns the kept files and the number of dropped files. A file without
/// statistics stays. Each failure keeps all files: a prune must never lose rows.
pub(crate) async fn prune_files(
    session: &dyn Session,
    provider: &dyn ExternalTableStatisticsProvider,
    name: &str,
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

    let objects: Vec<&ObjectMeta> = files.iter().map(|file| &file.object_meta).collect();
    let fetched = provider
        .fetch_statistics(name, &objects, predicate_schema(&predicate, table_schema))
        .await;
    let statistics = match fetched {
        Ok(statistics) => statistics,
        Err(error) => {
            tracing::debug!(%error, "the statistics provider failed; keeping all files");
            return (files, 0);
        }
    };
    if statistics.num_containers() != files.len() {
        tracing::debug!(
            containers = statistics.num_containers(),
            files = files.len(),
            "the statistics do not match the files; keeping all files"
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

    let before = files.len();
    let kept: Vec<PartitionedFile> = files
        .into_iter()
        .zip(keep)
        .filter_map(|(file, keep)| keep.then_some(file))
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

/// The columns of `schema` that `predicate` reads, in schema order.
///
/// A provider fetches statistics per column, so a wide table must not ask for all columns.
fn predicate_schema(predicate: &PruningPredicate, schema: &SchemaRef) -> SchemaRef {
    let names: HashSet<String> = collect_columns(predicate.orig_expr())
        .into_iter()
        .map(|column| column.name().to_string())
        .collect();
    let fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .filter(|field| names.contains(field.name()))
        .cloned()
        .collect();
    Arc::new(Schema::new(fields))
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
pub(crate) mod tests {
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

    use beacon_datafusion_ext::type_widening::DefaultArrowTypeWidening;

    use super::*;
    use crate::{
        error::{ErrorContext, ExternalTableError},
        scan_errors::OnScanError,
        statistics_cache::{SharedPruningStatistics, StatisticsRegistration},
    };

    /// The policy of a table `obs` that fails on a file error.
    fn fail_policy(file_type: &str) -> ScanErrorPolicy {
        ScanErrorPolicy::new(ErrorContext::new("obs", file_type), OnScanError::Fail)
    }

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
    pub(crate) struct TestProvider {
        ranges: HashMap<&'static str, (f64, f64)>,
        fail: bool,
        wrong_count: bool,
        /// The registrations the provider got, in order.
        pub(crate) registrations: parking_lot::Mutex<Vec<StatisticsRegistration>>,
        /// Whether `register_table` refuses each table.
        refuse: bool,
        /// The tables the provider dropped.
        pub(crate) dropped: parking_lot::Mutex<Vec<String>>,
        /// The table name and the column names of the last fetch.
        seen: parking_lot::Mutex<Option<(String, Vec<String>)>>,
    }

    impl TestProvider {
        pub(crate) fn with_ranges(ranges: &[(&'static str, f64, f64)]) -> Self {
            Self {
                ranges: ranges
                    .iter()
                    .map(|(name, min, max)| (*name, (*min, *max)))
                    .collect(),
                ..Self::default()
            }
        }

        /// A provider that refuses each registration.
        pub(crate) fn refusing() -> Self {
            Self {
                refuse: true,
                ..Self::default()
            }
        }
    }

    impl ExternalTableStatisticsProvider for TestProvider {
        fn register_table(
            &self,
            registration: StatisticsRegistration,
        ) -> BoxFuture<'static, anyhow::Result<()>> {
            self.registrations.lock().push(registration);
            let refuse = self.refuse;
            Box::pin(async move {
                if refuse {
                    return Err(anyhow!("too many statistics columns"));
                }
                Ok(())
            })
        }

        fn drop_table(&self, table: &str) -> BoxFuture<'static, anyhow::Result<()>> {
            self.dropped.lock().push(table.to_string());
            Box::pin(async { Ok(()) })
        }

        fn fetch_statistics(
            &self,
            table: &str,
            objects: &[&ObjectMeta],
            schema: SchemaRef,
        ) -> BoxFuture<'static, anyhow::Result<SharedPruningStatistics>> {
            let columns = schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();
            *self.seen.lock() = Some((table.to_string(), columns));
            if self.fail {
                return Box::pin(async { Err(anyhow!("the store is down")) });
            }
            // An object without a range reads as null, so the prune keeps it.
            let ranges: Vec<Option<(f64, f64)>> = objects
                .iter()
                .map(|object| {
                    let name = object.location.filename()?;
                    self.ranges.get(name).copied()
                })
                .collect();
            let min = Float64Array::from_iter(ranges.iter().map(|range| range.map(|r| r.0)));
            let max = Float64Array::from_iter(ranges.iter().map(|range| range.map(|r| r.1)));
            let statistics: SharedPruningStatistics = Arc::new(TempStatistics {
                min: Arc::new(min),
                max: Arc::new(max),
                containers: objects.len() + usize::from(self.wrong_count),
            });
            Box::pin(async move { Ok(statistics) })
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "temp",
            DataType::Float64,
            true,
        )]))
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
            "obs",
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
    async fn the_prune_keeps_the_order_of_the_files() {
        let provider = TestProvider::with_ranges(&[
            ("a.csv", 20.0, 30.0),
            ("b.csv", 0.0, 5.0),
            ("c.csv", 40.0, 50.0),
        ]);
        let (kept, dropped) = prune(&provider, &["c.csv", "b.csv", "x.csv", "a.csv"]).await;
        assert_eq!(names(&kept), ["c.csv", "x.csv", "a.csv"]);
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
            "obs",
            &schema(),
            &[lit(true)],
            files(&["a.csv"]),
        )
        .await;
        assert_eq!(names(&kept), ["a.csv"]);
        assert_eq!(dropped, 0);
    }

    #[tokio::test]
    async fn the_provider_gets_the_table_name_and_only_the_predicate_columns() {
        let provider = TestProvider::with_ranges(&[("a.csv", 0.0, 5.0)]);
        let wide: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("depth", DataType::Float64, true),
            Field::new("temp", DataType::Float64, true),
            Field::new("psal", DataType::Float64, true),
        ]));
        let session = SessionContext::new().state();
        prune_files(
            &session,
            &provider,
            "obs",
            &wide,
            &[col("temp").gt(lit(10.0))],
            files(&["a.csv"]),
        )
        .await;
        assert_eq!(
            *provider.seen.lock(),
            Some(("obs".to_string(), vec!["temp".to_string()]))
        );
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
        provider: Option<&dyn ExternalTableStatisticsProvider>,
        type_widening: Arc<dyn ArrowTypeWideningStrategy>,
    ) -> (usize, Option<usize>) {
        let dir = tempfile::tempdir().unwrap();
        let table = csv_table(dir.path());
        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = scan_with_statistics(
            &state,
            &table,
            provider.map(|provider| (provider, "obs")),
            type_widening,
            fail_policy("csv"),
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
        let provider = TestProvider::with_ranges(&[("a.csv", 1.0, 5.0), ("b.csv", 20.0, 30.0)]);
        let (rows, pruned) =
            scan_rows(Some(&provider), Arc::new(DefaultArrowTypeWidening::new())).await;
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
    async fn the_scan_config_holds_the_adapter_of_the_table_strategy() {
        let dir = tempfile::tempdir().unwrap();
        let table = csv_table(dir.path());
        let state = SessionContext::new().state();
        let plan = scan_with_statistics(
            &state,
            &table,
            None,
            Arc::new(DefaultArrowTypeWidening::new()),
            fail_policy("csv"),
            None,
            &[],
            None,
        )
        .await
        .unwrap();
        let exec = plan.downcast_ref::<DataSourceExec>().unwrap();
        let config = exec.data_source().downcast_ref::<FileScanConfig>().unwrap();
        assert!(config.expr_adapter_factory.is_some());
    }

    #[tokio::test]
    async fn a_store_that_is_not_registered_fails_in_the_list_phase() {
        let url = ListingTableUrl::parse("s3://no-such-bucket/data/").unwrap();
        let options = ListingOptions::new(Arc::new(CsvFormat::default())).with_file_extension(".csv");
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(schema());
        let table = ListingTable::try_new(config).unwrap();
        let state = SessionContext::new().state();

        let error = scan_with_statistics(
            &state,
            &table,
            None,
            Arc::new(DefaultArrowTypeWidening::new()),
            fail_policy("csv"),
            None,
            &[],
            None,
        )
        .await
        .unwrap_err();

        let found = ExternalTableError::find(&error).expect("an external table error");
        assert_eq!(found.table, "obs");
        assert_eq!(found.phase, ScanPhase::List);
        assert_eq!(found.object, None);
        assert!(
            error.to_string().contains("external table 'obs' (csv): cannot list the files"),
            "{error}"
        );
    }

    fn write_parquet(path: &std::path::Path, column: ArrayRef) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "temp",
            column.data_type().clone(),
            true,
        )]));
        let batch =
            datafusion::arrow::array::RecordBatch::try_new(Arc::clone(&schema), vec![column])
                .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer =
            datafusion::parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// `a.parquet` holds `Int32` 1, `b.parquet` holds `Utf8` "abc". The table declares `Int32`.
    async fn parquet_scan(
        type_widening: Arc<dyn ArrowTypeWideningStrategy>,
    ) -> Result<Vec<datafusion::arrow::array::RecordBatch>> {
        let dir = tempfile::tempdir().unwrap();
        write_parquet(
            &dir.path().join("a.parquet"),
            Arc::new(datafusion::arrow::array::Int32Array::from(vec![1])),
        );
        write_parquet(
            &dir.path().join("b.parquet"),
            Arc::new(datafusion::arrow::array::StringArray::from(vec!["abc"])),
        );
        let url = ListingTableUrl::parse(format!("{}/", dir.path().display())).unwrap();
        let options = ListingOptions::new(Arc::new(
            datafusion::datasource::file_format::parquet::ParquetFormat::default(),
        ))
        .with_file_extension(".parquet");
        let declared = Arc::new(Schema::new(vec![Field::new("temp", DataType::Int32, true)]));
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(declared);
        let table = ListingTable::try_new(config).unwrap();
        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = scan_with_statistics(
            &state,
            &table,
            None,
            type_widening,
            fail_policy("parquet"),
            None,
            &[],
            None,
        )
        .await?;
        collect(plan, ctx.task_ctx()).await
    }

    #[tokio::test]
    async fn parquet_reads_a_value_of_another_family_as_null_with_the_table_strategy() {
        let batches = parquet_scan(Arc::new(DefaultArrowTypeWidening::keeping_first_type()))
            .await
            .unwrap();
        let mut values: Vec<Option<i32>> = batches
            .iter()
            .flat_map(|batch| {
                let column = batch.column(0);
                let column = column
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int32Array>()
                    .unwrap();
                column.iter().collect::<Vec<_>>()
            })
            .collect();
        values.sort();
        assert_eq!(values, [None, Some(1)]);
    }
}
