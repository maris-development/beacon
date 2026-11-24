//! The NetCDF [`FileFormat`]: schema inference, statistics, and the read and write plans.

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_datafusion_ext::type_widening::{label_by_object, session_widening};
use beacon_datafusion_ext::unique_values::UniqueValuesExec;
use beacon_nd_execution::exec::nd_scan_plan;
use beacon_nd_scan::{
    encoded_table_schema, file_statistics, open_for_read, to_datafusion, NdFileOpener, NdSource,
};
use datafusion::catalog::Session;
use datafusion::common::{exec_datafusion_err, plan_err, Statistics};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::{
    FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource,
};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::{LexOrdering, LexRequirement, PhysicalSortExpr};
use datafusion::physical_plan::sorts::{
    sort::SortExec, sort_preserving_merge::SortPreservingMergeExec,
};
use datafusion::physical_plan::ExecutionPlan;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};

use super::options::NetcdfOptions;
use super::sink::{NetCDFNdSink, NetCDFSink};
use super::NETCDF_EXTENSION;
use crate::nd::NetcdfOpener;

#[derive(Debug, Clone)]
pub struct NetcdfFormat {
    pub listing_factory: Arc<ListingFactory>,
    pub options: NetcdfOptions,
    /// Whether `infer_stats` computes file statistics.
    enable_statistics: bool,
}

impl NetcdfFormat {
    pub fn new(listing_factory: Arc<ListingFactory>, options: NetcdfOptions) -> Self {
        Self {
            listing_factory,
            options,
            enable_statistics: false,
        }
    }

    /// Set whether `infer_stats` computes file statistics.
    pub fn with_enable_statistics(mut self, enable_statistics: bool) -> Self {
        self.enable_statistics = enable_statistics;
        self
    }

    fn source(&self, table_schema: TableSchema) -> NdSource {
        NdSource::new(
            Arc::new(NetcdfOpener),
            self.options.scan_options(),
            table_schema,
        )
    }

    /// The schema of one file. A file that a skip option passes over has no column.
    async fn file_schema(
        &self,
        store: &Arc<dyn ObjectStore>,
        object: &ObjectMeta,
    ) -> Result<SchemaRef> {
        let opened = open_for_read(
            &NetcdfOpener,
            Arc::clone(store),
            object,
            &self.options.scan_options(),
        )
        .await?;
        let Some(dataset) = opened else {
            return Ok(Arc::new(Schema::empty()));
        };
        Ok(Arc::new(beacon_nd_scan::file_schema(dataset.as_ref())))
    }

    /// The statistics of one file for the columns of `table_schema`.
    async fn file_statistics(
        &self,
        store: &Arc<dyn ObjectStore>,
        object: &ObjectMeta,
        table_schema: &Schema,
    ) -> Result<Statistics> {
        let dataset = NetcdfOpener
            .open(Arc::clone(store), object)
            .await
            .map_err(to_datafusion)?;
        file_statistics(dataset.as_ref(), table_schema).await
    }

    /// The plan that writes one variable per column, on one `obs` dimension.
    fn flat_writer_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Arc<dyn ExecutionPlan> {
        let sink = Arc::new(NetCDFSink::new(conf, self.options.writer.clone()));
        Arc::new(DataSinkExec::new(input, sink, order_requirements))
    }

    /// The plan that writes a grid on the dimensions `dimension_columns`.
    fn grid_writer_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
        dimension_columns: &[String],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let flat_keys = self.options.writer.set_keys();
        if !flat_keys.is_empty() {
            return Err(exec_datafusion_err!(
                "{} applies only to the flat NetCDF output",
                flat_keys.join(", ")
            ));
        }

        let (unique_exec, collection_handle) =
            UniqueValuesExec::new(input, dimension_columns.to_vec())?;
        // The sink writes the cells of the grid in the order of its dimensions.
        let schema = unique_exec.schema();
        let sort_exprs = dimension_columns
            .iter()
            .map(|column| {
                Ok(PhysicalSortExpr::new_default(
                    datafusion::physical_expr::expressions::col(column, &schema)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let lex_order = LexOrdering::new(sort_exprs).ok_or(exec_datafusion_err!(
            "Failed to create LexOrdering for NetCDF dimension columns"
        ))?;
        let sort = SortExec::new(lex_order.clone(), Arc::new(unique_exec));
        let merge = SortPreservingMergeExec::new(lex_order, Arc::new(sort));

        let sink = Arc::new(NetCDFNdSink::new(
            conf,
            dimension_columns.len(),
            collection_handle,
        )?);
        Ok(Arc::new(DataSinkExec::new(
            Arc::new(merge),
            sink,
            order_requirements,
        )))
    }
}

#[async_trait::async_trait]
impl FileFormat for NetcdfFormat {
    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        NETCDF_EXTENSION.to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        Ok(NETCDF_EXTENSION.to_string())
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        // Bounded, because each open holds a file descriptor until its schema is
        // read. `try_join_all` polls every future at once, so a table over a
        // hundred thousand objects opened a hundred thousand files before the
        // first one closed and died with `Too many open files` — the crash in
        // issue #361. `buffered` keeps the width to the session's
        // `meta_fetch_concurrency`, the same knob Parquet's inference uses, and
        // preserves order so the merged schema does not depend on which file
        // finished first.
        let width = state
            .config_options()
            .execution
            .meta_fetch_concurrency
            .max(1);
        // An index, not a borrowed `ObjectMeta`: `async_trait` rejects a closure
        // argument that holds a reference.
        let schemas: Vec<SchemaRef> = futures::stream::iter(0..objects.len())
            .map(|index| async move {
                let object = &objects[index];
                // A glob can match many files, so the error names the bad one.
                self.file_schema(store, object).await.map_err(|e| {
                    let message = format!(
                        "Failed to infer the schema of NetCDF file '{}': {}",
                        object.location,
                        e.message()
                    );
                    // A ragged file stays NotImplemented, so a caller can tell it apart.
                    match e {
                        DataFusionError::NotImplemented(_) => {
                            DataFusionError::NotImplemented(message)
                        }
                        _ => exec_datafusion_err!("{message}"),
                    }
                })
            })
            .buffered(width)
            .try_collect()
            .await?;
        if schemas.is_empty() {
            return Ok(Arc::new(Schema::empty()));
        }
        // The rule of the session decides the result for a column that two
        // files describe differently. Each schema names its file, so a refused
        // column names both files.
        session_widening(state)
            .merge_schemas(&label_by_object(objects, &schemas))
            .map_err(|e| {
                exec_datafusion_err!("Failed to merge the schemas of the NetCDF datasets: {}", e)
            })
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        if !self.enable_statistics {
            return Ok(Statistics::new_unknown(&table_schema));
        }
        tracing::trace!(object = %object.location, "generating statistics for NetCDF file");
        Ok(self
            .file_statistics(store, object, &table_schema)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(
                    "Failed to generate statistics for object {}: {}",
                    object.location,
                    e
                );
                Statistics::new_unknown(&table_schema)
            }))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let partition_columns: Vec<&str> = conf
            .table_partition_cols()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        if !partition_columns.is_empty() {
            return Err(DataFusionError::NotImplemented(format!(
                "NetCDF does not support partitioned tables (PARTITIONED BY {})",
                partition_columns.join(", ")
            )));
        }
        // Keep a projection that the scan pushed into the incoming source.
        let projection = conf.file_source().projection().cloned();
        if projection.as_ref().is_some_and(names_no_column) {
            return plan_err!(
                "a NetCDF scan needs at least one file column; use COUNT(column) in place of COUNT(*)"
            );
        }
        let source = self
            .source(encoded_table_schema(conf.file_schema()))
            .with_projection(projection);
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();
        nd_scan_plan(conf)
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // The sinks write a local scratch file and upload it to the store of `conf`.
        match &self.options.write_dimensions {
            Some(columns) if !columns.is_empty() => {
                self.grid_writer_plan(input, conf, order_requirements, columns)
            }
            _ => Ok(self.flat_writer_plan(input, conf, order_requirements)),
        }
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(self.source(table_schema))
    }
}

/// Whether `projection` reads no column, as `COUNT(*)` does. A NetCDF table has no partition column.
fn names_no_column(projection: &ProjectionExprs) -> bool {
    projection
        .iter()
        .all(|item| collect_columns(&item.expr).is_empty())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::datafusion::test_util::*;

    /// A format with statistics on computes real ranges.
    #[tokio::test]
    async fn a_format_with_statistics_on_computes_ranges() {
        let ctx = one_partition_session();
        let state = ctx.state();
        let (store, object) = local_object(&test_file(GRIDDED_FILE)).await;

        let format = format().with_enable_statistics(true);
        let schema = format
            .infer_schema(&state, &store, std::slice::from_ref(&object))
            .await
            .expect("a schema");

        let statistics = format
            .infer_stats(&state, &store, schema, &object)
            .await
            .expect("statistics are never an error");
        assert!(
            statistics
                .column_statistics
                .iter()
                .any(|column| column.min_value.get_value().is_some()),
            "the coordinate variables get real ranges"
        );
    }

    /// A format with statistics off computes nothing.
    #[tokio::test]
    async fn a_format_with_statistics_off_computes_nothing() {
        let ctx = one_partition_session();
        let state = ctx.state();
        let (store, object) = local_object(&test_file(GRIDDED_FILE)).await;

        let oxcdf = format().with_enable_statistics(true);
        let schema = oxcdf
            .infer_schema(&state, &store, std::slice::from_ref(&object))
            .await
            .unwrap();

        let off = format().with_enable_statistics(false);
        let statistics = off
            .infer_stats(&state, &store, schema, &object)
            .await
            .unwrap();
        assert!(
            statistics
                .column_statistics
                .iter()
                .all(|column| column.min_value.get_value().is_none()),
            "enable_statistics=false must still mean no statistics"
        );
    }

    /// One bad file in a glob must be named, so the user can find it.
    #[tokio::test]
    async fn infer_schema_names_the_invalid_file() {
        // `put` and `head` live on this extension trait.
        use object_store::ObjectStoreExt;

        let ctx = one_partition_session();
        let store = Arc::new(object_store::memory::InMemory::new());
        let location = object_store::path::Path::from("argo/broken.nc");
        store
            .put(&location, b"not a netcdf file".to_vec().into())
            .await
            .expect("the in-memory store accepts the object");
        let object = store
            .head(&location)
            .await
            .expect("the object was just written");
        let object_store: Arc<dyn ObjectStore> = store;

        let error = format()
            .infer_schema(&ctx.state(), &object_store, &[object])
            .await
            .expect_err("random bytes are not a NetCDF file");

        let message = error.to_string();
        assert!(
            message.contains("'argo/broken.nc'"),
            "the error must name the file: {message}"
        );
        assert_eq!(
            message.matches("Execution error").count(),
            1,
            "the error must not repeat its prefix: {message}"
        );
    }

    /// What a statistics backfill actually costs per file.
    ///
    /// Run it with:
    ///
    /// ```text
    /// cargo test --release -p beacon-arrow-netcdf --lib \
    ///     statistics_backfill_cost -- --ignored --nocapture
    /// ```
    ///
    /// Ignored because it is a measurement, not an assertion. It reads the same
    /// file repeatedly, so the bytes are in the page cache: this is the parse
    /// and range-scan cost, and a **lower bound**. A cold local disk adds seek
    /// time, and object storage adds a round trip per file that will usually
    /// dominate everything here.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "measurement, not an assertion"]
    async fn statistics_backfill_cost() {
        use futures::StreamExt;
        use std::time::Instant;

        const ITERATIONS: usize = 200;
        const CONCURRENCY: usize = 8;

        for file in [GRIDDED_FILE] {
            let ctx = one_partition_session();
            let state = ctx.state();
            let (store, object) = local_object(&test_file(file)).await;
            let format = Arc::new(format().with_enable_statistics(true));

            let schema = format
                .infer_schema(&state, &store, std::slice::from_ref(&object))
                .await
                .unwrap();
            let bytes = object.size;

            // Serial: schema + stats, which is what the analyzer does per file.
            let start = Instant::now();
            for _ in 0..ITERATIONS {
                let schema = format
                    .infer_schema(&state, &store, std::slice::from_ref(&object))
                    .await
                    .unwrap();
                let _ = format
                    .infer_stats(&state, &store, schema, &object)
                    .await
                    .unwrap();
            }
            let serial = start.elapsed().as_secs_f64() / ITERATIONS as f64;

            // Concurrent: the shape a collector runs in.
            let start = Instant::now();
            futures::stream::iter(0..ITERATIONS)
                .map(|_| {
                    let format = format.clone();
                    let state = state.clone();
                    let store = store.clone();
                    let object = object.clone();
                    async move {
                        let schema = format
                            .infer_schema(&state, &store, std::slice::from_ref(&object))
                            .await
                            .unwrap();
                        format
                            .infer_stats(&state, &store, schema, &object)
                            .await
                            .unwrap()
                    }
                })
                .buffer_unordered(CONCURRENCY)
                .collect::<Vec<_>>()
                .await;
            let concurrent_total = start.elapsed().as_secs_f64();
            let rate = ITERATIONS as f64 / concurrent_total;

            // The same work spawned onto the runtime rather than polled from one
            // task. `buffer_unordered` gives concurrency, not parallelism: if the
            // work is CPU bound between await points, every future runs on the
            // single task polling them.
            let start = Instant::now();
            let mut set = tokio::task::JoinSet::new();
            for _ in 0..ITERATIONS {
                let format = format.clone();
                let state = state.clone();
                let store = store.clone();
                let object = object.clone();
                set.spawn(async move {
                    let schema = format
                        .infer_schema(&state, &store, std::slice::from_ref(&object))
                        .await
                        .unwrap();
                    format
                        .infer_stats(&state, &store, schema, &object)
                        .await
                        .unwrap()
                });
            }
            while set.join_next().await.is_some() {}
            let spawned_rate = ITERATIONS as f64 / start.elapsed().as_secs_f64();

            let ranged = {
                let statistics = format
                    .infer_stats(&state, &store, schema.clone(), &object)
                    .await
                    .unwrap();
                statistics
                    .column_statistics
                    .iter()
                    .filter(|c| c.min_value.get_value().is_some())
                    .count()
            };

            println!(
                "\n{file}  ({} KiB, {} columns, {ranged} with ranges)\n  \
                 serial               : {:.1} ms/file ({:.0} files/s)\n  \
                 buffer_unordered({CONCURRENCY})  : {:.0} files/s   -> {:.1} h for 1M\n  \
                 spawned              : {:.0} files/s   -> {:.1} h for 1M",
                bytes / 1024,
                schema.fields().len(),
                serial * 1e3,
                1.0 / serial,
                rate,
                1_000_000.0 / rate / 3600.0,
                spawned_rate,
                1_000_000.0 / spawned_rate / 3600.0,
            );
        }
    }

    #[tokio::test]
    async fn combine_attrs_with_write_dimensions_is_an_error() {
        use datafusion::datasource::physical_plan::{FileGroup, FileOutputMode};
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion::logical_expr::dml::InsertOp;
        use datafusion::physical_plan::empty::EmptyExec;

        let format = NetcdfFormat::new(
            Arc::new(ListingFactory::dynamic()),
            NetcdfOptions {
                write_dimensions: Some(vec!["lat".to_string()]),
                writer: crate::datafusion::options::NetcdfWriterOptions {
                    combine_attrs: Some(crate::attributes::CombineAttrs::Identical),
                    ..Default::default()
                },
                ..NetcdfOptions::default()
            },
        );
        let ctx = one_partition_session();
        let schema: SchemaRef = Arc::new(arrow::datatypes::Schema::empty());
        let conf = FileSinkConfig {
            original_url: String::new(),
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_group: FileGroup::default(),
            table_paths: vec![],
            output_schema: schema.clone(),
            table_partition_cols: vec![],
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension: "nc".to_string(),
            file_output_mode: FileOutputMode::SingleFile,
        };
        let error = format
            .create_writer_physical_plan(Arc::new(EmptyExec::new(schema)), &ctx.state(), conf, None)
            .await
            .err()
            .expect("combine_attrs with write_dimensions is an error")
            .to_string();
        assert!(error.contains("combine_attrs"), "{error}");
    }

    /// A partitioned table is refused, and the error names the partition column.
    #[tokio::test]
    async fn a_partitioned_table_is_refused() {
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };

        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("year=2023");
        std::fs::create_dir_all(&part).unwrap();
        std::fs::copy(test_file(GRIDDED_FILE), part.join("obs.nc")).unwrap();

        let options = ListingOptions::new(Arc::new(format()))
            .with_file_extension("")
            .with_collect_stat(false)
            .with_table_partition_cols(vec![(
                "year".to_string(),
                arrow::datatypes::DataType::Utf8,
            )]);
        let ctx = one_partition_session();
        let url = ListingTableUrl::parse(dir.path().to_string_lossy()).unwrap();
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .infer_schema(&ctx.state())
            .await
            .unwrap();
        ctx.register_table("obs", Arc::new(ListingTable::try_new(config).unwrap()))
            .unwrap();

        let error = ctx
            .sql("SELECT lat FROM obs")
            .await
            .unwrap()
            .collect()
            .await
            .expect_err("a partitioned table is refused");

        assert!(
            matches!(
                error.find_root(),
                datafusion::error::DataFusionError::NotImplemented(_)
            ),
            "{error}"
        );
        assert!(error.to_string().contains("PARTITIONED BY year"), "{error}");
    }
}
