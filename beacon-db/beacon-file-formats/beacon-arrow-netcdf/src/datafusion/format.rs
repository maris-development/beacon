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
