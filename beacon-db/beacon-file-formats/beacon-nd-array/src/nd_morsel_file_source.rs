//! DataFusion [`FileSource`] for any nd format, scheduled as morsels.
//!
//! A format implements only [`NdFileOpener`], and calls [`nd_scan_plan`] from
//! `FileFormat::create_physical_plan`.

use std::{
    fmt,
    sync::{Arc, OnceLock},
};

use beacon_datafusion_ext::{
    morsel_planner::{MorselPlanner, MorselPlannerQueue},
    nd::{
        encoded_schema,
        exec::{NdBroadcastExec, NdSourceExec},
        logical_schema,
    },
};
use datafusion::{
    common::internal_datafusion_err,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileGroup, FileOpener, FileScanConfig, FileScanConfigBuilder, FileSource},
        source::DataSourceExec,
        table_schema::TableSchema,
    },
    error::Result,
    physical_expr::{LexOrdering, expressions::Column, projection::ProjectionExprs},
    physical_plan::{ExecutionPlan, metrics::ExecutionPlanMetricsSet},
};
use object_store::ObjectStore;

use crate::{
    arrow::morsel::reject_partition_columns, nd_file_opener::NdFileOpener,
    nd_morsel_planner::NdMorselPlanner,
};

/// Files below this size give morsels that stay with the partition that plans them.
const LOCAL_FILE_MAX_SIZE: u64 = 1 << 20;

/// DataFusion [`FileSource`] for the files of an nd format.
///
/// Each file is cut into morsels: one per chunk of a gridded file, one per range
/// of casts of a ragged file. A [`MorselPlannerQueue`] gives them to the partitions.
/// Files below `local_file_max_size` keep their morsels in the partition that
/// plans them. Other partitions can steal the morsels of larger files.
#[derive(Debug, Clone)]
pub struct NdMorselFileSource {
    opener: Arc<dyn NdFileOpener>,
    metrics: ExecutionPlanMetricsSet,
    file_type: String,
    table_schema: TableSchema, // nd-encoded, as `NdSourceExec` above the scan expects
    batch_size: usize,
    projection: Option<ProjectionExprs>, // Plain file columns only
    local_file_max_size: u64,

    morsel_planner_queue: Option<Arc<MorselPlannerQueue>>,
}

impl NdMorselFileSource {
    /// Create the source of a scan.
    ///
    /// # Arguments
    ///
    /// * `opener` - The opener that reads a file of the format into a dataset
    /// * `file_type` - The name of the format, for plans and errors
    /// * `table_schema` - The nd-encoded table schema, as `NdSourceExec` above the scan expects
    pub fn new(
        opener: Arc<dyn NdFileOpener>,
        file_type: String,
        table_schema: TableSchema,
    ) -> Self {
        Self {
            opener,
            file_type,
            table_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            batch_size: usize::MAX,
            projection: None,
            local_file_max_size: LOCAL_FILE_MAX_SIZE,
            morsel_planner_queue: None,
        }
    }

    /// The same source, with another size limit for files whose morsels stay local.
    pub fn with_local_file_max_size(mut self, size: u64) -> Self {
        self.local_file_max_size = size;
        self
    }

    /// The same source, with the given projection.
    ///
    /// # Errors
    ///
    /// Returns an error when the projection is not a list of file columns.
    pub fn with_projection(mut self, projection: Option<ProjectionExprs>) -> Result<Self> {
        if let Some(projection) = &projection
            && !is_column_projection(projection, &self.table_schema)
        {
            return Err(internal_datafusion_err!(
                "NdMorselFileSource can only project file columns, not {projection:?}"
            ));
        }
        self.projection = projection;
        Ok(self)
    }
}

impl FileSource for NdMorselFileSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let queue = self
            .morsel_planner_queue
            .as_ref()
            .ok_or_else(|| internal_datafusion_err!("Morsel planner queue not initialized"))?;
        let partition_morsels = queue
            .partition(Arc::clone(&object_store), partition)
            .map_err(|e| internal_datafusion_err!("Failed to get partition morsels: {e}"))?;
        Ok(Arc::new(partition_morsels))
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            batch_size,
            ..self.clone()
        })
    }

    fn supports_repartitioning(&self) -> bool {
        true
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        todo!(
            "Repartitioning should fill the morsel queue and initialize the morsels planner etc.."
        )
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        &self.file_type
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.projection.as_ref()
    }

    /// Take a projection of plain file columns. Decline any other, so the projection stays above.
    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let merged = match &self.projection {
            Some(existing) => existing.try_merge(projection)?,
            None => projection.clone(),
        };
        if !is_column_projection(&merged, &self.table_schema) {
            return Ok(None);
        }
        Ok(Some(Arc::new(Self {
            projection: Some(merged),
            ..self.clone()
        })))
    }
}

/// True when each expression of `projection` is a file column under its own name.
fn is_column_projection(projection: &ProjectionExprs, table_schema: &TableSchema) -> bool {
    let file_columns = table_schema.file_schema().fields().len();
    projection.iter().all(|projected| {
        projected
            .expr
            .downcast_ref::<Column>()
            .is_some_and(|column| column.index() < file_columns && column.name() == projected.alias)
    })
}

/// Split `files` into the files below `max_size`, and the rest.
pub(crate) fn split_by_size(
    files: impl IntoIterator<Item = PartitionedFile>,
    max_size: u64,
) -> (Vec<PartitionedFile>, Vec<PartitionedFile>) {
    files
        .into_iter()
        .partition(|file| file.object_meta.size < max_size)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use arrow::{
        array::RecordBatch,
        compute::concat_batches,
        datatypes::{DataType, Field, Schema, SchemaRef},
    };
    use datafusion::{
        execution::object_store::ObjectStoreUrl,
        physical_expr::{expressions::lit, projection::ProjectionExpr},
        physical_plan::collect,
        prelude::SessionContext,
    };
    use futures::future::BoxFuture;
    use indexmap::IndexMap;

    use super::*;
    use crate::dataset_v2::{
        Dataset,
        morsel::test_util::{array, nullable_i32_values, schema, text},
    };

    /// Gives `grid_<i>.nc` a 2x3 `temp` grid with values `10 * i + 0..6`, and
    /// `ragged.nc` three casts with 2, 1 and 3 depths.
    #[derive(Debug)]
    struct TestOpener;

    impl NdFileOpener for TestOpener {
        fn open(
            &self,
            _store: Arc<dyn ObjectStore>,
            file: PartitionedFile,
        ) -> BoxFuture<'static, anyhow::Result<Dataset>> {
            let name = file.object_meta.location.to_string();
            Box::pin(async move {
                let arrays = match name.strip_prefix("grid_") {
                    Some(rest) => {
                        let i: i32 = rest.trim_end_matches(".nc").parse()?;
                        IndexMap::from([
                            (
                                "temp".to_string(),
                                array(
                                    (0..6).map(|v| 10 * i + v).collect(),
                                    &[2, 3],
                                    &["time", "lat"],
                                ),
                            ),
                            ("lat".to_string(), array(vec![1, 2, 3], &[3], &["lat"])),
                        ])
                    }
                    None => IndexMap::from([
                        (
                            "station".to_string(),
                            array(vec![1, 2, 3], &[3], &["casts"]),
                        ),
                        (
                            "z_row_size".to_string(),
                            array(vec![2, 1, 3], &[3], &["casts"]),
                        ),
                        ("z_row_size.sample_dimension".to_string(), text("z_obs")),
                        (
                            "depth".to_string(),
                            array((0..6).collect(), &[6], &["z_obs"]),
                        ),
                    ]),
                };
                Dataset::from_arrays(name, arrays, HashSet::new())
            })
        }
    }

    fn grid_schema() -> SchemaRef {
        schema(&[("temp", DataType::Int32), ("lat", DataType::Int32)])
    }

    /// A source on the nd-encoded form of `logical`, with `partition_columns`.
    fn source(logical: &Schema, partition_columns: Vec<Arc<Field>>) -> NdMorselFileSource {
        let table_schema = TableSchema::new(Arc::new(encoded_schema(logical)), partition_columns);
        NdMorselFileSource::new(Arc::new(TestOpener), "nd".to_string(), table_schema)
    }

    fn config(source: NdMorselFileSource, files: &[(&str, u64)]) -> FileScanConfigBuilder {
        let files = files
            .iter()
            .map(|(name, size)| PartitionedFile::new(*name, *size))
            .collect();
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
            .with_file_group(FileGroup::new(files))
    }

    fn grid_files() -> Vec<(&'static str, u64)> {
        vec![("grid_0.nc", 10), ("grid_1.nc", 2 << 20), ("grid_2.nc", 20)]
    }

    /// Run the scan of `conf` on `partitions` partitions, and join its batches.
    async fn run(conf: FileScanConfig, partitions: usize) -> RecordBatch {
        let conf = match conf
            .file_source()
            .repartitioned(partitions, 0, None, &conf)
            .unwrap()
        {
            Some(repartitioned) => repartitioned,
            None => conf,
        };
        let plan = nd_exec(conf).unwrap();
        let batches = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        concat_batches(&batches[0].schema(), &batches).unwrap()
    }

    fn sorted_values(batch: &RecordBatch, name: &str) -> Vec<Option<i32>> {
        let mut values = nullable_i32_values(batch, name);
        values.sort();
        values
    }

    fn scan_of(config: &FileScanConfig) -> Arc<MorselScan> {
        let source = config
            .file_source
            .downcast_ref::<NdMorselFileSource>()
            .expect("an NdMorselFileSource");
        Arc::clone(source.scan.as_ref().expect("a morsel scan"))
    }

    // ── repartitioned ────────────────────────────────────────────────

    #[test]
    fn split_by_size_keeps_small_files_local() {
        let files = [10, LOCAL_FILE_MAX_SIZE, 5]
            .iter()
            .enumerate()
            .map(|(i, &size)| PartitionedFile::new(format!("file_{i}.nc"), size));

        let (local, shared) = split_by_size(files, LOCAL_FILE_MAX_SIZE);

        assert_eq!(local.len(), 2);
        assert_eq!(shared.len(), 1);
    }

    #[test]
    fn repartitioned_gives_one_placeholder_per_partition() {
        let source = source(&grid_schema(), vec![]);
        let config = config(source.clone(), &grid_files()).build();

        let config = source
            .repartitioned(4, 0, None, &config)
            .unwrap()
            .expect("a morsel scan");

        assert_eq!(config.file_groups.len(), 4);
        assert!(config.file_groups.iter().all(|group| group.len() == 1));
        let scan = scan_of(&config);
        assert_eq!(scan.local_files.len(), 2);
        assert_eq!(scan.shared_files.len(), 1);
        assert_eq!(scan.num_partitions, 4);
    }

    #[test]
    fn repartitioned_keeps_a_single_partition_as_planned() {
        let source = source(&grid_schema(), vec![]);
        let config = config(source.clone(), &[("grid_0.nc", 10)]).build();

        assert!(source.repartitioned(1, 0, None, &config).unwrap().is_none());
    }

    #[test]
    fn repartitioned_keeps_an_empty_scan_as_planned() {
        let source = source(&grid_schema(), vec![]);
        let config = config(source.clone(), &[]).build();

        assert!(source.repartitioned(4, 0, None, &config).unwrap().is_none());
    }

    // ── projection pushdown ──────────────────────────────────────────

    fn column(name: &str, index: usize) -> ProjectionExpr {
        ProjectionExpr::new(Arc::new(Column::new(name, index)), name)
    }

    fn pushdown(source: &NdMorselFileSource, exprs: Vec<ProjectionExpr>) -> bool {
        source
            .try_pushdown_projection(&ProjectionExprs::new(exprs))
            .unwrap()
            .is_some()
    }

    #[test]
    fn file_columns_in_any_order_are_pushed_down() {
        let source = source(&grid_schema(), vec![]);

        assert!(pushdown(&source, vec![column("lat", 1), column("temp", 0)]));
    }

    #[test]
    fn an_alias_is_not_pushed_down() {
        let source = source(&grid_schema(), vec![]);
        let alias = ProjectionExpr::new(Arc::new(Column::new("temp", 0)), "t");

        assert!(!pushdown(&source, vec![alias]));
    }

    #[test]
    fn a_computed_expression_is_not_pushed_down() {
        let source = source(&grid_schema(), vec![]);

        assert!(!pushdown(&source, vec![ProjectionExpr::new(lit(1), "one")]));
    }

    #[test]
    fn a_partition_column_is_not_pushed_down() {
        let year = Arc::new(Field::new("year", DataType::Utf8, false));
        let source = source(&grid_schema(), vec![year]);

        assert!(!pushdown(&source, vec![column("year", 2)]));
    }

    // ── partition columns ────────────────────────────────────────────

    #[test]
    fn a_partitioned_table_is_rejected() {
        let year = Arc::new(Field::new("year", DataType::Utf8, false));
        let source = source(&grid_schema(), vec![year]);
        let config = config(source.clone(), &grid_files()).build();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());

        assert!(source.create_file_opener(store, &config, 0).is_err());
    }

    // ── end to end ───────────────────────────────────────────────────

    #[tokio::test]
    async fn a_morsel_scan_reads_every_file() {
        let conf = config(source(&grid_schema(), vec![]), &grid_files()).build();

        let batch = run(conf, 4).await;

        let expected: Vec<Option<i32>> = (0..3)
            .flat_map(|i| (0..6).map(move |v| Some(10 * i + v)))
            .collect();
        assert_eq!(sorted_values(&batch, "temp"), expected);
        assert_eq!(batch.num_rows(), 18);
    }

    #[tokio::test]
    async fn one_partition_gives_the_same_rows() {
        let conf = |partitions| async move {
            let conf = config(source(&grid_schema(), vec![]), &grid_files()).build();
            sorted_values(&run(conf, partitions).await, "temp")
        };

        assert_eq!(conf(1).await, conf(4).await);
    }

    #[tokio::test]
    async fn a_projection_reads_only_its_columns() {
        let conf = config(source(&grid_schema(), vec![]), &grid_files())
            .with_projection_indices(Some(vec![1]))
            .unwrap()
            .build();

        let batch = run(conf, 4).await;

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "lat");
        // The grid follows the projection: `lat` alone gives 3 rows per file.
        assert_eq!(batch.num_rows(), 9);
    }

    #[tokio::test]
    async fn an_empty_projection_counts_every_row() {
        let conf = config(source(&grid_schema(), vec![]), &grid_files())
            .with_projection_indices(Some(vec![]))
            .unwrap()
            .build();
        let conf = conf
            .file_source()
            .repartitioned(4, 0, None, &conf)
            .unwrap()
            .unwrap();

        let plan = nd_exec(conf).unwrap();
        let batches = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 18);
    }

    #[tokio::test]
    async fn a_ragged_file_gives_the_rows_of_each_cast() {
        let logical = schema(&[("station", DataType::Int32), ("depth", DataType::Int32)]);
        let conf = config(source(&logical, vec![]), &[("ragged.nc", 10)]).build();

        let batch = run(conf, 1).await;

        assert_eq!(
            nullable_i32_values(&batch, "station"),
            [1, 1, 2, 3, 3, 3].map(Some)
        );
        assert_eq!(
            nullable_i32_values(&batch, "depth"),
            (0..6).map(Some).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn nd_scan_plan_keeps_the_pushed_down_projection() {
        // A format's own source, on the logical schema, as DataFusion planned it.
        let format_source = NdMorselFileSource::new(
            Arc::new(TestOpener),
            "nd",
            TableSchema::new(grid_schema(), vec![]),
        );
        let conf = config(format_source, &grid_files())
            .with_projection_indices(Some(vec![0]))
            .unwrap()
            .build();

        let plan = nd_scan_plan(conf, Arc::new(TestOpener), "nd").unwrap();
        let batches = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        let batch = concat_batches(&batches[0].schema(), &batches).unwrap();

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "temp");
        assert_eq!(batch.num_rows(), 18);
    }

    #[test]
    fn nd_scan_plan_rejects_a_partitioned_table() {
        let year = Arc::new(Field::new("year", DataType::Utf8, false));
        let format_source = NdMorselFileSource::new(
            Arc::new(TestOpener),
            "nd",
            TableSchema::new(grid_schema(), vec![year]),
        );
        let conf = config(format_source, &grid_files()).build();

        assert!(nd_scan_plan(conf, Arc::new(TestOpener), "nd").is_err());
    }
}
