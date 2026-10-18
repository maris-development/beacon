//! DataFusion [`FileSource`] for any nd format, scheduled as morsels.
//!
//! A format implements only [`NdFileOpener`].

use std::sync::{Arc, OnceLock};

use beacon_datafusion_ext::{morsel_planner::MorselPlannerQueue, nd::logical_schema};
use datafusion::{
    common::internal_datafusion_err,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileGroup, FileOpener, FileScanConfig, FileSource},
        table_schema::TableSchema,
    },
    error::Result,
    physical_expr::{LexOrdering, expressions::Column, projection::ProjectionExprs},
    physical_plan::metrics::ExecutionPlanMetricsSet,
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
///
/// The first opener of the scan builds the planner and the queue, so they see the
/// final projection and batch size.
#[derive(Debug, Clone)]
pub struct NdMorselFileSource {
    opener: Arc<dyn NdFileOpener>,
    metrics: ExecutionPlanMetricsSet,
    file_type: String,
    table_schema: TableSchema, // nd-encoded, as `NdSourceExec` above the scan expects
    batch_size: usize,
    projection: Option<ProjectionExprs>, // Plain file columns only
    local_file_max_size: u64,
    scan: Option<Arc<MorselScan>>, // Set by `repartitioned`: the files behind the placeholder groups
    unrepartitioned: Arc<OnceLock<Arc<MorselScan>>>, // The scan of the planned file groups, when `repartitioned` did not run
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
            scan: None,
            unrepartitioned: Arc::new(OnceLock::new()),
        }
    }

    /// The same source for a new scan shape, with a scan of its own.
    fn with_scan(&self, scan: Option<Arc<MorselScan>>) -> Self {
        Self {
            scan,
            unrepartitioned: Arc::new(OnceLock::new()),
            ..self.clone()
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
    /// Take the queue handle of `partition`.
    ///
    /// The first call of the scan builds the planner from `base_config` and the queue.
    /// Without `repartitioned`, one queue holds the files of all planned file groups.
    ///
    /// # Errors
    ///
    /// Returns an error for a table with `PARTITIONED BY` columns, or when `partition`
    /// already has a handle.
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        reject_partition_columns(&self.file_type, base_config)?;
        let scan = match &self.scan {
            Some(scan) => Arc::clone(scan),
            None => Arc::clone(self.unrepartitioned.get_or_init(|| {
                let files = base_config.file_groups.iter().flat_map(FileGroup::iter);
                Arc::new(MorselScan::new(
                    files.cloned().collect(),
                    base_config.file_groups.len(),
                    self.local_file_max_size,
                ))
            })),
        };
        let queue = scan.queue(|| {
            let projected = logical_schema(base_config.projected_schema()?.as_ref())?;
            Ok(Arc::new(NdMorselPlanner::new(
                Arc::clone(&self.opener),
                projected,
                None,
                self.batch_size,
            )))
        })?;
        let partition_morsels = queue
            .partition(object_store, partition)
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

    /// Put every file of the scan behind one morsel queue, and give each partition a placeholder.
    ///
    /// This only splits the files. The first opener builds the planner and the queue.
    /// `repartition_file_min_size` is unused: the queue balances on completion, not on size.
    ///
    /// # Errors
    ///
    /// Returns an error for a table with `PARTITIONED BY` columns.
    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        reject_partition_columns(&self.file_type, config)?;
        // The morsels of many files interleave, so the scan cannot keep an output order.
        if output_ordering.is_some() {
            return Ok(None);
        }

        let files: Vec<PartitionedFile> = config
            .file_groups
            .iter()
            .flat_map(FileGroup::iter)
            .cloned()
            .collect();
        let num_partitions = target_partitions.max(1);
        let total: u64 = files.iter().map(PartitionedFile::effective_size).sum();
        let placeholder =
            PartitionedFile::new(format!("nd-morsel-scan/{}-files", files.len()), total);
        let scan = MorselScan::new(files, num_partitions, self.local_file_max_size);

        let mut config = config.clone();
        config.file_groups = (0..num_partitions)
            .map(|_| FileGroup::new(vec![placeholder.clone()]))
            .collect();
        // The openers are built from the config's source, so the scan travels with it.
        config.file_source = Arc::new(self.with_scan(Some(Arc::new(scan))));
        Ok(Some(config))
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
            ..self.with_scan(self.scan.clone())
        })))
    }
}

/// The files of a morsel scan, and the queue that gives their morsels to the partitions.
#[derive(Debug)]
struct MorselScan {
    local_files: Vec<PartitionedFile>,
    shared_files: Vec<PartitionedFile>,
    num_partitions: usize,
    queue: OnceLock<MorselPlannerQueue>, // Built by the first opener, which has the final config
}

impl MorselScan {
    /// Split `files` by `local_file_max_size` for a queue of `num_partitions` partitions.
    fn new(files: Vec<PartitionedFile>, num_partitions: usize, local_file_max_size: u64) -> Self {
        let (local_files, shared_files) = split_by_size(files, local_file_max_size);
        Self {
            local_files,
            shared_files,
            num_partitions: num_partitions.max(1),
            queue: OnceLock::new(),
        }
    }

    /// The queue of the scan. The first call builds it with the planner of `planner`.
    ///
    /// # Errors
    ///
    /// Returns the error of `planner`.
    fn queue(
        &self,
        planner: impl FnOnce() -> Result<Arc<NdMorselPlanner>>,
    ) -> Result<MorselPlannerQueue> {
        if let Some(queue) = self.queue.get() {
            return Ok(queue.clone());
        }
        let planner = planner()?;
        // Two openers can build a planner at the same time; the queue keeps the first.
        let queue = self.queue.get_or_init(|| {
            MorselPlannerQueue::new(
                self.local_files.clone(),
                self.shared_files.clone(),
                planner,
                self.num_partitions,
            )
        });
        Ok(queue.clone())
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
    use beacon_datafusion_ext::nd::{
        encoded_schema,
        exec::{NdBroadcastExec, NdSourceExec},
    };
    use datafusion::{
        datasource::{physical_plan::FileScanConfigBuilder, source::DataSourceExec},
        execution::object_store::ObjectStoreUrl,
        physical_expr::{expressions::lit, projection::ProjectionExpr},
        physical_plan::{ExecutionPlan, collect},
        prelude::{SessionConfig, SessionContext},
    };
    use futures::future::BoxFuture;
    use indexmap::IndexMap;
    use object_store::ObjectStore;

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

    /// Repartition `conf` into `partitions` placeholders behind one morsel queue.
    fn repartition(conf: &FileScanConfig, partitions: usize) -> FileScanConfig {
        conf.file_source()
            .repartitioned(partitions, 0, None, conf)
            .unwrap()
            .expect("a morsel scan")
    }

    /// The nd plan above the scan of `conf`, as a format builds it.
    fn nd_exec(conf: FileScanConfig) -> Arc<dyn ExecutionPlan> {
        let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(conf);
        let decoded = Arc::new(NdSourceExec::try_new(scan).unwrap());
        Arc::new(NdBroadcastExec::try_new(decoded).unwrap())
    }

    /// Execute the scan of `conf` on `partitions` partitions.
    async fn execute(conf: &FileScanConfig, partitions: usize) -> Vec<RecordBatch> {
        let plan = nd_exec(repartition(conf, partitions));
        collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap()
    }

    /// Run the scan of `conf` on `partitions` partitions, and join its batches.
    async fn run(conf: FileScanConfig, partitions: usize) -> RecordBatch {
        let batches = execute(&conf, partitions).await;
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

    /// Execute the nd plan of `conf` as planned, with a session batch size of `batch_size`.
    async fn execute_planned(conf: FileScanConfig, batch_size: usize) -> Vec<RecordBatch> {
        let session =
            SessionContext::new_with_config(SessionConfig::new().with_batch_size(batch_size));
        collect(nd_exec(conf), session.task_ctx()).await.unwrap()
    }

    fn row_count(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
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

        let config = repartition(&config, 4);

        assert_eq!(config.file_groups.len(), 4);
        assert!(config.file_groups.iter().all(|group| group.len() == 1));
        let scan = scan_of(&config);
        assert_eq!(scan.local_files.len(), 2);
        assert_eq!(scan.shared_files.len(), 1);
        assert_eq!(scan.num_partitions, 4);
        // The first opener builds the queue.
        assert!(scan.queue.get().is_none());
    }

    #[test]
    fn a_single_partition_gets_a_scan_too() {
        let source = source(&grid_schema(), vec![]);
        let config = config(source.clone(), &[("grid_0.nc", 10)]).build();

        let config = repartition(&config, 1);

        assert_eq!(config.file_groups.len(), 1);
        assert_eq!(scan_of(&config).num_partitions, 1);
    }

    #[tokio::test]
    async fn an_empty_scan_gives_no_rows() {
        let config = config(source(&grid_schema(), vec![]), &[]).build();

        let batches = execute(&config, 4).await;

        assert_eq!(row_count(&batches), 0);
    }

    // ── no repartitioning ────────────────────────────────────────────

    #[tokio::test]
    async fn a_scan_without_repartitioning_reads_every_file() {
        let conf = config(source(&grid_schema(), vec![]), &grid_files()).build();

        let batches = execute_planned(conf, 8192).await;

        assert_eq!(row_count(&batches), 18);
    }

    #[tokio::test]
    async fn file_groups_without_repartitioning_share_one_queue() {
        let files = grid_files()
            .into_iter()
            .map(|(name, size)| FileGroup::new(vec![PartitionedFile::new(name, size)]))
            .collect();
        let conf = config(source(&grid_schema(), vec![]), &[])
            .with_file_groups(files)
            .build();

        let batches = execute_planned(conf, 8192).await;

        let batch = concat_batches(&batches[0].schema(), &batches).unwrap();
        let expected: Vec<Option<i32>> = (0..3)
            .flat_map(|i| (0..6).map(move |v| Some(10 * i + v)))
            .collect();
        assert_eq!(sorted_values(&batch, "temp"), expected);
    }

    // ── settings after repartitioning ────────────────────────────────

    #[tokio::test]
    async fn the_session_batch_size_cuts_the_morsels() {
        let conf = repartition(
            &config(source(&grid_schema(), vec![]), &grid_files()).build(),
            1,
        );

        let batches = execute_planned(conf, 2).await;

        // Without the session batch size, each 2x3 grid is one morsel of 6 rows.
        assert!(batches.iter().all(|batch| batch.num_rows() <= 2));
        assert_eq!(row_count(&batches), 18);
    }

    #[tokio::test]
    async fn a_projection_after_repartitioning_reaches_the_planner() {
        let conf = repartition(
            &config(source(&grid_schema(), vec![]), &grid_files()).build(),
            4,
        );
        let projection =
            ProjectionExprs::from_indices(&[1], conf.file_source().table_schema().table_schema());
        let projected = conf
            .file_source()
            .try_pushdown_projection(&projection)
            .unwrap()
            .expect("a column projection");
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(projected)
            .build();

        let batches = execute_planned(conf, 8192).await;

        let batch = concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(batch.num_columns(), 1);
        // The grid follows the projection: `lat` alone gives 3 rows per file.
        assert_eq!(batch.num_rows(), 9);
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

        assert!(source.repartitioned(4, 0, None, &config).is_err());
    }

    #[test]
    fn an_opener_of_a_partitioned_table_is_rejected() {
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

        let batches = execute(&conf, 4).await;

        assert_eq!(row_count(&batches), 18);
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
}
