//! DataFusion [`FileSource`] for NetCDF files, read by oxcdf and scheduled as morsels.

use std::{
    fmt,
    sync::{Arc, OnceLock},
};

use beacon_datafusion_ext::{morsel_planner::MorselPlannerQueue, nd::logical_schema};
use datafusion::{
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileGroup, FileOpener, FileScanConfig, FileSource},
        table_schema::TableSchema,
    },
    physical_expr::{conjunction, projection::ProjectionExprs, LexOrdering, PhysicalExpr},
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};

use super::{opener::OxcdfOpener, planner::OxcdfMorselPlanner};

/// Files below this size give morsels that stay with the partition that plans them.
const LOCAL_FILE_MAX_SIZE: u64 = 1 << 20;

/// DataFusion [`FileSource`] for NetCDF (`.nc`) files, read by oxcdf.
///
/// Each file is cut into morsels: one per chunk of a gridded file, one per range
/// of casts of a ragged file. A [`MorselPlannerQueue`] gives them to the partitions.
/// Files below `local_file_max_size` keep their morsels in the partition that
/// plans them. Other partitions can steal the morsels of larger files.
#[derive(Debug, Clone)]
pub struct OxcdfSource {
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    projection: Option<ProjectionExprs>,
    local_file_max_size: u64,
    scan: Option<Arc<MorselScan>>, // Set by `repartitioned`: the files behind the placeholder groups
}

impl OxcdfSource {
    /// Create the source of a scan over `table_schema`.
    ///
    /// `table_schema` is nd-encoded, as `NdSourceExec` above the scan expects.
    pub fn new(table_schema: TableSchema) -> Self {
        Self {
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            batch_size: usize::MAX,
            predicate: None,
            projection: None,
            local_file_max_size: LOCAL_FILE_MAX_SIZE,
            scan: None,
        }
    }

    /// The same source, with another size limit for files whose morsels stay local.
    pub fn with_local_file_max_size(mut self, size: u64) -> Self {
        self.local_file_max_size = size;
        self
    }

    /// The same source, with the given projection.
    pub fn with_projection(mut self, projection: Option<ProjectionExprs>) -> Self {
        self.projection = projection;
        self
    }
}

impl FileSource for OxcdfSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let projected_schema = base_config.projected_schema()?;
        let partition_columns: Vec<String> = base_config
            .table_partition_cols()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let planner = Arc::new(OxcdfMorselPlanner::new(
            object_store,
            &logical_schema(&projected_schema)?,
            &partition_columns,
            self.batch_size,
        ));
        // TODO: keep the queue in a OnceLock that `reset_state` replaces; a re-run panics.
        let queue = self.scan.as_ref().map(|scan| scan.queue(&planner));

        Ok(Arc::new(OxcdfOpener::new(
            projected_schema,
            planner,
            queue,
            partition,
            self.local_file_max_size,
        )))
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

    /// Put every file of the scan behind one queue, and give each partition a placeholder.
    ///
    /// `repartition_file_min_size` is unused: the queue balances on completion, not on size.
    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        if output_ordering.is_some() || target_partitions <= 1 {
            return Ok(None);
        }
        let files: Vec<PartitionedFile> = config
            .file_groups
            .iter()
            .flat_map(FileGroup::iter)
            .cloned()
            .collect();
        if files.is_empty() {
            return Ok(None);
        }

        let total: u64 = files.iter().map(PartitionedFile::effective_size).sum();
        let placeholder =
            PartitionedFile::new(format!("oxcdf-morsel-scan/{}-files", files.len()), total);
        let (local_files, shared_files) = split_by_size(files, self.local_file_max_size);

        let mut config = config.clone();
        config.file_groups = (0..target_partitions)
            .map(|_| FileGroup::new(vec![placeholder.clone()]))
            .collect();
        // The openers are built from the config's source, so the scan travels with it.
        config.file_source = Arc::new(Self {
            scan: Some(Arc::new(MorselScan {
                local_files,
                shared_files,
                num_partitions: target_partitions,
                queue: OnceLock::new(),
            })),
            ..self.clone()
        });
        Ok(Some(config))
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn file_type(&self) -> &str {
        "nc"
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.projection.as_ref()
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> datafusion::error::Result<Option<Arc<dyn FileSource>>> {
        let merged = match &self.projection {
            Some(existing) => existing.try_merge(projection)?,
            None => projection.clone(),
        };
        Ok(Some(Arc::new(Self {
            projection: Some(merged),
            ..self.clone()
        })))
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        // The predicate is kept for chunk pruning later; the filter above the scan stays.
        let predicate = match self.predicate.clone() {
            Some(existing) => conjunction(std::iter::once(existing).chain(filters.clone())),
            None => conjunction(filters.clone()),
        };
        let source = Self {
            predicate: Some(predicate),
            ..self.clone()
        };
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(vec![
            PushedDown::No;
            filters.len()
        ])
        .with_updated_node(Arc::new(source)))
    }
}

/// The files of a morsel scan, and the queue that gives their morsels to the partitions.
struct MorselScan {
    local_files: Vec<PartitionedFile>,
    shared_files: Vec<PartitionedFile>,
    num_partitions: usize,
    queue: OnceLock<MorselPlannerQueue>, // Built by the first opener, which has the object store
}

impl MorselScan {
    /// The queue of the scan, built on the first call with `planner`.
    fn queue(&self, planner: &Arc<OxcdfMorselPlanner>) -> MorselPlannerQueue {
        self.queue
            .get_or_init(|| {
                MorselPlannerQueue::new(
                    self.local_files.clone(),
                    self.shared_files.clone(),
                    Arc::clone(planner) as _,
                    self.num_partitions,
                )
            })
            .clone()
    }
}

impl fmt::Debug for MorselScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MorselScan")
            .field("local_files", &self.local_files.len())
            .field("shared_files", &self.shared_files.len())
            .field("num_partitions", &self.num_partitions)
            .finish()
    }
}

/// Split `files` into the files below `max_size`, and the rest.
pub(super) fn split_by_size(
    files: impl IntoIterator<Item = PartitionedFile>,
    max_size: u64,
) -> (Vec<PartitionedFile>, Vec<PartitionedFile>) {
    files
        .into_iter()
        .partition(|file| file.object_meta.size < max_size)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::physical_plan::FileScanConfigBuilder;
    use datafusion::execution::object_store::ObjectStoreUrl;

    use super::*;

    fn source() -> OxcdfSource {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        OxcdfSource::new(TableSchema::new(schema, vec![]))
    }

    fn config(source: OxcdfSource, sizes: &[u64]) -> FileScanConfig {
        let files = sizes
            .iter()
            .enumerate()
            .map(|(i, &size)| PartitionedFile::new(format!("file_{i}.nc"), size))
            .collect();
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
            .with_file_group(FileGroup::new(files))
            .build()
    }

    fn scan_of(config: &FileScanConfig) -> Arc<MorselScan> {
        let source = config
            .file_source
            .downcast_ref::<OxcdfSource>()
            .expect("an OxcdfSource");
        Arc::clone(source.scan.as_ref().expect("a morsel scan"))
    }

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
        let source = source();
        let config = config(source.clone(), &[10, 2 << 20, 20]);

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
        let source = source();
        let config = config(source.clone(), &[10]);

        assert!(source.repartitioned(1, 0, None, &config).unwrap().is_none());
    }

    #[test]
    fn repartitioned_keeps_an_empty_scan_as_planned() {
        let source = source();
        let config = config(source.clone(), &[]);

        assert!(source.repartitioned(4, 0, None, &config).unwrap().is_none());
    }
}
