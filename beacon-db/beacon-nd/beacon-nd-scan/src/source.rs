//! The DataFusion [`FileSource`] of the ND scan.

use std::sync::Arc;

use datafusion::common::not_impl_err;
use datafusion::config::ConfigOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileOpener, FileScanConfig, FileSource};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::Result;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, conjunction};
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::morsel::Morselizer;
use object_store::ObjectStore;

use crate::metrics::ScanMetrics;
use crate::opener::{NdFileOpener, NdScanOptions};
use crate::planner::{NdMorselizer, ScanContext};
use crate::slices::{slice_files, spread_files};

/// The DataFusion [`FileSource`] of each ND format.
///
/// It reads on the morsel API: one morsel per chunk of a file.
///
/// With [`FileScanConfig::limit`], the planner of each partition plans no more
/// reads after its chunks hold the rows of the limit. DataFusion sets that limit
/// only when no filter stays above the scan. The scan answers `PushedDown::No`
/// for each filter, so a `FilterExec` stays above it and stops a limit. A caller
/// who sets the limit by hand must make sure of the same.
///
/// `FileStream` also applies the limit to the encoded rows. One encoded row is
/// one chunk, so that second limit is looser and never cuts rows that the query needs.
#[derive(Debug, Clone)]
pub struct NdSource {
    opener: Arc<dyn NdFileOpener>,
    options: NdScanOptions,
    table_schema: TableSchema,
    metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// The projection the scan pushed down, on top of the table schema.
    projection: Option<ProjectionExprs>,
}

impl NdSource {
    /// A source that opens each file with `opener`.
    ///
    /// `table_schema` holds the encoded file schema and the encoded partition columns.
    pub fn new(
        opener: Arc<dyn NdFileOpener>,
        options: NdScanOptions,
        table_schema: TableSchema,
    ) -> Self {
        Self {
            opener,
            options,
            table_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            batch_size: usize::MAX,
            predicate: None,
            projection: None,
        }
    }

    /// The same source with `projection`. The format keeps a pushed projection this way.
    pub fn with_projection(mut self, projection: Option<ProjectionExprs>) -> Self {
        self.projection = projection;
        self
    }
}

impl FileSource for NdSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        not_impl_err!("ND scans use create_morselizer")
    }

    fn create_morselizer(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Box<dyn Morselizer>> {
        Ok(Box::new(NdMorselizer::new(ScanContext {
            opener: Arc::clone(&self.opener),
            options: self.options.clone(),
            store: object_store,
            projected_schema: base_config.projected_schema()?,
            partition_fields: self.table_schema.table_partition_cols().clone(),
            batch_size: self.batch_size,
            predicate: self.predicate.clone(),
            limit: base_config.limit,
            metrics: ScanMetrics::new(&self.metrics, partition),
        })))
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

    /// Spread the files over `target_partitions` groups, and slice a file when
    /// there are fewer files than partitions. A file is never cut by byte range.
    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        if output_ordering.is_some() || target_partitions <= 1 {
            return Ok(None);
        }
        let entries: Vec<PartitionedFile> = config
            .file_groups
            .iter()
            .flat_map(|group| group.files().iter().cloned())
            .collect();

        // A scan with a row limit stops early, so slices only add opens.
        let entries = if config.limit.is_some() {
            entries
        } else {
            slice_files(entries, target_partitions)
        };
        let mut config = config.clone();
        config.file_groups = spread_files(entries, target_partitions);
        Ok(Some(config))
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        self.opener.format_name()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.projection.as_ref()
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
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
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let predicate = match self.predicate.clone() {
            Some(existing) => conjunction(std::iter::once(existing).chain(filters.clone())),
            None => conjunction(filters.clone()),
        };
        let source = Self {
            predicate: Some(predicate),
            ..self.clone()
        };
        // The scan only prunes chunks, so the FilterExec above it stays.
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(vec![
            PushedDown::No;
            filters.len()
        ])
        .with_updated_node(Arc::new(source)))
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, FileSource};
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::error::DataFusionError;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::filter_pushdown::PushedDown;
    use object_store::memory::InMemory;

    use super::NdSource;
    use crate::opener::NdScanOptions;
    use crate::slices::NdSlice;
    use crate::testing::MemoryOpener;

    /// A source of the test opener on a table with the Float64 column `name`.
    fn source(name: &str) -> NdSource {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Float64, true)]));
        NdSource::new(
            Arc::new(MemoryOpener::default()),
            NdScanOptions::default(),
            TableSchema::from_file_schema(schema),
        )
    }

    /// A scan of `source` with one group for each of `files` files of 1000 bytes.
    fn config_of(source: &NdSource, files: usize) -> FileScanConfigBuilder {
        let groups = (0..files)
            .map(|i| FileGroup::new(vec![PartitionedFile::new(format!("f{i}.nc"), 1000)]))
            .collect();
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source.clone()))
            .with_file_groups(groups)
    }

    /// A scan gets one group per partition. A lone file is cut into slices, and
    /// no entry is a byte range.
    #[test]
    fn a_scan_spreads_whole_files_and_slices_a_lone_file() {
        let source = source("V0");

        // Twelve files on four partitions: whole files, three in each group.
        let many = source
            .repartitioned(4, 0, None, &config_of(&source, 12).build())
            .unwrap()
            .expect("the scan repartitions");
        assert_eq!(many.file_groups.len(), 4);
        assert!(
            many.file_groups
                .iter()
                .all(|group| group.files().len() == 3)
        );

        // One file on four partitions: four slices of it.
        let one = source
            .repartitioned(4, 0, None, &config_of(&source, 1).build())
            .unwrap()
            .expect("the scan repartitions");
        assert_eq!(one.file_groups.len(), 4);
        for group in &one.file_groups {
            assert_eq!(group.files().len(), 1);
            assert!(group.files()[0].range.is_none());
            assert_eq!(group.files()[0].object_meta.location.as_ref(), "f0.nc");
        }

        // One partition: the scan stays as it is planned.
        assert!(
            source
                .repartitioned(1, 0, None, &config_of(&source, 3).build())
                .unwrap()
                .is_none()
        );
    }

    /// An ordered scan stays as it is. A partition with a part of a file cannot
    /// emit its rows in file order.
    #[test]
    fn an_ordered_scan_is_not_shared() {
        let schema = Arc::new(Schema::new(vec![Field::new("time", DataType::Int64, true)]));
        let source = source("time");
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_file(PartitionedFile::new("large.nc", 64 * 1024 * 1024))
        .build();

        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col("time", &schema).unwrap(),
        )])
        .unwrap();

        assert!(
            source
                .repartitioned(4, 0, Some(ordering), &config)
                .unwrap()
                .is_none(),
            "an ordered scan must keep its single group"
        );
    }

    #[test]
    fn a_scan_with_a_limit_is_not_sliced() {
        let source = source("V0");
        let config = config_of(&source, 1).with_limit(Some(5)).build();

        let planned = source
            .repartitioned(4, 0, None, &config)
            .unwrap()
            .expect("the scan repartitions");

        assert_eq!(planned.file_groups.len(), 4);
        let entries: Vec<&PartitionedFile> = planned
            .file_groups
            .iter()
            .flat_map(|group| group.files())
            .collect();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].extension::<NdSlice>().is_none());
    }

    #[test]
    fn the_file_type_is_the_name_of_the_format() {
        assert_eq!(source("V0").file_type(), "memory");
    }

    #[test]
    fn a_filter_is_kept_and_not_claimed() {
        let schema = Schema::new(vec![Field::new("lat", DataType::Float64, true)]);
        let filter = binary(
            col("lat", &schema).unwrap(),
            Operator::Gt,
            lit(3.5),
            &schema,
        )
        .unwrap();

        let result = source("lat")
            .try_pushdown_filters(vec![filter], &ConfigOptions::default())
            .unwrap();

        assert_eq!(result.filters.len(), 1);
        assert!(
            result
                .filters
                .iter()
                .all(|pushed| matches!(pushed, PushedDown::No))
        );
        let node: Arc<dyn Any + Send + Sync> = result.updated_node.expect("a new source");
        let updated = node.downcast::<NdSource>().expect("an ND source");
        assert!(updated.predicate.is_some());
    }

    #[test]
    fn the_file_opener_path_is_not_implemented() {
        let source = source("V0");
        let config = config_of(&source, 1).build();

        let result = source.create_file_opener(Arc::new(InMemory::new()), &config, 0);

        assert!(
            matches!(result, Err(DataFusionError::NotImplemented(_))),
            "{:?}",
            result.err()
        );
    }
}
