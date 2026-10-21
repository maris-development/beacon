use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::morsel_planner::{Morsel, MorselPlanner};
use datafusion::{
    datasource::listing::PartitionedFile,
    error::{DataFusionError, Result},
    physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder},
};
use futures::prelude::future::BoxFuture;
use indexmap::IndexMap;
use object_store::ObjectStore;

use crate::{
    NdArrayD,
    array::subset::ArraySubset,
    dataset_v2::{
        Dataset,
        gridded::GriddedDataset,
        morsel::{GriddedChunkMorsel, RaggedCastsMorsel},
        pruning::{ChunkPruner, PruningPredicate},
        ragged::RaggedDataset,
    },
    nd_file_opener::NdFileOpener,
};
use std::{ops::Range, sync::Arc};

/// A file cut into at most this many chunks or cast ranges is not pruned.
pub const DEFAULT_PRUNING_CHUNK_THRESHOLD: usize = 4;

/// Plans the morsels of the ND files of one scan.
///
/// A gridded file gives one morsel per chunk of its grid. A ragged file gives
/// one morsel per range of casts. A pushdown predicate drops the morsels that
/// hold no wanted row, when the file has more than `pruning_chunk_threshold` of them.
#[derive(Debug)]
pub struct NdMorselPlanner {
    opener: Arc<dyn NdFileOpener>,
    table_schema: SchemaRef, // Table Schema with projection applied
    pushdown: Option<PruningPredicate>,
    pruning_chunk_threshold: usize,
    batch_size: usize,
    metrics: PruningMetrics,
}

impl MorselPlanner for NdMorselPlanner {
    fn plan(
        self: Arc<Self>,
        store: Arc<dyn ObjectStore>,
        file: PartitionedFile,
    ) -> BoxFuture<'static, Result<Vec<Box<dyn Morsel>>>> {
        let fut = async move {
            let location = file.object_meta.location.clone();
            let dataset = self
                .opener
                .open(Arc::clone(&store), file)
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!("Failed to open ND file {location}: {e}"))
                })?;

            // A file that holds none of the projected columns adds no rows.
            if !self.table_schema.fields().is_empty()
                && !self
                    .table_schema
                    .fields()
                    .iter()
                    .any(|field| dataset.arrays.contains_key(field.name()))
            {
                return Ok(Vec::new());
            }

            // The projection can drop an array that only the predicate names.
            let unprojected = self.pushdown.is_some().then(|| dataset.arrays.clone());

            // No file column (`COUNT(*)`, or only partition columns): the full grid gives the rows.
            let projection =
                (!self.table_schema.fields().is_empty()).then(|| Arc::clone(&self.table_schema));
            if is_ragged(&dataset) {
                let ragged = RaggedDataset::try_new(dataset, projection)
                    .await
                    .map_err(external)?;
                self.plan_ragged(ragged, unprojected.as_ref()).await
            } else {
                let gridded = GriddedDataset::try_new(dataset, projection).map_err(external)?;
                Ok(self.plan_gridded(gridded, unprojected.as_ref()).await)
            }
        };
        Box::pin(fut)
    }
}

impl NdMorselPlanner {
    /// Create the planner of one scan.
    ///
    /// # Arguments
    ///
    /// * `opener` - The opener that reads a file into a dataset
    /// * `table_schema` - The logical file schema that the scan projects, without partition columns
    /// * `pushdown` - The pruning conjuncts that drop the chunks without a wanted row
    /// * `batch_size` - The largest number of rows in a morsel of a file stored in one chunk
    pub fn new(
        opener: Arc<dyn NdFileOpener>,
        table_schema: SchemaRef,
        pushdown: Option<PruningPredicate>,
        batch_size: usize,
    ) -> Self {
        Self {
            opener,
            table_schema,
            pushdown,
            pruning_chunk_threshold: DEFAULT_PRUNING_CHUNK_THRESHOLD,
            batch_size,
            metrics: PruningMetrics::default(),
        }
    }

    /// The same planner, which prunes only a file of more than `threshold` chunks or cast ranges.
    pub fn with_pruning_chunk_threshold(mut self, threshold: usize) -> Self {
        self.pruning_chunk_threshold = threshold;
        self
    }

    /// The same planner, which counts its pruned chunks and rows in `metrics`.
    pub fn with_metrics(mut self, metrics: &ExecutionPlanMetricsSet) -> Self {
        self.metrics = PruningMetrics::new(metrics);
        self
    }

    /// Cut a gridded dataset into one morsel per chunk of its grid.
    async fn plan_gridded(
        &self,
        dataset: GriddedDataset,
        unprojected: Option<&IndexMap<String, Arc<dyn NdArrayD>>>,
    ) -> Vec<Box<dyn Morsel>> {
        let mut chunks = dataset.chunks(self.batch_size);
        if let Some(pruner) = self
            .pruner(unprojected, chunks.len(), &dataset.grid_dimensions)
            .await
        {
            self.prune(
                &mut chunks,
                |subset| pruner.keeps_chunk(&dataset.grid_dimensions, subset),
                ArraySubset::rows,
            );
        }

        let dataset = Arc::new(dataset);
        chunks
            .into_iter()
            .map(|subset| {
                let morsel = GriddedChunkMorsel::new(
                    Arc::clone(&dataset),
                    subset,
                    Arc::clone(&self.table_schema),
                );
                Box::new(morsel) as Box<dyn Morsel>
            })
            .collect()
    }

    /// Cut a ragged dataset into one morsel per range of casts.
    async fn plan_ragged(
        &self,
        dataset: RaggedDataset,
        unprojected: Option<&IndexMap<String, Arc<dyn NdArrayD>>>,
    ) -> Result<Vec<Box<dyn Morsel>>> {
        let mut ranges = dataset
            .cast_ranges(self.batch_size)
            .await
            .map_err(external)?;
        let offsets = Arc::new(dataset.row_offsets().await.map_err(external)?);

        let instance_dimension = std::slice::from_ref(&dataset.instance_dimension);
        if let Some(pruner) = self
            .pruner(unprojected, ranges.len(), instance_dimension)
            .await
        {
            // The rows of a range are the most observations one row-size array counts for it.
            let rows = |casts: &Range<usize>| {
                offsets
                    .values()
                    .map(|o| o[casts.end] - o[casts.start])
                    .max()
                    .unwrap_or(0)
            };
            self.prune(
                &mut ranges,
                |casts| pruner.keeps_casts(&dataset.instance_dimension, casts),
                rows,
            );
        }

        let dataset = Arc::new(dataset);
        Ok(ranges
            .into_iter()
            .map(|casts| {
                let morsel = RaggedCastsMorsel::new(
                    Arc::clone(&dataset),
                    casts,
                    Arc::clone(&offsets),
                    Arc::clone(&self.table_schema),
                );
                Box::new(morsel) as Box<dyn Morsel>
            })
            .collect())
    }

    /// The pruner of a file cut into `count` parts along `dimensions`.
    ///
    /// Returns `None` without a predicate, or when the file has too few parts to gain from it.
    async fn pruner(
        &self,
        unprojected: Option<&IndexMap<String, Arc<dyn NdArrayD>>>,
        count: usize,
        dimensions: &[String],
    ) -> Option<ChunkPruner> {
        let (Some(predicate), Some(arrays)) = (&self.pushdown, unprojected) else {
            return None;
        };
        if count <= self.pruning_chunk_threshold {
            return None;
        }
        Some(ChunkPruner::new(arrays, predicate, dimensions).await)
    }

    /// Drop the parts that `keep` rejects, and count them and their rows.
    fn prune<T>(&self, parts: &mut Vec<T>, keep: impl Fn(&T) -> bool, rows: impl Fn(&T) -> usize) {
        let count = parts.len();
        let mut pruned_rows = 0;
        parts.retain(|part| {
            let kept = keep(part);
            if !kept {
                pruned_rows += rows(part);
            }
            kept
        });
        self.metrics.chunks_pruned.add(count - parts.len());
        self.metrics.rows_pruned.add(pruned_rows);
    }
}

/// The pruning counters of a scan, shared by all its partitions.
#[derive(Debug, Clone)]
struct PruningMetrics {
    chunks_pruned: Count,
    rows_pruned: Count,
}

impl PruningMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            chunks_pruned: MetricBuilder::new(metrics).global_counter("chunks_pruned"),
            rows_pruned: MetricBuilder::new(metrics).global_counter("rows_pruned"),
        }
    }
}

impl Default for PruningMetrics {
    /// Counters that no metrics set shows.
    fn default() -> Self {
        Self {
            chunks_pruned: Count::new(),
            rows_pruned: Count::new(),
        }
    }
}

/// A `<var>.sample_dimension` attribute marks a ragged dataset.
fn is_ragged(dataset: &Dataset) -> bool {
    dataset
        .arrays
        .keys()
        .any(|name| name.ends_with(".sample_dimension"))
}

fn external(error: anyhow::Error) -> DataFusionError {
    DataFusionError::External(error.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NdArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::{
        logical_expr::Operator,
        physical_expr::expressions::{BinaryExpr, Column, Literal},
        physical_plan::PhysicalExpr,
        scalar::ScalarValue,
    };
    use futures::FutureExt;
    use object_store::memory::InMemory;
    use std::collections::HashSet;

    /// Opens a grid of 8 `lat` values, 0 to 7, with a `temp` array on the same dimension.
    #[derive(Debug)]
    struct GridOpener;

    impl NdFileOpener for GridOpener {
        fn open(
            &self,
            _store: Arc<dyn ObjectStore>,
            _file: PartitionedFile,
        ) -> BoxFuture<'static, anyhow::Result<Dataset>> {
            let array = |values: Vec<f64>| -> Arc<dyn NdArrayD> {
                Arc::new(
                    NdArray::try_new_from_vec_in_mem(values, vec![8], vec!["lat".into()], None)
                        .unwrap(),
                )
            };
            let arrays = IndexMap::from([
                ("lat".to_string(), array((0..8).map(f64::from).collect())),
                ("temp".to_string(), array(vec![0.0; 8])),
            ]);
            async move { Dataset::from_arrays("grid".to_string(), arrays, HashSet::new()) }.boxed()
        }
    }

    fn lat_above(value: f64) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("lat", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Float64(Some(value)))),
        ))
    }

    /// The morsel count of the grid, at one row per morsel.
    async fn morsels(
        columns: &[&str],
        predicate: Arc<dyn PhysicalExpr>,
        threshold: usize,
    ) -> usize {
        let schema = |names: &[&str]| {
            let fields: Vec<Field> = names
                .iter()
                .map(|name| Field::new(*name, DataType::Float64, true))
                .collect();
            Arc::new(Schema::new(fields))
        };
        let pruning = PruningPredicate::try_new(&predicate, schema(&["lat", "temp"]));
        let planner = NdMorselPlanner::new(Arc::new(GridOpener), schema(columns), pruning, 1)
            .with_pruning_chunk_threshold(threshold);
        Arc::new(planner)
            .plan(
                Arc::new(InMemory::new()),
                PartitionedFile::new("grid.nc", 0),
            )
            .await
            .unwrap()
            .len()
    }

    #[tokio::test]
    async fn a_file_above_the_threshold_drops_the_rejected_chunks() {
        assert_eq!(morsels(&["lat"], lat_above(5.5), 4).await, 2);
    }

    #[tokio::test]
    async fn a_file_at_the_threshold_keeps_every_chunk() {
        assert_eq!(morsels(&["lat"], lat_above(5.5), 8).await, 8);
    }

    #[tokio::test]
    async fn a_column_outside_the_projection_still_prunes() {
        assert_eq!(morsels(&["temp"], lat_above(5.5), 4).await, 2);
    }
}
