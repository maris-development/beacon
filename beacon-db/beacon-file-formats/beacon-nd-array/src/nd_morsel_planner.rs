use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::morsel_planner::{Morsel, MorselPlanner};
use datafusion::{
    datasource::listing::PartitionedFile,
    error::{DataFusionError, Result},
    physical_plan::PhysicalExpr,
};
use futures::prelude::future::BoxFuture;
use object_store::ObjectStore;

use crate::{
    dataset_v2::{
        Dataset,
        gridded::GriddedDataset,
        morsel::{GriddedChunkMorsel, RaggedCastsMorsel},
        ragged::RaggedDataset,
    },
    nd_file_opener::NdFileOpener,
};
use std::sync::Arc;

/// Plans the morsels of the ND files of one scan.
///
/// A gridded file gives one morsel per chunk of its grid. A ragged file gives
/// one morsel per range of casts.
#[derive(Debug)]
pub struct NdMorselPlanner {
    opener: Arc<dyn NdFileOpener>,
    table_schema: SchemaRef, // Table Schema with projection applied
    _pushdown_predicate: Option<Arc<dyn PhysicalExpr>>, // To implement later to filter out chunks during planning
    batch_size: usize,
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

            // No file column (`COUNT(*)`, or only partition columns): the full grid gives the rows.
            let projection =
                (!self.table_schema.fields().is_empty()).then(|| Arc::clone(&self.table_schema));
            if is_ragged(&dataset) {
                let ragged = RaggedDataset::try_new(dataset, projection)
                    .await
                    .map_err(external)?;
                self.plan_ragged(ragged).await
            } else {
                let gridded = GriddedDataset::try_new(dataset, projection).map_err(external)?;
                Ok(self.plan_gridded(gridded))
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
    /// * `pushdown_predicate` - An optional predicate to filter out chunks during planning
    /// * `batch_size` - The largest number of rows in a morsel of a file stored in one chunk
    pub fn new(
        opener: Arc<dyn NdFileOpener>,
        table_schema: SchemaRef,
        pushdown_predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
    ) -> Self {
        Self {
            opener,
            table_schema,
            _pushdown_predicate: pushdown_predicate,
            batch_size,
        }
    }

    /// Cut a gridded dataset into one morsel per chunk of its grid.
    fn plan_gridded(&self, dataset: GriddedDataset) -> Vec<Box<dyn Morsel>> {
        let dataset = Arc::new(dataset);
        dataset
            .chunks(self.batch_size)
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
    async fn plan_ragged(&self, dataset: RaggedDataset) -> Result<Vec<Box<dyn Morsel>>> {
        let dataset = Arc::new(dataset);

        let ranges = match dataset.cast_ranges(self.batch_size).await {
            Ok(r) => r,
            Err(e) => {
                return Err(external(e));
            }
        };
        let offsets = match dataset.row_offsets().await {
            Ok(r) => Arc::new(r),
            Err(e) => {
                return Err(external(e));
            }
        };
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
