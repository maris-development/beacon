//! Cuts one NetCDF file into morsels, through the oxcdf reader.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::morsel_planner::{Morsel, MorselPlanner};
use beacon_nd_array::dataset_v2::{
    gridded::GriddedDataset,
    morsel::{GriddedChunkMorsel, RaggedCastsMorsel},
    ragged::RaggedDataset,
    Dataset,
};
use datafusion::{
    datasource::listing::PartitionedFile,
    error::{DataFusionError, Result},
};
use futures::future::BoxFuture;
use object_store::ObjectStore;

use crate::oxcdf_reader::open_dataset_v2;

/// Plans the morsels of the NetCDF files of one scan.
///
/// A gridded file gives one morsel per chunk of its grid. A ragged file gives
/// one morsel per range of casts.
#[derive(Debug)]
pub struct OxcdfMorselPlanner {
    store: Arc<dyn ObjectStore>,
    columns: Arc<[String]>, // The file columns that the scan projects, without partition columns
    batch_size: usize,
}

impl OxcdfMorselPlanner {
    /// Create the planner of one scan.
    ///
    /// # Arguments
    ///
    /// * `store` - The store that holds the files
    /// * `projected_schema` - The schema that the scan projects, partition columns included
    /// * `partition_columns` - The names of the `PARTITIONED BY` columns of the table
    /// * `batch_size` - The largest number of rows in a morsel of a file stored in one chunk
    pub fn new(
        store: Arc<dyn ObjectStore>,
        projected_schema: &SchemaRef,
        partition_columns: &[String],
        batch_size: usize,
    ) -> Self {
        let columns = projected_schema
            .fields()
            .iter()
            .map(|field| field.name())
            .filter(|name| !partition_columns.contains(name))
            .cloned()
            .collect();
        Self {
            store,
            columns,
            batch_size,
        }
    }

    /// Cut a ragged file into one morsel per range of casts.
    async fn plan_ragged(&self, dataset: Dataset) -> Result<Vec<Box<dyn Morsel>>> {
        let ragged = Arc::new(RaggedDataset::try_new(dataset, None).await.map_err(external)?);
        let ranges = ragged
            .cast_ranges(self.batch_size)
            .await
            .map_err(external)?;
        Ok(ranges
            .into_iter()
            .map(|casts| {
                let morsel =
                    RaggedCastsMorsel::new(Arc::clone(&ragged), casts, Arc::clone(&self.columns));
                Box::new(morsel) as Box<dyn Morsel>
            })
            .collect())
    }

    /// Cut a gridded file into one morsel per chunk of its grid.
    fn plan_gridded(&self, dataset: Dataset) -> Result<Vec<Box<dyn Morsel>>> {
        // No file column (`COUNT(*)`, or only partition columns): the full grid gives the rows.
        let projection = (!self.columns.is_empty()).then(|| Arc::clone(&self.columns));
        let gridded = Arc::new(GriddedDataset::try_new(dataset, projection).map_err(external)?);
        Ok(gridded
            .chunks(self.batch_size)
            .into_iter()
            .map(|subset| {
                Box::new(GriddedChunkMorsel::new(Arc::clone(&gridded), subset)) as Box<dyn Morsel>
            })
            .collect())
    }
}

impl MorselPlanner for OxcdfMorselPlanner {
    fn plan(
        self: Arc<Self>,
        file: PartitionedFile,
    ) -> BoxFuture<'static, Result<Vec<Box<dyn Morsel>>>> {
        Box::pin(async move {
            let location = file.object_meta.location;
            let dataset = open_dataset_v2(Arc::clone(&self.store), location.clone())
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to open NetCDF file {location}: {e}"
                    ))
                })?;

            // A file that holds none of the projected columns adds no rows.
            if !self.columns.is_empty()
                && !self
                    .columns
                    .iter()
                    .any(|column| dataset.arrays.contains_key(column))
            {
                return Ok(Vec::new());
            }

            if is_ragged(&dataset) {
                self.plan_ragged(dataset).await
            } else {
                self.plan_gridded(dataset)
            }
        })
    }
}

/// A `<var>.sample_dimension` attribute marks a ragged file.
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
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::local::LocalFileSystem;

    use super::*;

    const GRIDDED_FILE: &str = "gridded-example.nc";
    const RAGGED_FILE: &str = "wod_ctd_1964.nc";

    fn test_store() -> Arc<dyn ObjectStore> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_files");
        Arc::new(LocalFileSystem::new_with_prefix(root).expect("test_files store"))
    }

    fn schema(columns: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            columns
                .iter()
                .map(|name| Field::new(*name, DataType::Float64, true))
                .collect::<Vec<_>>(),
        ))
    }

    fn planner(columns: &[&str], batch_size: usize) -> Arc<OxcdfMorselPlanner> {
        Arc::new(OxcdfMorselPlanner::new(
            test_store(),
            &schema(columns),
            &["year".to_string()],
            batch_size,
        ))
    }

    async fn plan(planner: Arc<OxcdfMorselPlanner>, file: &str) -> Vec<Box<dyn Morsel>> {
        let size = std::fs::metadata(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("test_files")
                .join(file),
        )
        .expect("fixture")
        .len();
        planner
            .plan(PartitionedFile::new(file, size))
            .await
            .expect("plan")
    }

    #[tokio::test]
    async fn a_gridded_file_gives_one_morsel_per_native_chunk() {
        let dataset = open_dataset_v2(test_store(), GRIDDED_FILE.into())
            .await
            .unwrap();
        let projection: Arc<[String]> = vec!["analysed_sst".to_string()].into();
        let expected = GriddedDataset::try_new(dataset, Some(projection))
            .unwrap()
            .chunks(usize::MAX)
            .len();

        let morsels = plan(planner(&["analysed_sst"], usize::MAX), GRIDDED_FILE).await;

        assert_eq!(morsels.len(), expected);
        assert!(
            morsels.len() > 1,
            "analysed_sst is stored in more than one chunk"
        );
    }

    #[tokio::test]
    async fn a_ragged_file_gives_one_morsel_per_cast_range() {
        let whole = plan(planner(&["Temperature"], usize::MAX), RAGGED_FILE).await;
        let cut = plan(planner(&["Temperature"], 100), RAGGED_FILE).await;

        assert_eq!(whole.len(), 1);
        assert!(cut.len() > 1);
        assert!(format!("{:?}", cut[0]).starts_with("RaggedCastsMorsel"));
    }

    #[tokio::test]
    async fn a_file_without_a_projected_column_gives_no_morsels() {
        let morsels = plan(planner(&["not_in_the_file"], usize::MAX), GRIDDED_FILE).await;

        assert!(morsels.is_empty());
    }

    #[tokio::test]
    async fn count_star_plans_the_full_grid() {
        let morsels = plan(planner(&["year"], usize::MAX), GRIDDED_FILE).await;

        assert!(!morsels.is_empty());
    }
}
