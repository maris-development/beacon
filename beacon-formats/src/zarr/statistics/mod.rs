use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::{HashMap, exec_datafusion_err};
use zarrs::group::Group;
use zarrs_storage::AsyncReadableListableStorageTraits;

use beacon_arrow_zarr::reader::AsyncArrowZarrGroupReader;
use datafusion::common::{ColumnStatistics, Statistics};

use crate::zarr::{
    expr_util::ZarrFilterRange,
    statistics::{array::ZarrArrayStatistics, pushdown::ArraySlicePushDownResult},
};

pub mod array;
pub mod pushdown;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ZarrStatisticsSelection {
    pub columns: Vec<String>,
}

impl ZarrStatisticsSelection {
    pub fn has_array(&self, array_name: &str) -> bool {
        self.columns.contains(&array_name.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct ZarrStatistics {
    table_schema: SchemaRef,
    arrays: HashMap<String, ZarrArrayStatistics>,
}

impl ZarrStatistics {
    pub async fn create(
        table_schema: &SchemaRef,
        statistics_selection: &ZarrStatisticsSelection,
        zarr_group: Arc<Group<dyn AsyncReadableListableStorageTraits>>,
    ) -> datafusion::error::Result<Self, datafusion::error::DataFusionError> {
        let zarr_reader = AsyncArrowZarrGroupReader::new(zarr_group.clone())
            .await
            .map_err(|e| exec_datafusion_err!("{}", e))?;

        let mut arrays = HashMap::new();

        for field in table_schema.fields() {
            // Check if in pushdown statistics
            if statistics_selection.has_array(field.name()) {
                // Compute statistics
                if let Ok(Some(array)) = zarr_reader.read_array_full(field.name()).await {
                    arrays.insert(field.name().clone(), ZarrArrayStatistics::new(array));
                }
            }
        }

        Ok(ZarrStatistics {
            arrays,
            table_schema: table_schema.clone(),
        })
    }

    pub fn statistics_columns(&self) -> Vec<String> {
        self.arrays.keys().cloned().collect()
    }

    pub fn get_slice_pushdown(
        &self,
        array_name: &str,
        range: ZarrFilterRange,
    ) -> Option<ArraySlicePushDownResult> {
        self.arrays
            .get(array_name)
            .map(|array_stats| array_stats.get_slice_pushdown(range))
    }

    pub fn get_statistics(&self) -> Statistics {
        let mut statistics = Statistics::default();

        for field in self.table_schema.fields() {
            if let Some(array_stats) = self.arrays.get(field.name()) {
                let col_stats = array_stats.column_statistics();
                statistics = statistics.add_column_statistics(col_stats);
            } else {
                statistics = statistics.add_column_statistics(ColumnStatistics::new_unknown());
            }
        }
        statistics
    }
}
