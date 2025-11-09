use std::{collections::HashMap, ops::Range, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, AsArray, PrimitiveArray, Scalar},
    compute::CastOptions,
    datatypes::{
        DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, SchemaRef,
        TimeUnit, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
    util::display::FormatOptions,
};
use beacon_arrow_zarr::{
    array_slice_pushdown::ArraySlicePushDown, reader::AsyncArrowZarrGroupReader,
};
use datafusion::{
    common::{ColumnStatistics, Statistics, stats::Precision},
    scalar::ScalarValue,
};
use nd_arrow_array::NdArrowArray;
use zarrs::group::Group;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::zarr::expr_util::ZarrFilterRange;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ZarrPushDownStatistics {
    pub arrays: Vec<String>,
}

impl ZarrPushDownStatistics {
    pub fn has_array(&self, array_name: &str) -> bool {
        self.arrays.contains(&array_name.to_string())
    }
}

pub async fn generate_zarr_statistics_from_zarr_group(
    table_schema: &SchemaRef,
    pushdown: &ZarrPushDownStatistics,
    zarr_group: Arc<Group<dyn AsyncReadableListableStorageTraits>>,
) -> Option<ZarrStatistics> {
    let zarr_reader = AsyncArrowZarrGroupReader::new(zarr_group.clone())
        .await
        .ok()?;

    let mut arrays = HashMap::new();

    for field in table_schema.fields() {
        // Check if in pushdown statistics
        if pushdown.has_array(field.name()) {
            // Compute statistics
            if let Ok(Some(array)) = zarr_reader.read_array_full(field.name()).await {
                let column_statistics = compute_statistics_for_array(&array);
                arrays.insert(
                    field.name().clone(),
                    ZarrArrayStatistics {
                        array_ref: array,
                        column_statistics: column_statistics
                            .unwrap_or_else(|| ColumnStatistics::new_unknown()),
                    },
                );
            }
        }
    }

    let zarr_stats = ZarrStatistics {
        arrays,
        table_schema: table_schema.clone(),
    };

    Some(zarr_stats)
}

#[derive(Debug, Clone)]
pub struct ZarrStatistics {
    table_schema: SchemaRef,
    arrays: HashMap<String, ZarrArrayStatistics>,
}

impl ZarrStatistics {
    pub fn as_extensions(&self) -> Arc<dyn std::any::Any + Send + Sync> {
        Arc::new(self.clone())
    }

    pub fn statistics_columns(&self) -> Vec<String> {
        self.arrays.keys().cloned().collect()
    }

    pub fn statistics(&self) -> Statistics {
        let mut stats = Statistics::default();

        for field in self.table_schema.fields() {
            if let Some(array_stats) = self.arrays.get(field.name()) {
                stats = stats.add_column_statistics(array_stats.column_statistics().clone());
            } else {
                stats = stats.add_column_statistics(ColumnStatistics::new_unknown());
            }
        }

        stats
    }

    pub fn zarr_array_statistics(&self, array_name: &str) -> Option<&ZarrArrayStatistics> {
        self.arrays.get(array_name)
    }

    pub fn dimension_slice_pushdown(
        &self,
        filters: HashMap<String, ZarrFilterRange>,
    ) -> HashMap<String, ArraySlicePushDownResult> {
        let mut pushdowns = HashMap::new();
        for (array_name, stats) in &self.arrays {
            if let Some(filter) = filters.get(array_name)
                && let Some(slice_pushdown) = stats.get_slice_pushdown(filter.clone())
            {
                // Check if already exists or is new
                pushdowns
                    .entry(array_name.clone())
                    .and_modify(|existing_pushdown: &mut ArraySlicePushDownResult| {
                        // Coalesce the pushdown
                        existing_pushdown.start = slice_pushdown.start.max(existing_pushdown.start);
                        existing_pushdown.end = slice_pushdown.end.min(existing_pushdown.end);
                    })
                    .or_insert(slice_pushdown);
            }
        }

        pushdowns
    }
}

#[derive(Debug, Clone)]
pub enum ArraySlicePushDownResult {
    Prune,
    PushDown(ArraySlicePushDown),
    Retain,
}
