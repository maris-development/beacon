//! DataFusion column statistics derived from NetCDF datasets.
//!
//! [`generate_statistics`] is the entry point.  It opens a NetCDF file and
//! computes per-column min/max statistics for the columns listed in
//! `table_schema`, dispatching to a regular-dataset or ragged-dataset path.
//!
//! Statistics are used by DataFusion's query optimizer for predicate pushdown
//! and partition pruning.  Arrays for which a range cannot be determined
//! (multi-dimensional, String, Binary) get [`ColumnStatistics::new_unknown`].

use std::path::PathBuf;

use beacon_nd_array::{
    arrow::compute::value_range,
    dataset::{ragged::RaggedArray, AnyDataset, Dataset, RaggedDataset},
    NdArrayD,
};
use datafusion::{
    common::{stats::Precision, ColumnStatistics, Statistics},
    scalar::ScalarValue,
};

use crate::reader::open_dataset;

// ─── Public entry point ─────────────────────────────────────────────────────

/// Open the NetCDF file at `object` and return DataFusion [`Statistics`] for
/// the columns in `table_schema`.
pub async fn generate_statistics(
    datasets_root: PathBuf,
    object: &object_store::ObjectMeta,
    table_schema: &arrow::datatypes::Schema,
) -> anyhow::Result<Statistics> {
    let netcdf_path = beacon_object_storage::local_object_path(&datasets_root, &object.location)?;
    let dataset = open_dataset(netcdf_path).await?;

    match dataset {
        AnyDataset::Regular(dataset) => {
            generate_statistics_regular_dataset(&dataset, table_schema).await
        }
        AnyDataset::Ragged { ragged, .. } => {
            generate_statistics_ragged_dataset(&ragged, table_schema).await
        }
    }
}

// ─── Per-dataset-type helpers ───────────────────────────────────────────────

/// Compute statistics for a regular (non-ragged) dataset.
///
/// Only 0- and 1-dimensional arrays have their range computed; higher-rank
/// arrays (e.g. gridded data variables) get [`ColumnStatistics::new_unknown`]
/// because scanning them eagerly would be too expensive.
async fn generate_statistics_regular_dataset(
    dataset: &Dataset,
    schema: &arrow::datatypes::Schema,
) -> anyhow::Result<Statistics> {
    let mut stats = Statistics::default();

    for field in schema.fields() {
        let col_stats = match dataset.get_array(field.name()) {
            // 0-d and 1-d arrays are cheap to scan (attributes, coordinate vars).
            Some(array) if array.dimensions().len() <= 1 => {
                column_stats_for_array(array.as_ref()).await?
            }
            _ => ColumnStatistics::new_unknown(),
        };
        stats = stats.add_column_statistics(col_stats);
    }

    Ok(stats)
}

/// Compute statistics for a CF contiguous ragged-array dataset.
///
/// - **Attributes** and **instance variables** are small enough to scan fully.
/// - **Observation variables** (the ragged rows) are skipped — scanning the
///   full flattened array is potentially very large and is not worth the I/O.
async fn generate_statistics_ragged_dataset(
    ragged_dataset: &RaggedDataset,
    schema: &arrow::datatypes::Schema,
) -> anyhow::Result<Statistics> {
    let mut stats = Statistics::default();

    for field in schema.fields() {
        let col_stats = match ragged_dataset.get_ragged_array(field.name()) {
            Some(RaggedArray::Attribute(arr) | RaggedArray::InstanceVariable(arr)) => {
                column_stats_for_array(arr.as_ref()).await?
            }
            _ => ColumnStatistics::new_unknown(),
        };
        stats = stats.add_column_statistics(col_stats);
    }

    Ok(stats)
}

// ─── Shared primitive ───────────────────────────────────────────────────────

/// Return exact min/max [`ColumnStatistics`] for `array`, or unknown if the
/// array type has no orderable range (String, Binary, empty, all-fill).
async fn column_stats_for_array(array: &dyn NdArrayD) -> anyhow::Result<ColumnStatistics> {
    let Some((min_arr, max_arr)) = value_range(array).await else {
        return Ok(ColumnStatistics::new_unknown());
    };
    let min = ScalarValue::try_from_array(&min_arr, 0)
        .map_err(|e| anyhow::anyhow!("Failed to parse min value to scalar: {}", e))?;
    let max = ScalarValue::try_from_array(&max_arr, 0)
        .map_err(|e| anyhow::anyhow!("Failed to parse max value to scalar: {}", e))?;
    Ok(ColumnStatistics::new_unknown()
        .with_min_value(Precision::Exact(min))
        .with_max_value(Precision::Exact(max)))
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use beacon_nd_array::NdArray;
    use datafusion::scalar::ScalarValue;

    fn make_nd<T: beacon_nd_array::datatypes::NdArrayType>(
        values: Vec<T>,
        fill_value: Option<T>,
    ) -> NdArray<T> {
        let len = values.len();
        NdArray::try_new_from_vec_in_mem(values, vec![len], vec!["x".to_string()], fill_value)
            .unwrap()
    }

    #[tokio::test]
    async fn test_i32_exact_min_max() {
        let nd = make_nd(vec![3i32, 1, 7, 4], None);
        let col = column_stats_for_array(&nd).await.unwrap();
        assert_eq!(col.min_value, Precision::Exact(ScalarValue::Int32(Some(1))));
        assert_eq!(col.max_value, Precision::Exact(ScalarValue::Int32(Some(7))));
    }

    #[tokio::test]
    async fn test_f64_exact_min_max() {
        let nd = make_nd(vec![-1.5f64, 0.0, 3.5], None);
        let col = column_stats_for_array(&nd).await.unwrap();
        assert_eq!(
            col.min_value,
            Precision::Exact(ScalarValue::Float64(Some(-1.5)))
        );
        assert_eq!(
            col.max_value,
            Precision::Exact(ScalarValue::Float64(Some(3.5)))
        );
    }

    #[tokio::test]
    async fn test_fill_value_excluded_from_range() {
        // Fill value -9999 is below the real minimum; must not appear in stats.
        let nd = make_nd(vec![10i32, -9999, 5, -9999, 20], Some(-9999));
        let col = column_stats_for_array(&nd).await.unwrap();
        assert_eq!(col.min_value, Precision::Exact(ScalarValue::Int32(Some(5))));
        assert_eq!(
            col.max_value,
            Precision::Exact(ScalarValue::Int32(Some(20)))
        );
    }

    #[tokio::test]
    async fn test_all_fill_returns_unknown() {
        let nd = make_nd(vec![-9999i32, -9999], Some(-9999));
        let col = column_stats_for_array(&nd).await.unwrap();
        assert_eq!(col.min_value, Precision::Absent);
        assert_eq!(col.max_value, Precision::Absent);
    }

    #[tokio::test]
    async fn test_string_returns_unknown() {
        let nd = make_nd(vec!["a".to_string(), "z".to_string()], None);
        let col = column_stats_for_array(&nd).await.unwrap();
        assert_eq!(col.min_value, Precision::Absent);
        assert_eq!(col.max_value, Precision::Absent);
    }
}
