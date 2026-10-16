use std::{ops::Range, sync::Arc};

use datafusion::common::{
    ColumnStatistics, Statistics, pruning::PrunableStatistics, stats::Precision,
};

use crate::{
    NdArrayD,
    array::subset::ArraySubset,
    arrow::batch::build_dataset_schema,
    dataset_v2::gridded::GriddedDataset,
    statistics::{file::column_statistics, range_statistics},
};

/// One container per chunk, one column per array of `gridded`.
///
/// `chunks` are subsets of the grid. Only 0-D and 1-D arrays get a min and max.
pub async fn gridded_chunk_statistics(
    gridded: &GriddedDataset,
    chunks: &[ArraySubset],
) -> anyhow::Result<PrunableStatistics> {
    let arrays = &gridded.inner.arrays;
    let mut columns: Vec<Vec<ColumnStatistics>> =
        vec![Vec::with_capacity(arrays.len()); chunks.len()];
    for array in arrays.values() {
        let array_columns = array_chunk_statistics(gridded, array, chunks).await?;
        for (chunk_columns, column) in columns.iter_mut().zip(array_columns) {
            chunk_columns.push(column);
        }
    }

    let statistics = chunks
        .iter()
        .zip(columns)
        .map(|(chunk, column_statistics)| {
            Arc::new(Statistics {
                num_rows: Precision::Exact(chunk.rows()),
                total_byte_size: Precision::Absent,
                column_statistics,
            })
        })
        .collect();
    Ok(PrunableStatistics::new(
        statistics,
        build_dataset_schema(arrays),
    ))
}

/// The statistics of `array` for each chunk, in the order of `chunks`.
async fn array_chunk_statistics(
    gridded: &GriddedDataset,
    array: &Arc<dyn NdArrayD>,
    chunks: &[ArraySubset],
) -> anyhow::Result<Vec<ColumnStatistics>> {
    let dimensions = array.dimensions();
    match dimensions.as_slice() {
        [] => Ok(vec![column_statistics(array.as_ref()).await; chunks.len()]),
        [dimension] => {
            let axis = gridded
                .grid_dimensions
                .iter()
                .position(|d| d == dimension)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "dimension '{dimension}' is not in the grid {:?}",
                        gridded.grid_dimensions
                    )
                })?;
            let ranges: Vec<Range<usize>> = chunks
                .iter()
                .map(|chunk| chunk.start[axis]..chunk.start[axis] + chunk.shape[axis])
                .collect();
            range_statistics(array, &ranges).await
        }
        _ => Ok(vec![ColumnStatistics::new_unknown(); chunks.len()]),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use datafusion::{
        common::{Column, pruning::PruningStatistics},
        scalar::ScalarValue,
    };
    use indexmap::IndexMap;

    use super::*;
    use crate::{NdArray, dataset_v2::Dataset};

    fn array<T: crate::datatypes::NdArrayType + Clone>(
        values: Vec<T>,
        shape: &[usize],
        dims: &[&str],
    ) -> Arc<dyn NdArrayD> {
        Arc::new(
            NdArray::<T>::try_new_from_vec_in_mem(
                values,
                shape.to_vec(),
                dims.iter().map(|d| d.to_string()).collect(),
                None,
            )
            .unwrap(),
        )
    }

    fn gridded(arrays: Vec<(&str, Arc<dyn NdArrayD>)>) -> GriddedDataset {
        let dataset = Dataset {
            name: "test".to_string(),
            dimensions: IndexMap::new(),
            arrays: arrays
                .into_iter()
                .map(|(name, array)| (name.to_string(), array))
                .collect(),
            invented_dimensions: HashSet::new(),
        };
        GriddedDataset::try_new(dataset, None).unwrap()
    }

    /// The value at `index` of the min or max array of column `name`.
    fn value(
        stats: &PrunableStatistics,
        name: &str,
        max: bool,
        index: usize,
    ) -> Option<ScalarValue> {
        let column = Column::from_name(name);
        let values = if max {
            stats.max_values(&column)
        } else {
            stats.min_values(&column)
        }?;
        Some(ScalarValue::try_from_array(&values, index).unwrap())
    }

    #[tokio::test]
    async fn chunks_get_the_range_of_their_slice() {
        let gridded = gridded(vec![
            ("scalar", array(vec![7i32], &[], &[])),
            ("time", array(vec![10i64, 20, 30, 40], &[4], &["time"])),
            ("lat", array(vec![-1.0f64, 1.0], &[2], &["lat"])),
            ("temp", array(vec![0.0f32; 8], &[4, 2], &["time", "lat"])),
        ]);
        let chunks = vec![
            ArraySubset::new(vec![0, 0], vec![2, 1]),
            ArraySubset::new(vec![0, 1], vec![2, 1]),
            ArraySubset::new(vec![2, 0], vec![2, 2]),
        ];
        let stats = gridded_chunk_statistics(&gridded, &chunks).await.unwrap();

        assert_eq!(stats.num_containers(), 3);
        let rows = stats.row_counts().unwrap();
        assert_eq!(ScalarValue::try_from_array(&rows, 2).unwrap(), ScalarValue::UInt64(Some(4)));

        assert_eq!(value(&stats, "time", false, 0), Some(ScalarValue::Int64(Some(10))));
        assert_eq!(value(&stats, "time", true, 0), Some(ScalarValue::Int64(Some(20))));
        assert_eq!(value(&stats, "time", false, 2), Some(ScalarValue::Int64(Some(30))));
        assert_eq!(value(&stats, "time", true, 2), Some(ScalarValue::Int64(Some(40))));

        assert_eq!(value(&stats, "lat", false, 1), Some(ScalarValue::Float64(Some(1.0))));
        assert_eq!(value(&stats, "lat", false, 2), Some(ScalarValue::Float64(Some(-1.0))));
        assert_eq!(value(&stats, "lat", true, 2), Some(ScalarValue::Float64(Some(1.0))));

        assert_eq!(value(&stats, "scalar", true, 1), Some(ScalarValue::Int32(Some(7))));
        assert_eq!(value(&stats, "temp", false, 0), None);
    }
}
