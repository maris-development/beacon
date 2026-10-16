use std::sync::Arc;

use datafusion::{
    common::{ColumnStatistics, Statistics, stats::Precision},
    scalar::ScalarValue,
};

use crate::{NdArrayD, arrow::compute::value_range, dataset_v2::Dataset};

/// One column per array, in the order of `dataset.arrays`.
pub async fn get_dataset_statistics(dataset: &Dataset) -> Arc<Statistics> {
    // Generate statistics for the given dataset. Only for the arrays that are either 0-dimensional or 1-dimensional. Everything else should only have minimal statistics.
    let mut statistics = Statistics::default();
    for (name, array) in &dataset.arrays {
        // A `<name>.sample_dimension` attribute marks a ragged row-size array.
        let is_row_size = dataset
            .arrays
            .contains_key(&format!("{name}.sample_dimension"));
        let column = if array.dimensions().len() <= 1 && !is_row_size {
            column_statistics(array.as_ref()).await
        } else {
            ColumnStatistics::new_unknown()
        };
        statistics = statistics.add_column_statistics(column);
    }
    Arc::new(statistics)
}

/// The exact min and max of `array`, or unknown when it has no orderable range.
pub(crate) async fn column_statistics(array: &dyn NdArrayD) -> ColumnStatistics {
    let Some((min, max)) = value_range(array).await else {
        return ColumnStatistics::new_unknown();
    };
    match (
        ScalarValue::try_from_array(&min, 0),
        ScalarValue::try_from_array(&max, 0),
    ) {
        (Ok(min), Ok(max)) => ColumnStatistics::new_unknown()
            .with_min_value(Precision::Exact(min))
            .with_max_value(Precision::Exact(max)),
        _ => ColumnStatistics::new_unknown(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use indexmap::IndexMap;

    use super::*;
    use crate::NdArray;

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

    fn dataset(arrays: Vec<(&str, Arc<dyn NdArrayD>)>) -> Dataset {
        Dataset {
            name: "test".to_string(),
            dimensions: IndexMap::new(),
            arrays: arrays
                .into_iter()
                .map(|(name, array)| (name.to_string(), array))
                .collect(),
            invented_dimensions: HashSet::new(),
        }
    }

    #[tokio::test]
    async fn low_rank_arrays_get_a_range() {
        let statistics = get_dataset_statistics(&dataset(vec![
            ("scalar", array(vec![5i32], &[], &[])),
            ("time", array(vec![3.0f64, 1.0, 2.0], &[3], &["time"])),
            ("temp", array(vec![1.0f32; 6], &[3, 2], &["time", "lat"])),
            ("name", array(vec!["a".to_string()], &[], &[])),
        ]))
        .await;

        let columns = &statistics.column_statistics;
        assert_eq!(columns.len(), 4);
        assert_eq!(
            columns[0].min_value,
            Precision::Exact(ScalarValue::Int32(Some(5)))
        );
        assert_eq!(
            columns[0].max_value,
            Precision::Exact(ScalarValue::Int32(Some(5)))
        );
        assert_eq!(
            columns[1].min_value,
            Precision::Exact(ScalarValue::Float64(Some(1.0)))
        );
        assert_eq!(
            columns[1].max_value,
            Precision::Exact(ScalarValue::Float64(Some(3.0)))
        );
        assert_eq!(columns[2], ColumnStatistics::new_unknown());
        assert_eq!(columns[3], ColumnStatistics::new_unknown());
        assert_eq!(statistics.num_rows, Precision::Absent);
    }

    #[tokio::test]
    async fn row_size_arrays_get_unknown_statistics() {
        let statistics = get_dataset_statistics(&dataset(vec![
            ("station", array(vec![1i32, 2, 3], &[3], &["casts"])),
            ("z_row_size", array(vec![2i32, 1, 3], &[3], &["casts"])),
            (
                "z_row_size.sample_dimension",
                array(vec!["z_obs".to_string()], &[], &[]),
            ),
        ]))
        .await;

        let columns = &statistics.column_statistics;
        assert_eq!(columns.len(), 3);
        assert_eq!(
            columns[0].max_value,
            Precision::Exact(ScalarValue::Int32(Some(3)))
        );
        assert_eq!(columns[1], ColumnStatistics::new_unknown());
    }
}
