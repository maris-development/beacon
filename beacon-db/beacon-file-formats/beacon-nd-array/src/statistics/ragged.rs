use std::{ops::Range, sync::Arc};

use datafusion::common::{ColumnStatistics, Statistics, pruning::PrunableStatistics};
use indexmap::IndexMap;

use crate::{
    NdArrayD,
    arrow::batch::build_dataset_schema,
    dataset_v2::ragged::RaggedDataset,
    statistics::{file::column_statistics, range_statistics},
};

/// One container per range of casts, one column per ragged or instance array.
///
/// The columns follow the order of `ragged.inner.arrays` and leave out the
/// row-size arrays. Only 0-D and 1-D arrays get a min and max.
pub async fn ragged_chunk_statistics(
    ragged: &RaggedDataset,
    chunks: &[Range<usize>],
) -> anyhow::Result<PrunableStatistics> {
    let offsets = ragged.row_offsets().await?;
    let casts = offsets
        .values()
        .next()
        .map_or(0, |offsets| offsets.len() - 1);
    if let Some(chunk) = chunks
        .iter()
        .find(|chunk| chunk.start > chunk.end || chunk.end > casts)
    {
        anyhow::bail!("cast range {chunk:?} is not inside the {casts} casts");
    }

    let mut arrays = IndexMap::new();
    let mut columns: Vec<Vec<ColumnStatistics>> = vec![Vec::new(); chunks.len()];
    for (name, array) in &ragged.inner.arrays {
        let array_columns = if let Some(row_size_name) = ragged.ragged_arrays.get(name) {
            let offsets = &offsets[row_size_name];
            let ranges: Vec<Range<usize>> = chunks
                .iter()
                .map(|chunk| offsets[chunk.start]..offsets[chunk.end])
                .collect();
            array_statistics(array, &ranges).await?
        } else if ragged.instance_arrays.contains(name) {
            if array.dimensions().first() == Some(&ragged.instance_dimension) {
                array_statistics(array, chunks).await?
            } else {
                // The array does not change between casts.
                whole_array_statistics(array, chunks.len()).await
            }
        } else {
            continue;
        };
        arrays.insert(name.clone(), array.clone());
        for (chunk_columns, column) in columns.iter_mut().zip(array_columns) {
            chunk_columns.push(column);
        }
    }

    let statistics = columns
        .into_iter()
        .map(|column_statistics| {
            Arc::new(Statistics {
                column_statistics,
                ..Statistics::default()
            })
        })
        .collect();
    Ok(PrunableStatistics::new(
        statistics,
        build_dataset_schema(&arrays),
    ))
}

/// The statistics of each range of the leading axis of `array`.
async fn array_statistics(
    array: &Arc<dyn NdArrayD>,
    ranges: &[Range<usize>],
) -> anyhow::Result<Vec<ColumnStatistics>> {
    if array.dimensions().len() == 1 {
        range_statistics(array, ranges).await
    } else {
        Ok(vec![ColumnStatistics::new_unknown(); ranges.len()])
    }
}

/// The statistics of the whole `array`, once for each of the `count` chunks.
async fn whole_array_statistics(array: &Arc<dyn NdArrayD>, count: usize) -> Vec<ColumnStatistics> {
    let column = if array.dimensions().len() <= 1 {
        column_statistics(array.as_ref()).await
    } else {
        ColumnStatistics::new_unknown()
    };
    vec![column; count]
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use datafusion::{
        common::{Column, pruning::PruningStatistics},
        scalar::ScalarValue,
    };

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

    fn text(value: &str) -> Arc<dyn NdArrayD> {
        array(vec![value.to_string()], &[], &[])
    }

    async fn ragged(arrays: Vec<(&str, Arc<dyn NdArrayD>)>) -> RaggedDataset {
        let mut dimensions = IndexMap::new();
        for (_, array) in &arrays {
            for (dim, size) in array.dimensions().into_iter().zip(array.shape()) {
                dimensions.insert(dim, size);
            }
        }
        let dataset = Dataset {
            name: "test".to_string(),
            dimensions,
            arrays: arrays
                .into_iter()
                .map(|(name, array)| (name.to_string(), array))
                .collect(),
            invented_dimensions: HashSet::new(),
        };
        RaggedDataset::try_new(dataset).await.unwrap()
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

    /// Three casts with row sizes [2, 1, 3] on `z_obs`.
    async fn three_casts() -> RaggedDataset {
        ragged(vec![
            ("station", array(vec![100i32, 200, 300], &[3], &["casts"])),
            ("z_row_size", array(vec![2i16, 1, 3], &[3], &["casts"])),
            ("z_row_size.sample_dimension", text("z_obs")),
            (
                "depth",
                array(vec![1.0f64, 2.0, 5.0, 10.0, 20.0, 30.0], &[6], &["z_obs"]),
            ),
            ("version", array(vec![4i32], &[], &[])),
            ("levels", array(vec![1u8, 9], &[2], &["n_levels"])),
            ("grid", array(vec![0.0f32; 6], &[3, 2], &["casts", "n"])),
        ])
        .await
    }

    #[tokio::test]
    async fn chunks_get_the_range_of_their_casts() {
        let ragged = three_casts().await;
        let stats = ragged_chunk_statistics(&ragged, &[0..1, 1..3])
            .await
            .unwrap();

        assert_eq!(stats.num_containers(), 2);

        assert_eq!(value(&stats, "station", false, 0), Some(ScalarValue::Int32(Some(100))));
        assert_eq!(value(&stats, "station", true, 0), Some(ScalarValue::Int32(Some(100))));
        assert_eq!(value(&stats, "station", false, 1), Some(ScalarValue::Int32(Some(200))));
        assert_eq!(value(&stats, "station", true, 1), Some(ScalarValue::Int32(Some(300))));

        assert_eq!(value(&stats, "depth", false, 0), Some(ScalarValue::Float64(Some(1.0))));
        assert_eq!(value(&stats, "depth", true, 0), Some(ScalarValue::Float64(Some(2.0))));
        assert_eq!(value(&stats, "depth", false, 1), Some(ScalarValue::Float64(Some(5.0))));
        assert_eq!(value(&stats, "depth", true, 1), Some(ScalarValue::Float64(Some(30.0))));

        assert_eq!(value(&stats, "version", true, 1), Some(ScalarValue::Int32(Some(4))));
        assert_eq!(value(&stats, "levels", true, 0), Some(ScalarValue::UInt8(Some(9))));
        assert_eq!(value(&stats, "grid", false, 0), None);
        assert_eq!(value(&stats, "z_row_size", false, 0), None);
    }

    #[tokio::test]
    async fn chunk_past_the_last_cast_is_rejected() {
        let ragged = three_casts().await;
        assert!(ragged_chunk_statistics(&ragged, &[2..4]).await.is_err());
    }

    #[tokio::test]
    async fn too_many_observations_are_rejected() {
        let ragged = ragged(vec![
            ("z_row_size", array(vec![2i32, 5], &[2], &["casts"])),
            ("z_row_size.sample_dimension", text("z_obs")),
            ("depth", array(vec![0.0f64; 4], &[4], &["z_obs"])),
        ])
        .await;
        assert!(ragged_chunk_statistics(&ragged, &[0..2]).await.is_err());
    }
}
