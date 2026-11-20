//! Chunk pruning: which chunks of a file no row of the predicate can come from.
//!
//! Pruning only skips work. The `FilterExec` above the scan applies the
//! predicate again, so a chunk that stays is always correct.

use beacon_nd_array::{
    array::subset::ArraySubset,
    arrow::{
        pushdown::{compute_chunk_mask, is_pushdown_candidate, mask_is_all_false, mask_pushdown},
        pushdown_filter::PushdownFilter,
    },
};

use super::chunks::FileScan;

/// The keep mask of each coordinate, keyed by its dimension.
pub(super) type Masks = Vec<(String, Vec<bool>)>;

/// Read the 1-D coordinates the predicate bounds, and build their keep masks.
///
/// A mask that cannot be built is left out, so its chunks are read.
pub(super) async fn coordinate_masks(scan: &FileScan, predicate: &PushdownFilter) -> Masks {
    let mut masks = Masks::new();
    for (name, array) in scan.arrays() {
        let Some(range) = predicate.ranges().get(name) else {
            continue;
        };
        if !is_pushdown_candidate(array) {
            continue;
        }
        let dim = array.dimensions()[0].clone();
        match mask_pushdown(array.clone(), range).await {
            Ok(mask) => masks.push((dim, mask)),
            Err(e) => {
                tracing::debug!("no pushdown mask for '{name}', reading every chunk of it: {e}")
            }
        }
    }
    masks
}

/// Whether no row of `chunk` can meet the predicate.
pub(super) fn is_pruned(masks: &Masks, dims: &[String], chunk: &ArraySubset) -> bool {
    if masks.is_empty() {
        return false;
    }
    mask_is_all_false(&compute_chunk_mask(masks, dims, &chunk.start, &chunk.shape))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use beacon_nd_array::dataset::{AnyDataset, Dataset};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use object_store::{local::LocalFileSystem, path::Path, ObjectStore, ObjectStoreExt};

    use super::*;

    async fn gridded(names: &[&str]) -> FileScan {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_files/gridded-example.nc");
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let object = store
            .head(&Path::from_absolute_path(&path).unwrap())
            .await
            .unwrap();
        let mut dataset: Dataset = match crate::oxcdf_reader::open_object(store, &object)
            .await
            .unwrap()
        {
            AnyDataset::Regular(dataset) => dataset,
            AnyDataset::Ragged { .. } => panic!("the gridded file is regular"),
        };
        dataset
            .arrays
            .retain(|name, _| names.contains(&name.as_str()));
        FileScan::try_new(dataset, 8192).unwrap()
    }

    /// `lat > threshold`, over a schema that holds `lat` as Float32.
    fn lat_above(threshold: f32) -> PushdownFilter {
        let schema = Schema::new(vec![Field::new("lat", DataType::Float32, true)]);
        let expr = binary(
            col("lat", &schema).unwrap(),
            Operator::Gt,
            lit(threshold),
            &schema,
        )
        .unwrap();
        PushdownFilter::new(expr)
    }

    fn kept(scan: &FileScan, masks: &Masks) -> usize {
        scan.chunks()
            .iter()
            .filter(|chunk| !is_pruned(masks, scan.dims(), chunk))
            .count()
    }

    // The fixture's `lat` runs from about 38.8 to 48.8, in 2 native chunks of 604.

    #[tokio::test]
    async fn a_filter_no_row_meets_prunes_every_chunk() {
        let scan = gridded(&["analysed_sst", "lat"]).await;
        let masks = coordinate_masks(&scan, &lat_above(1000.0)).await;
        assert_eq!(kept(&scan, &masks), 0);
    }

    #[tokio::test]
    async fn a_filter_every_row_meets_prunes_nothing() {
        let scan = gridded(&["analysed_sst", "lat"]).await;
        let masks = coordinate_masks(&scan, &lat_above(0.0)).await;
        assert_eq!(kept(&scan, &masks), 4);
    }

    #[tokio::test]
    async fn a_filter_on_the_upper_half_keeps_half_the_chunks() {
        let scan = gridded(&["analysed_sst", "lat"]).await;
        let masks = coordinate_masks(&scan, &lat_above(44.5)).await;
        assert_eq!(kept(&scan, &masks), 2);
    }

    #[tokio::test]
    async fn a_filter_on_a_column_the_scan_does_not_hold_prunes_nothing() {
        let scan = gridded(&["analysed_sst"]).await;
        let masks = coordinate_masks(&scan, &lat_above(1000.0)).await;
        assert!(masks.is_empty());
        assert_eq!(kept(&scan, &masks), 4);
    }
}
