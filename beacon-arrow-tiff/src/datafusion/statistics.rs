//! DataFusion column statistics derived from TIFF/GeoTIFF datasets.
//!
//! [`generate_statistics`] opens a TIFF file and computes per-column min/max
//! statistics for the columns in `table_schema`. Only cheap, low-rank arrays are
//! scanned: 0-D metadata scalars (e.g. `image.*`, `geo.epsg`) and the 1-D
//! coordinate arrays `geo.lon`/`geo.lat`. The 2-D bands (`band.*`) are left
//! [`ColumnStatistics::new_unknown`] because scanning them would read every
//! tile/strip.
//!
//! These per-file statistics let DataFusion (and our opener-level pruning, see
//! `source.rs`) skip files whose spatial extent cannot satisfy a query's
//! `geo.lon`/`geo.lat` predicate.

use std::sync::Arc;

use arrow::datatypes::Schema;
use beacon_nd_array::{NdArrayD, arrow::compute::value_range, dataset::AnyDataset};
use datafusion::{
    common::{ColumnStatistics, Statistics, stats::Precision},
    scalar::ScalarValue,
};
use object_store::{ObjectMeta, ObjectStore};

/// Open the TIFF file at `object` and return DataFusion [`Statistics`] for the
/// columns in `table_schema`.
pub async fn generate_statistics(
    store: Arc<dyn ObjectStore>,
    object: &ObjectMeta,
    table_schema: &Schema,
) -> anyhow::Result<Statistics> {
    let dataset = crate::datafusion::reader::open_dataset(store, object.clone()).await?;

    let mut stats = Statistics::default();
    for field in table_schema.fields() {
        // 0-D (metadata scalars) and 1-D (coordinate axes) arrays are cheap to scan;
        // 2-D bands are skipped to avoid reading pixel data.
        let col_stats = match dataset.get_array(field.name()) {
            Some(array) if array.dimensions().len() <= 1 => {
                column_stats_for_array(array.as_ref()).await?
            }
            _ => ColumnStatistics::new_unknown(),
        };
        stats = stats.add_column_statistics(col_stats);
    }

    // Row count = total pixels = product of any 2-D array's shape (metadata only).
    if let Some(rows) = total_pixel_rows(&dataset, table_schema) {
        stats.num_rows = Precision::Exact(rows);
    }

    Ok(stats)
}

/// Number of output rows = total pixels, taken from the shape of any 2-D band.
/// Reads only array metadata (no pixel I/O).
fn total_pixel_rows(dataset: &AnyDataset, schema: &Schema) -> Option<usize> {
    for field in schema.fields() {
        if let Some(array) = dataset.get_array(field.name())
            && array.dimensions().len() == 2
        {
            return Some(array.shape().iter().product());
        }
    }
    None
}

/// Return exact min/max [`ColumnStatistics`] for `array`, or unknown if the array
/// type has no orderable range (String, Binary, empty, all-fill).
async fn column_stats_for_array(array: &dyn NdArrayD) -> anyhow::Result<ColumnStatistics> {
    let Some((min_arr, max_arr)) = value_range(array).await else {
        return Ok(ColumnStatistics::new_unknown());
    };
    let min = ScalarValue::try_from_array(&min_arr, 0)
        .map_err(|e| anyhow::anyhow!("Failed to parse min value to scalar: {e}"))?;
    let max = ScalarValue::try_from_array(&max_arr, 0)
        .map_err(|e| anyhow::anyhow!("Failed to parse max value to scalar: {e}"))?;
    Ok(ColumnStatistics::new_unknown()
        .with_min_value(Precision::Exact(min))
        .with_max_value(Precision::Exact(max)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use beacon_nd_array::NdArray;
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, path::Path};

    const TEST_TIF_BYTES: &[u8] = include_bytes!("../../test-files/test.tif");

    fn make_nd<T: beacon_nd_array::datatypes::NdArrayType>(
        values: Vec<T>,
        fill_value: Option<T>,
    ) -> NdArray<T> {
        let len = values.len();
        NdArray::try_new_from_vec_in_mem(values, vec![len], vec!["x".to_string()], fill_value)
            .unwrap()
    }

    #[tokio::test]
    async fn column_stats_exact_min_max_for_coordinate_like_array() {
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
    async fn column_stats_unknown_for_string() {
        let nd = make_nd(vec!["a".to_string(), "z".to_string()], None);
        let col = column_stats_for_array(&nd).await.unwrap();
        assert_eq!(col.min_value, Precision::Absent);
        assert_eq!(col.max_value, Precision::Absent);
    }

    fn f64_of(p: &Precision<ScalarValue>) -> f64 {
        match p {
            Precision::Exact(ScalarValue::Float64(Some(v))) => *v,
            other => panic!("expected exact f64, got {other:?}"),
        }
    }

    /// End-to-end: the real fixture's `geo.lon`/`geo.lat` columns get exact extents
    /// (matching the coordinate ranges asserted in `reader.rs`), while a `band.*`
    /// column stays unknown.
    #[tokio::test]
    async fn infer_stats_for_real_fixture_has_coordinate_extents() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/stats/test.tif");
        store
            .put(&path, bytes::Bytes::copy_from_slice(TEST_TIF_BYTES).into())
            .await
            .unwrap();
        let object = store.head(&path).await.unwrap();

        let schema = crate::datafusion::reader::fetch_schema(object_store.clone(), object.clone())
            .await
            .unwrap();
        let stats = generate_statistics(object_store, &object, &schema)
            .await
            .unwrap();

        let idx = |name: &str| schema.index_of(name).unwrap();

        let lon = &stats.column_statistics[idx("geo.lon")];
        assert!((f64_of(&lon.min_value) - -17.312_499_364_464_315).abs() < 1e-6);
        assert!((f64_of(&lon.max_value) - 36.270_834_604_651_895).abs() < 1e-6);

        let lat = &stats.column_statistics[idx("geo.lat")];
        assert!((f64_of(&lat.min_value) - 30.166_666_664_989_14).abs() < 1e-6);
        assert!((f64_of(&lat.max_value) - 45.958_334_603_221_566).abs() < 1e-6);

        // Bands are 2-D and must not be scanned for stats.
        let band = &stats.column_statistics[idx("band.0")];
        assert_eq!(band.min_value, Precision::Absent);
        assert_eq!(band.max_value, Precision::Absent);

        // Row count = total pixels = 1287 * 380.
        assert_eq!(stats.num_rows, Precision::Exact(1287 * 380));
    }
}
