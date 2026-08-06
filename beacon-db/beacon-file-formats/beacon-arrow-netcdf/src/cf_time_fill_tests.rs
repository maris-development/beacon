//! A CF time variable must null its `_FillValue` cells.
//!
//! Regression test for a wrong value, not for a missing one. Both readers used
//! to drop the `_FillValue` of a time variable, so the Arrow layer masked
//! nothing and every fill cell reached a query as a real date. A variable with
//! `units = "days since 1970-01-01"` and `_FillValue = -32768` gave `1880-03-15`
//! for each of those cells. The date passed a filter and joined a group.
//!
//! The two readers share one CF conversion, so the fixture and the assertions
//! are shared too. Each reader decodes the same file and must give the same
//! nulls and the same values.

use std::sync::Arc;

use arrow::array::{Array, TimestampNanosecondArray};
use beacon_nd_array::{
    arrow::array::ndarray_to_arrow_array, datatypes::TimestampNanosecond, NdArray, NdArrayD,
};
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use oxcdf::AsyncNetcdfFile;

/// The `_FillValue` of the fixture, in days since the Unix epoch.
const FILL: i16 = -32768;
/// The name of the fixture object, inside the temp directory.
const FILE: &str = "cf_time_fill.nc";
const NANOS_PER_DAY: i64 = 86_400 * 1_000_000_000;
/// Maximum rounding error (nanoseconds) of the `f64` conversion chain of
/// hifitime. 1 µs is far tighter than any real time precision.
const MAX_NS_ERROR: i64 = 1_000;

/// Write a time variable of `i16` days, with two fill cells.
///
/// Cell order: day 0, fill, day 1, fill.
fn write_fixture(directory: &std::path::Path) -> std::path::PathBuf {
    let path = directory.join(FILE);
    {
        let mut nc = netcdf::create(&path).expect("create the fixture");
        nc.add_dimension("obs", 4).unwrap();
        let mut variable = nc.add_variable::<i16>("time", &["obs"]).unwrap();
        // netCDF-4 refuses a `_FillValue` after the first write, so set the
        // attributes first.
        variable.put_attribute("_FillValue", FILL).unwrap();
        variable
            .put_attribute("units", "days since 1970-01-01")
            .unwrap();
        variable
            .put_values(&[0i16, FILL, 1, FILL], netcdf::Extents::All)
            .unwrap();
    }
    path
}

/// The `time` array of the fixture, through the netcdf-c reader.
fn read_with_netcdf_c(path: &std::path::Path) -> Arc<dyn NdArrayD> {
    crate::reader::read_arrays(path)
        .expect("read the fixture with netcdf-c")
        .swap_remove("time")
        .expect("the fixture holds a 'time' variable")
}

/// The `time` array of the fixture, through the oxcdf reader.
async fn read_with_oxcdf(directory: &std::path::Path) -> Arc<dyn NdArrayD> {
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(directory).expect("a store on the temp dir"));
    let file = Arc::new(
        AsyncNetcdfFile::open_store(store, Path::from(FILE))
            .await
            .expect("open the fixture with oxcdf"),
    );
    crate::oxcdf_reader::read_arrays(&file)
        .expect("read the fixture with oxcdf")
        .swap_remove("time")
        .expect("the fixture holds a 'time' variable")
}

/// The decoded `_FillValue` the reader exposes to the Arrow layer.
async fn fill_value(array: &Arc<dyn NdArrayD>) -> Option<TimestampNanosecond> {
    array
        .as_any()
        .downcast_ref::<NdArray<TimestampNanosecond>>()
        .expect("a timestamp array")
        .fill_value()
        .await
}

/// The array as Arrow, with the fill cells masked.
async fn as_arrow(array: &Arc<dyn NdArrayD>) -> TimestampNanosecondArray {
    ndarray_to_arrow_array(array.as_ref())
        .await
        .expect("convert to Arrow")
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("a timestamp array")
        .clone()
}

/// The fill value carries the CF arithmetic of the variable, not the raw number.
fn assert_fill_matches_cf_arithmetic(fill: Option<TimestampNanosecond>, reader: &str) {
    let fill = fill.unwrap_or_else(|| panic!("{reader} drops the fill value of a time variable"));
    let expected = FILL as i64 * NANOS_PER_DAY;
    assert!(
        (fill.0 - expected).abs() <= MAX_NS_ERROR,
        "{reader}: fill mismatch: got {}, expected ~{expected}",
        fill.0
    );
}

/// Cells 1 and 3 hold the fill value, so they are NULL. Cells 0 and 2 keep
/// their dates.
fn assert_fill_cells_are_null(array: &TimestampNanosecondArray, reader: &str) {
    assert_eq!(array.len(), 4, "{reader}: cell count");
    assert_eq!(array.null_count(), 2, "{reader}: two cells hold the fill");
    assert!(array.is_null(1), "{reader}: cell 1 holds the fill");
    assert!(array.is_null(3), "{reader}: cell 3 holds the fill");
    assert!(array.is_valid(0), "{reader}: cell 0 holds day 0");
    assert!(array.is_valid(2), "{reader}: cell 2 holds day 1");
    assert_eq!(array.value(0), 0, "{reader}: day 0 is the Unix epoch");
    assert!(
        (array.value(2) - NANOS_PER_DAY).abs() <= MAX_NS_ERROR,
        "{reader}: day 1 mismatch: got {}, expected ~{NANOS_PER_DAY}",
        array.value(2)
    );
}

// ── netcdf-c ───────────────────────────────────────────────────────────────

#[test]
fn netcdf_c_decodes_the_fill_value_of_a_time_variable() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_fixture(directory.path());

    let array = read_with_netcdf_c(&path);
    let fill = futures::executor::block_on(fill_value(&array));
    assert_fill_matches_cf_arithmetic(fill, "netcdf-c");
}

#[tokio::test]
async fn netcdf_c_nulls_the_fill_cells_of_a_time_variable() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_fixture(directory.path());

    let array = as_arrow(&read_with_netcdf_c(&path)).await;
    assert_fill_cells_are_null(&array, "netcdf-c");
}

// ── oxcdf ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn oxcdf_decodes_the_fill_value_of_a_time_variable() {
    let directory = tempfile::tempdir().unwrap();
    write_fixture(directory.path());

    let array = read_with_oxcdf(directory.path()).await;
    assert_fill_matches_cf_arithmetic(fill_value(&array).await, "oxcdf");
}

#[tokio::test]
async fn oxcdf_nulls_the_fill_cells_of_a_time_variable() {
    let directory = tempfile::tempdir().unwrap();
    write_fixture(directory.path());

    let array = as_arrow(&read_with_oxcdf(directory.path()).await).await;
    assert_fill_cells_are_null(&array, "oxcdf");
}

// ── Parity ─────────────────────────────────────────────────────────────────

/// The same file gives the same answer on both readers. A dataset must not
/// change its values when the reader flag changes.
#[tokio::test]
async fn both_readers_null_the_same_cells() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_fixture(directory.path());

    let c = as_arrow(&read_with_netcdf_c(&path)).await;
    let rust = as_arrow(&read_with_oxcdf(directory.path()).await).await;

    assert_eq!(
        c.nulls().map(|n| n.iter().collect::<Vec<_>>()),
        rust.nulls().map(|n| n.iter().collect::<Vec<_>>())
    );
    assert_eq!(c.values(), rust.values());
}
