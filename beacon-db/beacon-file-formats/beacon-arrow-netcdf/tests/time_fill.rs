//! A CF time variable must null its `_FillValue` cells.
//!
//! Without the `_FillValue`, the Arrow layer masks nothing and every fill cell
//! reaches a query as a real date. A variable with
//! `units = "days since 1970-01-01"` and `_FillValue = -32768` gives `1880-03-15`
//! for each of those cells. The date passes a filter and joins a group.

use std::sync::Arc;

use arrow::array::{Array, TimestampNanosecondArray};
use beacon_arrow_netcdf::oxcdf_reader::read_arrays;
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

/// Write a time variable of `i16` days, with two fill cells, and read it back.
///
/// Cell order: day 0, fill, day 1, fill.
async fn time_array() -> Arc<dyn NdArrayD> {
    let directory = tempfile::tempdir().unwrap();
    let mut nc = oxcdf::create(directory.path().join(FILE)).expect("create the fixture");
    nc.add_dimension("obs", 4).unwrap();
    let mut variable = nc.add_variable::<i16>("time", &["obs"]).unwrap();
    // netCDF-4 refuses a `_FillValue` after the first write, so set the
    // attributes first.
    variable.set_fill_value(FILL).unwrap();
    variable
        .put_attribute("units", "days since 1970-01-01")
        .unwrap();
    variable.put_values(&[0i16, FILL, 1, FILL], ..).unwrap();
    nc.close().expect("close the fixture");

    let store: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(directory.path()).expect("a store on the temp dir"),
    );
    let file = Arc::new(
        AsyncNetcdfFile::open_store(store, Path::from(FILE))
            .await
            .expect("open the fixture"),
    );
    read_arrays(&file)
        .expect("read the fixture")
        .swap_remove("time")
        .expect("the fixture holds a 'time' variable")
}

/// The fill value carries the CF arithmetic of the variable, not the raw number.
#[tokio::test]
async fn the_fill_value_of_a_time_variable_is_decoded() {
    let array = time_array().await;

    let fill = array
        .as_any()
        .downcast_ref::<NdArray<TimestampNanosecond>>()
        .expect("a timestamp array")
        .fill_value()
        .await
        .expect("a time variable keeps its fill value");

    let expected = FILL as i64 * NANOS_PER_DAY;
    assert!(
        (fill.0 - expected).abs() <= MAX_NS_ERROR,
        "fill mismatch: got {}, expected ~{expected}",
        fill.0
    );
}

/// Cells 1 and 3 hold the fill value, so they are NULL. Cells 0 and 2 keep their dates.
#[tokio::test]
async fn the_fill_cells_of_a_time_variable_are_null() {
    let array = ndarray_to_arrow_array(time_array().await.as_ref())
        .await
        .expect("convert to Arrow");
    let array = array
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("a timestamp array");

    assert_eq!(array.len(), 4);
    assert_eq!(array.null_count(), 2);
    assert!(array.is_null(1) && array.is_null(3));
    assert_eq!(array.value(0), 0, "day 0 is the Unix epoch");
    assert!(
        (array.value(2) - NANOS_PER_DAY).abs() <= MAX_NS_ERROR,
        "day 1 mismatch: got {}, expected ~{NANOS_PER_DAY}",
        array.value(2)
    );
}
