//! `CREATE EXTERNAL TABLE … OPTIONS (…)` reaches the format factory.
//!
//! DataFusion's SQL planner renames an `OPTIONS` key without a `.` to
//! `format.<key>`. The crawler and a persisted `table.json` pass the key
//! unchanged. The NetCDF, HDF5, Zarr and BBF factories read only the bare key,
//! so every option written in SQL was dropped without a word. They now read
//! both spellings.

mod common;

use std::path::Path;

use common::{column_strings, runtime, TestRuntime};

/// The gridded NetCDF fixture: `analysed_sst`, `analysis_error`, `mask` and
/// `sea_ice_fraction` over `time`, `lat` and `lon`.
fn copy_gridded_netcdf(rt: &TestRuntime, rel: &str) {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("beacon-file-formats/beacon-arrow-netcdf/test_files/gridded-example.nc");
    std::fs::copy(fixture, rt.datasets_dir().join(rel)).expect("copy the NetCDF fixture");
}

/// The same grid as a Zarr store.
fn copy_gridded_zarr(rt: &TestRuntime, rel: &str) {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("beacon-file-formats/beacon-arrow-zarr/test_files/gridded-example.zarr");
    copy_dir(&fixture, &rt.datasets_dir().join(rel));
}

fn copy_dir(source: &Path, target: &Path) {
    std::fs::create_dir_all(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let to = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).unwrap();
        }
    }
}

/// The variable columns of a table, sorted.
///
/// A netCDF or Zarr table also carries one column per attribute, named
/// `<variable>.<attribute>`. Those columns do not depend on the grid, so they
/// say nothing about the dimensions a table reads.
async fn variable_columns(rt: &TestRuntime, table: &str) -> Vec<String> {
    let batches = rt.sql(&format!("DESCRIBE {table}")).await;
    let mut names: Vec<String> = column_strings(&batches, 0)
        .into_iter()
        .filter(|name| !name.contains('.'))
        .collect();
    names.sort();
    names
}

#[tokio::test(flavor = "multi_thread")]
async fn a_netcdf_table_honours_read_dimensions() {
    let rt = runtime("options-nc-read-dimensions").await;
    copy_gridded_netcdf(&rt, "grid.nc");

    rt.sql(
        "CREATE EXTERNAL TABLE grid STORED AS NC LOCATION 'grid.nc' \
         OPTIONS ('read_dimensions' 'lat')",
    )
    .await;

    // Only a variable whose dimensions the option lists is read. Every data
    // variable of the fixture spans `time`, `lat` and `lon`, so `lat` alone
    // leaves the `lat` coordinate.
    assert_eq!(variable_columns(&rt, "grid").await, vec!["lat"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_netcdf_table_without_read_dimensions_reads_the_default_grid() {
    let rt = runtime("options-nc-default-dimensions").await;
    copy_gridded_netcdf(&rt, "grid.nc");

    rt.sql("CREATE EXTERNAL TABLE grid STORED AS NC LOCATION 'grid.nc'")
        .await;

    // The baseline the option narrows: the full grid holds the data variables.
    let columns = variable_columns(&rt, "grid").await;
    assert!(
        columns.iter().any(|name| name == "analysed_sst"),
        "the default read holds the data variables: {columns:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_zarr_table_honours_read_dimensions() {
    let rt = runtime("options-zarr-read-dimensions").await;
    copy_gridded_zarr(&rt, "grid.zarr");

    rt.sql(
        "CREATE EXTERNAL TABLE grid STORED AS ZARR LOCATION 'grid.zarr/zarr.json' \
         OPTIONS ('read_dimensions' 'lat')",
    )
    .await;

    assert_eq!(variable_columns(&rt, "grid").await, vec!["lat"]);
}

/// The rule the create-table page states: an unknown key is ignored, not an
/// error. A factory reads the keys it knows and leaves the rest.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_key_is_ignored() {
    let rt = runtime("options-unknown-key").await;
    copy_gridded_netcdf(&rt, "grid.nc");

    rt.sql(
        "CREATE EXTERNAL TABLE grid STORED AS NC LOCATION 'grid.nc' \
         OPTIONS ('no_such_key' 'whatever')",
    )
    .await;

    let columns = variable_columns(&rt, "grid").await;
    assert!(
        columns.iter().any(|name| name == "analysed_sst"),
        "the table reads as it would without the key: {columns:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_netcdf_table_rejects_an_invalid_boolean_option() {
    let rt = runtime("options-nc-invalid-bool").await;
    copy_gridded_netcdf(&rt, "grid.nc");

    let error = rt
        .try_sql(
            "CREATE EXTERNAL TABLE grid STORED AS NC LOCATION 'grid.nc' \
             OPTIONS ('enable_statistics' 'perhaps')",
        )
        .await
        .expect_err("an invalid boolean must not be ignored")
        .to_string();
    assert!(error.contains("enable_statistics"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_hdf5_table_rejects_an_unknown_convention() {
    let rt = runtime("options-hdf5-unknown-convention").await;

    let error = rt
        .try_sql(
            "CREATE EXTERNAL TABLE experiments STORED AS HDF5 LOCATION 'experiments/' \
             OPTIONS ('convention' 'no-such-convention')",
        )
        .await
        .expect_err("an unknown convention must not be ignored")
        .to_string();
    assert!(error.contains("no-such-convention"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bbf_table_rejects_an_invalid_boolean_option() {
    let rt = runtime("options-bbf-invalid-bool").await;

    let error = rt
        .try_sql(
            "CREATE EXTERNAL TABLE obs STORED AS BBF LOCATION 'obs/' \
             OPTIONS ('split_streams_slice' 'perhaps')",
        )
        .await
        .expect_err("an invalid boolean must not be ignored")
        .to_string();
    assert!(error.contains("split_streams_slice"), "{error}");
}
