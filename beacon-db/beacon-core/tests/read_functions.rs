//! The `read_*` table functions: ad-hoc scans over datasets-store files
//! addressed by relative path or glob, without any registered table.

mod common;

use common::{runtime, scalar_i64, total_rows, write_file, TestRuntime};

async fn seeded(tag: &str) -> TestRuntime {
    let rt = runtime(tag).await;
    write_file(&rt.datasets_dir().join("r/one.csv"), "v,name\n1,a\n2,b\n");
    write_file(&rt.datasets_dir().join("r/two.csv"), "v,name\n3,c\n");
    std::fs::copy(parquet_fixture(), rt.datasets_dir().join("pq.parquet"))
        .expect("copy parquet fixture");
    std::fs::copy(netcdf_fixture(), rt.datasets_dir().join("wod.nc"))
        .expect("copy netcdf fixture");
    rt
}

fn parquet_fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .join("test-datasets/test_file.parquet")
}

/// The WOD CTD fixture shipped with the NetCDF reader.
fn netcdf_fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("beacon-file-formats/beacon-arrow-netcdf/test_files/wod_ctd_1964.nc")
}

#[tokio::test(flavor = "multi_thread")]
async fn read_csv_scans_filters_and_projects() {
    let rt = seeded("read-csv").await;

    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM read_csv('r/one.csv')").await),
        2
    );
    // Projection + predicate over the ad-hoc scan.
    assert_eq!(
        scalar_i64(&rt.sql("SELECT v FROM read_csv('r/one.csv') WHERE name = 'b'").await),
        2,
        "the WHERE should select row (2, 'b')"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn read_csv_glob_merges_matching_files() {
    let rt = seeded("read-csv-glob").await;

    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM read_csv('r/*.csv')").await),
        3,
        "the glob should scan both CSVs"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn read_parquet_scans_the_fixture() {
    let rt = seeded("read-parquet").await;

    let count = scalar_i64(&rt.sql("SELECT count(*) FROM read_parquet('pq.parquet')").await);
    assert!(count > 0, "the parquet fixture should contain rows");

    let one = rt.sql("SELECT * FROM read_parquet('pq.parquet') LIMIT 1").await;
    assert_eq!(total_rows(&one), 1);
    assert!(
        one[0].num_columns() > 0,
        "SELECT * should project the fixture's columns"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn read_netcdf_scans_the_fixture() {
    let rt = seeded("read-netcdf").await;

    // `SELECT *` over an nd format narrows to a broadcast-compatible default
    // dimension set; the point here is only that rows come back at all.
    let rows = rt.sql("SELECT * FROM read_netcdf('wod.nc') LIMIT 5").await;
    assert!(
        total_rows(&rows) > 0,
        "the WOD CTD fixture should yield rows"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn read_of_a_missing_file_is_an_error_not_an_empty_result() {
    let rt = seeded("read-missing").await;

    let err = rt
        .try_sql("SELECT count(*) FROM read_csv('no/such/file.csv')")
        .await
        .err()
        .expect("reading a missing file should fail");
    // Whatever the exact wording, it must not silently return zero rows.
    assert!(!err.to_string().is_empty());
}
