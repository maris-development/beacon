//! The `read_*` table functions: ad-hoc scans over datasets-store files
//! addressed by relative path or glob, without any registered table.

mod common;

use beacon_arrow_hdf5::Hdf5Config;
use common::{runtime, runtime_with, scalar_i64, total_rows, write_file, TestRuntime};

async fn seeded(tag: &str) -> TestRuntime {
    seed(runtime(tag).await)
}

/// The same fixtures on a runtime that reads HDF5 with the pure-Rust reader.
///
/// That is the default, so this is `seeded` with the reader named. It stays
/// named, because each test that uses it is about what this reader does.
async fn seeded_with_rust_hdf5(tag: &str) -> TestRuntime {
    seeded_with_hdf5_backend(tag, true).await
}

/// The same fixtures on a runtime that reads HDF5 with netcdf-c, the fallback.
async fn seeded_with_netcdf_c_hdf5(tag: &str) -> TestRuntime {
    seeded_with_hdf5_backend(tag, false).await
}

async fn seeded_with_hdf5_backend(tag: &str, use_rust_reader: bool) -> TestRuntime {
    let rt = runtime_with(tag, |b| {
        b.with_hdf5_config(Hdf5Config {
            use_rust_reader,
            ..Hdf5Config::default()
        })
    })
    .await;
    seed(rt)
}

fn seed(rt: TestRuntime) -> TestRuntime {
    write_file(&rt.datasets_dir().join("r/one.csv"), "v,name\n1,a\n2,b\n");
    write_file(&rt.datasets_dir().join("r/two.csv"), "v,name\n3,c\n");
    std::fs::copy(parquet_fixture(), rt.datasets_dir().join("pq.parquet"))
        .expect("copy parquet fixture");
    std::fs::copy(netcdf_fixture(), rt.datasets_dir().join("wod.nc")).expect("copy netcdf fixture");
    // A NetCDF-4 file *is* HDF5, so the same bytes serve `read_hdf5`.
    std::fs::copy(netcdf_fixture(), rt.datasets_dir().join("wod.h5"))
        .expect("copy netcdf fixture as hdf5");
    std::fs::copy(nested_hdf5_fixture(), rt.datasets_dir().join("nested.h5"))
        .expect("copy nested hdf5 fixture");
    std::fs::copy(
        instrument_hdf5_fixture(),
        rt.datasets_dir().join("instrument.h5"),
    )
    .expect("copy instrument hdf5 fixture");
    std::fs::copy(optodas_hdf5_fixture(), rt.datasets_dir().join("optodas.h5"))
        .expect("copy optodas hdf5 fixture");
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

/// A plain HDF5 fixture shaped like an instrument file: a payload of two axes
/// in the root group, a description of each channel in a second group, and
/// metadata that outnumbers both.
fn instrument_hdf5_fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("beacon-file-formats/beacon-arrow-hdf5/test_files/instrument.h5")
}

/// A plain HDF5 fixture in the ASN OptoDAS layout: the instrument fixture plus
/// the metadata that layout records about itself.
fn optodas_hdf5_fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("beacon-file-formats/beacon-arrow-hdf5/test_files/optodas.h5")
}

/// A plain HDF5 fixture, shipped with the HDF5 reader: no netCDF convention,
/// and its datasets two group levels deep.
fn nested_hdf5_fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("beacon-file-formats/beacon-arrow-hdf5/test_files/nested-groups.h5")
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
        scalar_i64(
            &rt.sql("SELECT v FROM read_csv('r/one.csv') WHERE name = 'b'")
                .await
        ),
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

    let count = scalar_i64(
        &rt.sql("SELECT count(*) FROM read_parquet('pq.parquet')")
            .await,
    );
    assert!(count > 0, "the parquet fixture should contain rows");

    let one = rt
        .sql("SELECT * FROM read_parquet('pq.parquet') LIMIT 1")
        .await;
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

/// `read_hdf5` reads a NetCDF-4 file on either reader, and both count the same
/// rows. The runtime default picks the reader.
#[tokio::test(flavor = "multi_thread")]
async fn read_hdf5_counts_the_same_rows_on_either_reader() {
    const COUNT: &str = "SELECT count(*) FROM read_hdf5('wod.h5')";

    let netcdf_c = seeded_with_netcdf_c_hdf5("read-hdf5-netcdf-c").await;
    let rows = scalar_i64(&netcdf_c.sql(COUNT).await);
    assert!(rows > 0, "the WOD CTD fixture should yield rows");

    let rust = seeded_with_rust_hdf5("read-hdf5-rust").await;
    assert_eq!(scalar_i64(&rust.sql(COUNT).await), rows);
}

/// A plain HDF5 file whose datasets live two group levels deep. The netCDF
/// reader reports only the root group, so the column exists on the Rust reader
/// alone.
#[tokio::test(flavor = "multi_thread")]
async fn read_hdf5_reaches_a_nested_group_on_the_rust_reader() {
    const SQL: &str = r#"SELECT "observations/qc/flag" FROM read_hdf5('nested.h5')"#;

    let rust = seeded_with_rust_hdf5("read-hdf5-nested-rust").await;
    assert_eq!(
        total_rows(&rust.sql(SQL).await),
        12,
        "3 stations x 4 samples"
    );

    let netcdf_c = seeded_with_netcdf_c_hdf5("read-hdf5-nested-netcdf-c").await;
    assert!(
        netcdf_c.try_sql(SQL).await.is_err(),
        "netcdf-c reports only the root group, so the column is not there"
    );
}

/// The optional second argument sets the grid on the Rust reader too: a dataset
/// comes back only when the list holds every one of its dimensions.
#[tokio::test(flavor = "multi_thread")]
async fn read_hdf5_takes_a_dimensions_argument() {
    let rt = seeded_with_rust_hdf5("read-hdf5-dimensions").await;

    // `nested.h5` carries no dimension scales, so its axes are the ones netCDF
    // invents. Beacon names each by its length, over every group of the file.
    // The 1-d dataset lives on the 3-long axis alone; the 2-d ones need both.
    let narrowed = rt
        .sql("SELECT * FROM read_hdf5(['nested.h5'], ['phony_len_3'])")
        .await;
    let columns: Vec<String> = narrowed[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert!(columns.contains(&"station_id".to_string()), "{columns:?}");
    assert!(
        !columns.contains(&"observations/temperature".to_string()),
        "a 2-d dataset does not fit a 1-d grid: {columns:?}"
    );
    assert_eq!(total_rows(&narrowed), 3, "one row for each station");
}

/// `read_hdf5_schema` names the columns without reading the data, and it sees
/// the nested-group columns the Rust reader adds.
#[tokio::test(flavor = "multi_thread")]
async fn read_hdf5_schema_lists_the_nested_columns() {
    let rt = seeded_with_rust_hdf5("read-hdf5-schema").await;

    let schema = rt
        .sql("SELECT column_name FROM read_hdf5_schema('nested.h5')")
        .await;
    let names: Vec<String> = schema
        .iter()
        .flat_map(|batch| {
            use arrow::array::Array;
            let column = arrow::array::as_string_array(batch.column(0));
            (0..column.len())
                .map(|i| column.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(names.contains(&"station_id".to_string()), "{names:?}");
    assert!(
        names.contains(&"observations/qc/flag".to_string()),
        "{names:?}"
    );
}

/// `SELECT *` over an instrument file lands on the payload, not on the
/// metadata that outnumbers it.
///
/// The file names no dimension, so beacon picks the grid by volume. The
/// payload holds 24 cells and the largest metadata grid holds 3.
#[tokio::test(flavor = "multi_thread")]
async fn read_hdf5_defaults_to_the_payload_grid_of_an_instrument_file() {
    let rt = seeded_with_rust_hdf5("read-hdf5-instrument").await;

    assert_eq!(
        scalar_i64(
            &rt.sql("SELECT count(*) FROM read_hdf5('instrument.h5')")
                .await
        ),
        24,
        "6 samples x 4 channels"
    );

    let schema = rt
        .sql("SELECT column_name FROM read_hdf5_schema('instrument.h5')")
        .await;
    let names = common::column_strings(&schema, 0);
    assert!(names.contains(&"data".to_string()), "{names:?}");
    assert!(names.contains(&"header/channels".to_string()), "{names:?}");
}

/// One query reads the payload of one group and the description of another.
#[tokio::test(flavor = "multi_thread")]
async fn read_hdf5_joins_two_groups_of_one_file() {
    let rt = seeded_with_rust_hdf5("read-hdf5-groups").await;

    let rows = rt
        .sql(
            r#"SELECT "data", "header/channels", "header/dt"
               FROM read_hdf5('instrument.h5') ORDER BY "data" LIMIT 4"#,
        )
        .await;
    assert_eq!(total_rows(&rows), 4);

    let batch = &rows[0];
    let payload = arrow::array::as_primitive_array::<arrow::datatypes::Int16Type>(batch.column(0));
    let channels = arrow::array::as_primitive_array::<arrow::datatypes::Int32Type>(batch.column(1));
    let dt = arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(batch.column(2));

    // The first sample holds all four channels, in order.
    assert_eq!(
        (0..4).map(|row| payload.value(row)).collect::<Vec<i16>>(),
        vec![0, 1, 2, 3]
    );
    assert_eq!(
        (0..4).map(|row| channels.value(row)).collect::<Vec<i32>>(),
        vec![0, 4, 8, 12]
    );
    // A scalar of a group reaches every row.
    assert_eq!(dt.value(0), 0.008);
}

/// The dimensions argument reaches a grid the default leaves out.
#[tokio::test(flavor = "multi_thread")]
async fn read_hdf5_reaches_the_metadata_grid_with_a_dimensions_argument() {
    let rt = seeded_with_rust_hdf5("read-hdf5-metadata-grid").await;

    let rows = rt
        .sql(r#"SELECT "instrument/gains" FROM read_hdf5(['instrument.h5'], ['phony_len_3'])"#)
        .await;
    assert_eq!(total_rows(&rows), 3, "the metadata grid is 3 long");
}

/// `read_hdf5` takes the convention as its third argument, and reads the
/// container alone without it.
#[tokio::test(flavor = "multi_thread")]
async fn read_hdf5_takes_a_convention_argument() {
    let rt = seeded_with_rust_hdf5("read-hdf5-convention").await;

    // No convention: the file holds no `time` column, so naming one is an error.
    assert!(
        rt.try_sql("SELECT time FROM read_hdf5('optodas.h5')")
            .await
            .is_err(),
        "the container alone holds no 'time' column"
    );

    // With it, the coordinates the file describes are columns.
    let rows = rt
        .sql(
            r#"SELECT time, distance, "data"
               FROM read_hdf5('optodas.h5', NULL, 'optodas')
               ORDER BY time, distance LIMIT 4"#,
        )
        .await;
    assert_eq!(total_rows(&rows), 4);

    let batch = &rows[0];
    let distance =
        arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(batch.column(1));
    let payload =
        arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(batch.column(2));
    // 4 raw channels apart, 1.25 m each, and counts scaled by 0.5.
    assert_eq!(
        (0..4).map(|row| distance.value(row)).collect::<Vec<f64>>(),
        vec![0.0, 5.0, 10.0, 15.0]
    );
    assert_eq!(
        (0..4).map(|row| payload.value(row)).collect::<Vec<f64>>(),
        vec![0.0, 0.5, 1.0, 1.5]
    );
}

/// An unknown convention is refused by name, at plan time.
#[tokio::test(flavor = "multi_thread")]
async fn read_hdf5_refuses_a_convention_it_does_not_know() {
    let rt = seeded_with_rust_hdf5("read-hdf5-bad-convention").await;

    let error = rt
        .try_sql("SELECT * FROM read_hdf5('optodas.h5', NULL, 'nope')")
        .await
        .err()
        .expect("an unknown convention is an error");
    assert!(error.to_string().contains("nope"), "{error}");
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
