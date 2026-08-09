//! The `read_*` table functions: ad-hoc scans over datasets-store files
//! addressed by relative path or glob, without any registered table.

mod common;

use beacon_arrow_hdf5::Hdf5Config;
use common::{runtime, runtime_with, scalar_i64, total_rows, write_file, TestRuntime};

async fn seeded(tag: &str) -> TestRuntime {
    seed(runtime(tag).await)
}

/// The same fixtures on a runtime that reads HDF5 with the pure-Rust reader.
async fn seeded_with_rust_hdf5(tag: &str) -> TestRuntime {
    let rt = runtime_with(tag, |b| {
        b.with_hdf5_config(Hdf5Config {
            use_rust_reader: true,
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
    std::fs::copy(netcdf_fixture(), rt.datasets_dir().join("wod.nc"))
        .expect("copy netcdf fixture");
    // A NetCDF-4 file *is* HDF5, so the same bytes serve `read_hdf5`.
    std::fs::copy(netcdf_fixture(), rt.datasets_dir().join("wod.h5"))
        .expect("copy netcdf fixture as hdf5");
    std::fs::copy(nested_hdf5_fixture(), rt.datasets_dir().join("nested.h5"))
        .expect("copy nested hdf5 fixture");
    std::fs::copy(tiff_fixture(), rt.datasets_dir().join("raster.tif"))
        .expect("copy geotiff fixture");
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

/// A plain HDF5 fixture, shipped with the HDF5 reader: no netCDF convention,
/// and its datasets two group levels deep.
fn nested_hdf5_fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("beacon-file-formats/beacon-arrow-hdf5/test_files/nested-groups.h5")
}

/// A stripped single-band GeoTIFF, shipped with the TIFF reader: 1287 x 380
/// float32 pixels on the axes `x` and `y`.
fn tiff_fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("beacon-file-formats/beacon-arrow-tiff/test-files/test.tif")
}

/// The GeoTIFF fixture's grid: `y` (image rows) x `x` (image columns).
const TIFF_HEIGHT: i64 = 380;
const TIFF_WIDTH: i64 = 1287;

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

/// `read_hdf5` reads a NetCDF-4 file on either reader, and both count the same
/// rows. The runtime default picks the reader.
#[tokio::test(flavor = "multi_thread")]
async fn read_hdf5_counts_the_same_rows_on_either_reader() {
    const COUNT: &str = "SELECT count(*) FROM read_hdf5('wod.h5')";

    let netcdf_c = seeded("read-hdf5-netcdf-c").await;
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
    assert_eq!(total_rows(&rust.sql(SQL).await), 12, "3 stations x 4 samples");

    let netcdf_c = seeded("read-hdf5-nested-netcdf-c").await;
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

    // `nested.h5` carries no dimension scales, so its axes are phony. The 1-d
    // dataset lives on the first axis alone; the 2-d ones need both.
    let narrowed = rt
        .sql("SELECT * FROM read_hdf5(['nested.h5'], ['phony_dim_0'])")
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
            (0..column.len()).map(|i| column.value(i).to_string()).collect::<Vec<_>>()
        })
        .collect();
    assert!(names.contains(&"station_id".to_string()), "{names:?}");
    assert!(
        names.contains(&"observations/qc/flag".to_string()),
        "{names:?}"
    );
}

/// A GeoTIFF rides the same nd pipeline as netCDF, HDF5 and zarr: the raster is
/// a `y` x `x` grid, and the 1-d coordinate axes broadcast over it.
#[tokio::test(flavor = "multi_thread")]
async fn read_tiff_scans_the_fixture_as_a_grid() {
    let rt = seeded("read-tiff").await;

    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM read_tiff('raster.tif')").await),
        TIFF_HEIGHT * TIFF_WIDTH,
        "the raster is one row per pixel"
    );

    // `geo.lat` lives on `y` alone, so it is a broadcast column of the grid the
    // full-rank band establishes. The band is co-selected because, as for every
    // nd format, the projected columns are what define the grid.
    let broadcast = rt
        .sql(
            r#"SELECT count("geo.lat")          AS lat_rows,
                      count(DISTINCT "geo.lat") AS lat_values,
                      count("band.0")           AS band_values
               FROM read_tiff('raster.tif')"#,
        )
        .await;
    let column = |name: &str| {
        let index = broadcast[0].schema().index_of(name).expect(name);
        arrow::array::as_primitive_array::<arrow::datatypes::Int64Type>(broadcast[0].column(index))
            .value(0)
    };
    assert_eq!(column("lat_rows"), TIFF_HEIGHT * TIFF_WIDTH);
    assert_eq!(column("lat_values"), TIFF_HEIGHT);
    // The band's nodata pixels come back as nulls, so it counts fewer.
    assert!(column("band_values") > 0);
    assert!(column("band_values") < column("lat_rows"));
}

/// The optional second argument sets the grid for a raster too: `['y']` keeps
/// the latitude axis and drops the band, which needs both axes.
#[tokio::test(flavor = "multi_thread")]
async fn read_tiff_takes_a_dimensions_argument() {
    let rt = seeded("read-tiff-dimensions").await;

    let narrowed = rt
        .sql("SELECT * FROM read_tiff(['raster.tif'], ['y'])")
        .await;
    let columns: Vec<String> = narrowed[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert!(columns.contains(&"geo.lat".to_string()), "{columns:?}");
    assert!(
        !columns.contains(&"band.0".to_string()),
        "a 2-d band does not fit a 1-d grid: {columns:?}"
    );
    assert_eq!(
        total_rows(&narrowed) as i64,
        TIFF_HEIGHT,
        "one row for each image row"
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
