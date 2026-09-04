//! `PARTITIONED BY` over a netCDF collection, through the runtime.
//!
//! A partition column is in the *path* of a file rather than inside it, and
//! DataFusion's `FileStream` appends its value per plan entry — which it can do
//! only because an entry is a file. A netCDF scan reads a whole collection
//! behind one entry, so the reader appends the values itself, per file. These
//! tests drive the statement a user writes and read what comes back.

mod common;

use common::{column_strings, runtime, scalar_i64, total_rows};

/// The WOD CTD fixture shipped with the netCDF reader.
fn netcdf_fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("beacon-file-formats/beacon-arrow-netcdf/test_files/wod_ctd_1964.nc")
}

/// One copy of the fixture per `year=` directory, under `obs/`.
fn write_collection(datasets: &std::path::Path, years: &[&str]) {
    for year in years {
        let dir = datasets.join("obs").join(format!("year={year}"));
        std::fs::create_dir_all(&dir).expect("the partition directory is created");
        std::fs::copy(netcdf_fixture(), dir.join("ctd.nc")).expect("the fixture is copied");
    }
}

/// The statement from the issue plans, runs, and gives every row the value of
/// its own file's path.
#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_netcdf_table_reads_every_directory() {
    let rt = runtime("partitioned-nc-reads").await;
    write_collection(rt.datasets_dir(), &["1964", "1965"]);

    rt.sql(
        "CREATE EXTERNAL TABLE observations \
         STORED AS NC \
         LOCATION 'obs/' \
         PARTITIONED BY (year)",
    )
    .await;

    // `SELECT *` carries the partition column beside the file's own columns.
    let star = rt.sql("SELECT * FROM observations LIMIT 5").await;
    assert!(
        star[0].schema().index_of("year").is_ok(),
        "the partition column stands in the table's schema"
    );
    assert!(total_rows(&star) > 0, "and the scan returns rows");

    // The column is in the table, and holds the directory names.
    let years = rt
        .sql("SELECT DISTINCT CAST(year AS VARCHAR) AS y FROM observations ORDER BY y")
        .await;
    assert_eq!(
        column_strings(&years, 0),
        vec!["1964".to_string(), "1965".to_string()],
        "both directories are read, and each keeps its own value"
    );

    // The two copies are the same file, so they hold the same number of rows,
    // and the table holds both.
    let counted = rt
        .sql(
            "SELECT CAST(year AS VARCHAR) AS y, count(*) AS n \
             FROM observations GROUP BY y ORDER BY y",
        )
        .await;
    let per_directory = scalar_i64(&rt.sql("SELECT count(*) FROM observations").await) / 2;
    assert!(per_directory > 0, "the fixture holds rows");

    let counts =
        arrow::array::AsArray::as_primitive::<arrow::datatypes::Int64Type>(counted[0].column(1));
    assert_eq!(counted[0].num_rows(), 2, "one row per directory");
    assert_eq!(
        (counts.value(0), counts.value(1)),
        (per_directory, per_directory),
        "each directory counts its own copy of the fixture"
    );
}

/// A filter on the partition column reads one directory and leaves the other.
#[tokio::test(flavor = "multi_thread")]
async fn a_filter_on_the_partition_column_reads_one_directory() {
    let rt = runtime("partitioned-nc-prunes").await;
    write_collection(rt.datasets_dir(), &["1964", "1965"]);

    rt.sql(
        "CREATE EXTERNAL TABLE observations \
         STORED AS NC \
         LOCATION 'obs/' \
         PARTITIONED BY (year)",
    )
    .await;

    let whole = scalar_i64(&rt.sql("SELECT count(*) FROM observations").await);
    let one = scalar_i64(
        &rt.sql("SELECT count(*) FROM observations WHERE year = '1965'")
            .await,
    );

    assert!(one > 0, "the matching directory is read");
    assert_eq!(one * 2, whole, "and the other one is not");

    // The rows that come back carry the value that selected them.
    let selected = rt
        .sql(
            "SELECT DISTINCT CAST(year AS VARCHAR) AS y \
             FROM observations WHERE year = '1965'",
        )
        .await;
    assert_eq!(column_strings(&selected, 0), vec!["1965".to_string()]);
}
