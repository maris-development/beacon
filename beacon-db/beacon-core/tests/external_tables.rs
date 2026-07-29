//! `CREATE EXTERNAL TABLE`: registering datasets-store locations as named
//! tables. Locations are relative to the default (datasets) store, resolved
//! through the same root-store machinery as the `read_*` functions.
//!
//! The NetCDF variant has its own test (`explain_analyze_external_netcdf`); this
//! file covers the listing-based formats and the drop/re-register lifecycle.

mod common;

use common::{runtime, scalar_i64, total_rows, write_file};

fn parquet_fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .join("test-datasets/test_file.parquet")
}

#[tokio::test(flavor = "multi_thread")]
async fn csv_external_table_over_a_directory() {
    let rt = runtime("ext-csv-dir").await;
    write_file(&rt.datasets_dir().join("obs/one.csv"), "v,name\n1,a\n2,b\n");
    write_file(&rt.datasets_dir().join("obs/two.csv"), "v,name\n3,c\n");

    rt.sql("CREATE EXTERNAL TABLE obs STORED AS CSV LOCATION 'obs/'")
        .await;

    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM obs").await),
        3,
        "the directory location should scan both files"
    );
    assert_eq!(
        scalar_i64(&rt.sql("SELECT v FROM obs WHERE name = 'c'").await),
        3,
        "predicates should work over the external table"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn csv_external_table_over_a_single_file() {
    let rt = runtime("ext-csv-file").await;
    write_file(&rt.datasets_dir().join("solo.csv"), "v,name\n7,x\n");

    rt.sql("CREATE EXTERNAL TABLE solo STORED AS CSV LOCATION 'solo.csv'")
        .await;

    assert_eq!(scalar_i64(&rt.sql("SELECT count(*) FROM solo").await), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn parquet_external_table_reads_the_fixture() {
    let rt = runtime("ext-parquet").await;
    std::fs::create_dir_all(rt.datasets_dir().join("pq")).unwrap();
    std::fs::copy(parquet_fixture(), rt.datasets_dir().join("pq/data.parquet"))
        .expect("copy parquet fixture");

    rt.sql("CREATE EXTERNAL TABLE pq STORED AS PARQUET LOCATION 'pq/'")
        .await;

    assert!(
        scalar_i64(&rt.sql("SELECT count(*) FROM pq").await) > 0,
        "the parquet fixture should contain rows"
    );
    let one = rt.sql("SELECT * FROM pq LIMIT 1").await;
    assert_eq!(total_rows(&one), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_external_table_is_gone_and_the_name_is_reusable() {
    let rt = runtime("ext-drop").await;
    write_file(&rt.datasets_dir().join("d/a.csv"), "v\n1\n");
    write_file(&rt.datasets_dir().join("d2/b.csv"), "v\n1\n2\n");

    rt.sql("CREATE EXTERNAL TABLE dropme STORED AS CSV LOCATION 'd/'")
        .await;
    assert_eq!(scalar_i64(&rt.sql("SELECT count(*) FROM dropme").await), 1);

    rt.sql("DROP TABLE dropme").await;
    assert!(
        rt.try_sql("SELECT count(*) FROM dropme").await.is_err(),
        "a dropped external table should no longer resolve"
    );

    // The name is free again, pointing wherever the new definition says.
    rt.sql("CREATE EXTERNAL TABLE dropme STORED AS CSV LOCATION 'd2/'")
        .await;
    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM dropme").await),
        2,
        "the re-created table should read the new location"
    );
}
