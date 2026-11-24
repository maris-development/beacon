//! `PARTITIONED BY` over a netCDF collection, through the runtime.
//!
//! The netCDF format does not support partitioned tables. A query on one fails
//! with an error that names the partition columns.

mod common;

use common::runtime;

/// One regular file per `year=` directory, under `obs/`.
fn write_collection(datasets: &std::path::Path, years: &[&str]) {
    for year in years {
        let dir = datasets.join("obs").join(format!("year={year}"));
        std::fs::create_dir_all(&dir).expect("the partition directory is created");
        common::write_regular_netcdf(&dir.join("ctd.nc"), 4000);
    }
}

/// A query on a partitioned netCDF table fails, and the error names the column.
#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_netcdf_table_is_refused() {
    let rt = runtime("partitioned-nc-refused").await;
    write_collection(rt.datasets_dir(), &["1964", "1965"]);

    rt.sql(
        "CREATE EXTERNAL TABLE observations \
         STORED AS NC \
         LOCATION 'obs/' \
         PARTITIONED BY (year)",
    )
    .await;

    let error = rt
        .try_sql("SELECT \"V0\" FROM observations")
        .await
        .expect_err("a partitioned netCDF table is refused")
        .to_string();

    assert!(
        error.contains("does not support partitioned tables") && error.contains("year"),
        "{error}"
    );
}
