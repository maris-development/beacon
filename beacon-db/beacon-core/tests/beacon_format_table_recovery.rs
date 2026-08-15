//! Startup recovery must be able to rebuild tables stored in **beacon's own** file
//! formats, not just DataFusion's built-ins.
//!
//! An external table's persisted definition names its format (`STORED AS BBF`),
//! resolved at recovery through `SessionState::get_file_format_factory`. Beacon
//! registers its formats — NetCDF, BBF, Atlas, Zarr, GeoTIFF, GeoParquet, HDF5 —
//! on the session; DataFusion ships CSV/Parquet/Arrow/JSON. If recovery runs before
//! beacon's registration, a table in a beacon format fails to rebuild with
//! "Could not find FileFormat" and is *skipped*, so it silently disappears from the
//! catalog on restart while CSV/Parquet tables survive.
//!
//! That asymmetry is why this uses HDF5: a CSV fixture passes either way and
//! proves nothing.

mod common;

use beacon_core::query::Query;
use beacon_core::query_result::QueryOutput;
use serde_json::json;

/// Write a real `.h5` file into the datasets store by running a query with HDF5
/// output and copying the result out of the tmp store.
///
/// HDF5 is used because it is both writable through a query output format and
/// registered by beacon under the same name `STORED AS` uses. NetCDF would be the
/// obvious choice but registers under the extension `nc`, so `STORED AS NETCDF`
/// does not resolve at all.
async fn write_hdf5_fixture(rt: &common::TestRuntime, rel: &str) {
    rt.sql("CREATE TABLE seed (a BIGINT, b DOUBLE)").await;
    rt.sql("INSERT INTO seed VALUES (1, 1.5), (2, 2.5), (3, 3.5)")
        .await;

    let mut query = Query::sql("SELECT a, b FROM seed".to_string());
    query.output = Some(serde_json::from_value(json!({ "format": "hdf5" })).expect("valid output"));
    let result = rt
        .runtime
        .run_query(query, rt.admin().await)
        .await
        .expect("hdf5 output query should run");

    let QueryOutput::File(file) = result.query_output else {
        panic!("hdf5 output should produce a file");
    };
    let dest = rt.datasets_dir().join(rel);
    std::fs::create_dir_all(dest.parent().expect("fixture has a parent")).unwrap();
    std::fs::copy(file.path(), &dest).expect("copy the hdf5 fixture into the datasets store");
    assert!(std::fs::metadata(&dest).unwrap().len() > 0, "fixture is non-empty");

    // The seed managed table is incidental; drop it so the restart assertions are
    // only about the external table under test.
    rt.sql("DROP TABLE seed").await;
}

/// An external table in a **beacon-registered** format survives a restart.
///
/// Regression test for recovery running before `register_file_formats`: the table
/// was persisted correctly but skipped while rebuilding, logging
/// "Failed to build provider for table '<name>': ... Could not find FileFormat"
/// and vanishing from the catalog.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn beacon_format_external_table_survives_restart() {
    let rt = common::restartable_runtime("beacon-format-recovery", |b| b).await;
    write_hdf5_fixture(&rt, "h5/data.h5").await;

    rt.sql("CREATE EXTERNAL TABLE h5obs STORED AS HDF5 LOCATION 'h5/data.h5'")
        .await;
    let before = common::scalar_i64(&rt.sql("SELECT count(*) FROM h5obs").await);
    assert_eq!(before, 3, "sanity: the HDF5 external table reads before restart");

    let rt = rt.restart().await;

    let listed = rt
        .sql("SELECT table_name FROM information_schema.tables WHERE table_name = 'h5obs'")
        .await;
    assert_eq!(
        common::total_rows(&listed),
        1,
        "an HDF5 external table must still be registered after a restart — if this is 0 the \
         provider failed to rebuild and recovery skipped it (check the ERROR log for \
         'Could not find FileFormat')"
    );

    assert_eq!(
        common::scalar_i64(&rt.sql("SELECT count(*) FROM h5obs").await),
        3,
        "the recovered HDF5 external table should still read its rows"
    );
}

/// Beacon's formats are resolvable on the session at all — the registration the
/// recovery path depends on. Cheap, and pins the format keys themselves so a
/// renamed `get_ext` is caught here rather than as a vanished table.
#[tokio::test(flavor = "multi_thread")]
async fn beacon_file_formats_are_registered_on_the_session() {
    let rt = common::runtime("beacon-format-registration").await;

    // Written as external tables because that is the path recovery replays; a
    // format that does not resolve fails here the same way it fails on restart.
    for (name, format) in [
        ("f_bbf", "BBF"),
        ("f_zarr", "ZARR"),
        ("f_tiff", "TIFF"),
        ("f_geoparquet", "GEOPARQUET"),
        ("f_parquet", "PARQUET"), // a DataFusion built-in, as the control
    ] {
        let sql = format!("CREATE EXTERNAL TABLE {name} STORED AS {format} LOCATION 'missing/'");
        let err = rt.try_sql(&sql).await.err().map(|e| e.to_string());
        if let Some(msg) = err {
            assert!(
                !msg.contains("Could not find FileFormat"),
                "{format} is not registered as a file format: {msg}"
            );
        }
    }
}
