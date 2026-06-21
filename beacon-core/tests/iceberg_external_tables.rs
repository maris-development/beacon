//! End-to-end tests for external Iceberg support driven through the SQL endpoint
//! (`Runtime::run_query`): the `read_iceberg()` table function, `CREATE EXTERNAL
//! TABLE ... STORED AS ICEBERG`, `INSERT INTO`, rejected mutations, and a
//! `DROP TABLE` that leaves the underlying files intact.
//!
//! The fixture is produced with beacon's *managed* Iceberg path (`CREATE TABLE`
//! + `INSERT`), which writes a real Iceberg table under the datasets store's
//! internal warehouse. We then point an external table at that on-disk location,
//! using the same runtime/config, so paths are guaranteed consistent.

use std::sync::Arc;

use beacon_core::runtime::Runtime;
use datafusion::arrow::array::{Int64Array, RecordBatch};
use futures::TryStreamExt;

/// Run SQL as a super-user (DDL/DML allowed) and collect the result batches.
async fn run(runtime: &Runtime, sql: &str) -> Vec<RecordBatch> {
    runtime
        .run_query(beacon_core::query::Query::sql(sql.to_string()), true)
        .await
        .unwrap_or_else(|error| panic!("SQL failed to plan/execute: {sql}\n{error}"))
        .into_record_stream()
        .unwrap_or_else(|error| panic!("expected a streamed result: {sql}\n{error}"))
        .try_collect()
        .await
        .unwrap_or_else(|error| panic!("SQL stream failed: {sql}\n{error}"))
}

fn scalar_count(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column should be Int64")
        .value(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn iceberg_external_table_read_insert_drop() {
    let runtime = Runtime::new(Arc::new(beacon_config::Config::load().unwrap()))
        .await
        .expect("runtime should boot");

    let src = format!("ice_ext_src_{}", std::process::id());
    let ext = format!("ice_ext_{}", std::process::id());
    for t in [&src, &ext] {
        let _ = runtime
            .run_query(
                beacon_core::query::Query::sql(format!("DROP TABLE IF EXISTS {t}")),
                true,
            )
            .await;
    }

    // Produce a real Iceberg table via the managed path (2 rows).
    run(&runtime, &format!("CREATE TABLE {src} (id BIGINT, name VARCHAR)")).await;
    run(&runtime, &format!("INSERT INTO {src} VALUES (1, 'a'), (2, 'b')")).await;

    // Its on-disk location under the datasets internal warehouse.
    let warehouse_table_dir = beacon_config::DATASETS_DIR_PATH
        .join("__beacon__")
        .join("iceberg")
        .join("beacon")
        .join(&src);
    assert!(warehouse_table_dir.exists(), "fixture table should exist on disk");
    let location = format!("datasets://__beacon__/iceberg/beacon/{src}");

    // read_iceberg() table function sees the two rows.
    let func_count = scalar_count(
        &run(
            &runtime,
            &format!("SELECT count(*) FROM read_iceberg('{location}')"),
        )
        .await,
    );
    assert_eq!(func_count, 2, "read_iceberg should see the two rows");

    // CREATE EXTERNAL TABLE ... STORED AS ICEBERG, then SELECT.
    run(
        &runtime,
        &format!("CREATE EXTERNAL TABLE {ext} STORED AS ICEBERG LOCATION '{location}'"),
    )
    .await;
    let count = scalar_count(&run(&runtime, &format!("SELECT count(*) FROM {ext}")).await);
    assert_eq!(count, 2, "external Iceberg table should expose 2 rows");

    // INSERT INTO appends and commits through the per-location catalog.
    run(&runtime, &format!("INSERT INTO {ext} VALUES (3, 'c')")).await;
    let after_insert = scalar_count(
        &run(
            &runtime,
            &format!("SELECT count(*) FROM read_iceberg('{location}')"),
        )
        .await,
    );
    assert_eq!(after_insert, 3, "INSERT INTO should commit a third row");

    // Row mutations / schema evolution are not supported on external tables.
    for stmt in [
        format!("DELETE FROM {ext} WHERE id = 1"),
        format!("UPDATE {ext} SET name = 'z' WHERE id = 1"),
        format!("ALTER TABLE {ext} ADD COLUMN x INT"),
    ] {
        let result = runtime
            .run_query(beacon_core::query::Query::sql(stmt.clone()), true)
            .await;
        assert!(result.is_err(), "should be rejected on external table: {stmt}");
    }

    // DROP TABLE deregisters the external table but must NOT delete its files.
    run(&runtime, &format!("DROP TABLE {ext}")).await;
    let err = runtime
        .run_query(
            beacon_core::query::Query::sql(format!("SELECT count(*) FROM {ext}")),
            true,
        )
        .await;
    assert!(err.is_err(), "querying a dropped table should error");
    assert!(
        warehouse_table_dir.exists(),
        "DROP of an external table must leave the underlying files intact"
    );

    // The data is still there to be re-read after the drop.
    let reread = scalar_count(
        &run(
            &runtime,
            &format!("SELECT count(*) FROM read_iceberg('{location}')"),
        )
        .await,
    );
    assert_eq!(reread, 3, "files survive DROP and remain readable");

    // Cleanup the managed fixture.
    run(&runtime, &format!("DROP TABLE {src}")).await;
}
