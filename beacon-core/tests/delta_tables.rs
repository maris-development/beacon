//! End-to-end tests for Delta Lake support driven through the SQL endpoint
//! (`Runtime::run_query`): the `read_delta()` table function, `CREATE EXTERNAL
//! TABLE ... STORED AS DELTA`, `INSERT INTO`, time travel, and `DROP TABLE`.

use std::sync::Arc;

use beacon_core::runtime::Runtime;
use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use deltalake::protocol::SaveMode;
use deltalake::DeltaTableBuilder;
use futures::TryStreamExt;

/// Run SQL as a super-user and collect the result batches.
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

fn batch(ids: &[i32]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
}

/// Write a 2-version Delta table under the datasets dir (v0: 2 rows, v1: 4 rows)
/// and return its `datasets://`-relative location.
async fn write_fixture(rel: &str) -> String {
    let dir = beacon_config::DATASETS_DIR_PATH.join(rel);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    // `DATASETS_DIR_PATH` is relative (e.g. `./data/datasets`); delta-rs needs an
    // absolute `file://` URL for the fixture.
    let abs = std::fs::canonicalize(&dir).unwrap();
    let url = url::Url::from_directory_path(&abs).unwrap();

    let table = DeltaTableBuilder::from_url(url).unwrap().build().unwrap();
    let table = table
        .write(vec![batch(&[1, 2])])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();
    let _ = table
        .write(vec![batch(&[3, 4])])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();

    format!("datasets://{rel}")
}

#[tokio::test(flavor = "multi_thread")]
async fn delta_external_table_read_insert_time_travel_drop() {
    let runtime = Runtime::new(Arc::new(beacon_config::Config::load().unwrap()))
        .await
        .expect("runtime should boot");

    let rel = format!("delta_e2e_{}", std::process::id());
    let location = write_fixture(&rel).await;
    let table = format!("dt_{}", std::process::id());
    let _ = runtime
        .run_query(
            beacon_core::query::Query::sql(format!("DROP TABLE IF EXISTS {table}")),
            true,
        )
        .await;

    // read_delta() table function: latest snapshot has 4 rows.
    let func_count = scalar_count(
        &run(
            &runtime,
            &format!("SELECT count(*) FROM read_delta('{location}')"),
        )
        .await,
    );
    assert_eq!(func_count, 4, "read_delta should see the latest 4 rows");

    // read_delta() time travel to version 0: 2 rows.
    let v0_count = scalar_count(
        &run(
            &runtime,
            &format!("SELECT count(*) FROM read_delta('{location}', 0)"),
        )
        .await,
    );
    assert_eq!(v0_count, 2, "read_delta(.., 0) should see version 0 (2 rows)");

    // CREATE EXTERNAL TABLE ... STORED AS DELTA, then SELECT.
    run(
        &runtime,
        &format!("CREATE EXTERNAL TABLE {table} STORED AS DELTA LOCATION '{location}'"),
    )
    .await;
    let count = scalar_count(&run(&runtime, &format!("SELECT count(*) FROM {table}")).await);
    assert_eq!(count, 4, "external Delta table should expose 4 rows");

    // INSERT INTO appends a new Delta version.
    run(&runtime, &format!("INSERT INTO {table} VALUES (5), (6)")).await;
    let after_insert =
        scalar_count(&run(&runtime, &format!("SELECT count(*) FROM read_delta('{location}')")).await);
    assert_eq!(after_insert, 6, "INSERT INTO should commit two more rows");

    // DROP TABLE deregisters it (the underlying Delta files remain on disk).
    run(&runtime, &format!("DROP TABLE {table}")).await;
    let err = runtime
        .run_query(
            beacon_core::query::Query::sql(format!("SELECT count(*) FROM {table}")),
            true,
        )
        .await;
    assert!(err.is_err(), "querying a dropped table should error");

    let _ = std::fs::remove_dir_all(beacon_config::DATASETS_DIR_PATH.join(&rel));
}
