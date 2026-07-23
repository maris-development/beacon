//! End-to-end tests for `open_delta_provider`: read, time travel, and
//! `INSERT INTO`, all through Beacon's `DatasetsStore` backend.

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use beacon_delta::{open_delta_provider, TimeTravel};
use beacon_object_storage::DatasetsStore;
use datafusion::prelude::SessionContext;
use deltalake::protocol::SaveMode;
use deltalake::DeltaTableBuilder;
use object_store::local::LocalFileSystem;
use url::Url;

fn batch(ids: &[i32]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
}

/// Write a 2-version Delta table at `<root>/tbl` (v0: 2 rows, v1: 4 rows).
async fn write_fixture(root: &std::path::Path) {
    let table_dir = root.join("tbl");
    std::fs::create_dir_all(&table_dir).unwrap();
    let url = Url::from_directory_path(&table_dir).unwrap();

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
}

async fn datasets_store(root: &std::path::Path) -> Arc<DatasetsStore> {
    let local = Arc::new(LocalFileSystem::new_with_prefix(root).unwrap());
    Arc::new(DatasetsStore::new(local, None).await)
}

async fn row_count(ctx: &Arc<SessionContext>, table: &str) -> usize {
    let batches = ctx
        .sql(&format!("SELECT id FROM {table}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_latest_and_time_travels() {
    let tmp = tempfile::tempdir().unwrap();
    write_fixture(tmp.path()).await;
    let store = datasets_store(tmp.path()).await;

    // Latest version: 4 rows.
    let ctx = Arc::new(SessionContext::new());
    let provider = open_delta_provider(ctx.clone(), store.clone(), "datasets://tbl", None)
        .await
        .unwrap();
    ctx.register_table("latest", provider).unwrap();
    assert_eq!(row_count(&ctx, "latest").await, 4);

    // Time travel to version 0: 2 rows.
    let provider_v0 = open_delta_provider(
        ctx.clone(),
        store.clone(),
        "datasets://tbl",
        Some(TimeTravel::Version(0)),
    )
    .await
    .unwrap();
    ctx.register_table("v0", provider_v0).unwrap();
    assert_eq!(row_count(&ctx, "v0").await, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_into_appends_a_new_version() {
    let tmp = tempfile::tempdir().unwrap();
    write_fixture(tmp.path()).await;
    let store = datasets_store(tmp.path()).await;

    let ctx = Arc::new(SessionContext::new());
    let provider = open_delta_provider(ctx.clone(), store.clone(), "datasets://tbl", None)
        .await
        .unwrap();
    ctx.register_table("dt", provider).unwrap();
    assert_eq!(row_count(&ctx, "dt").await, 4);

    // Append two more rows via INSERT INTO.
    ctx.sql("INSERT INTO dt VALUES (5), (6)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Re-open to observe the freshly committed version (6 rows total).
    let ctx2 = Arc::new(SessionContext::new());
    let reopened = open_delta_provider(ctx2.clone(), store.clone(), "datasets://tbl", None)
        .await
        .unwrap();
    ctx2.register_table("dt2", reopened).unwrap();
    assert_eq!(row_count(&ctx2, "dt2").await, 6);
}
