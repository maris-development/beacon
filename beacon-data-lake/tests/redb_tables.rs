//! Integration coverage for the redb-backed tables store.
//!
//! Beacon's tables store — the catalog (`table.json`, …) *and* managed Lance
//! data — is a single-file [`beacon_redb_store::RedbStore`] opened by
//! [`ObjectStores::new`]. These tests exercise that store through the real
//! wiring: they build `ObjectStores` from a `StorageConfig` whose `db_path`
//! points at a temp `beacon.db`, then (1) run a full Lance table lifecycle over
//! it — the make-or-break check that Lance's atomic-commit path works on redb —
//! and (2) prove the catalog survives a store reopen (a "restart").

use std::sync::Arc;

use beacon_lance::{
    beacon_namespace, create_lance_table, drop_lance_table, replace_table_contents, LanceWarehouse,
};
use beacon_object_storage::{ObjectStores, StorageConfig};
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use tempfile::TempDir;

/// A `StorageConfig` rooted in `dir` with the tables store on a redb `beacon.db`.
/// Datasets/tmp are local-filesystem (unused here but built by `ObjectStores`).
fn storage_config(dir: &TempDir) -> StorageConfig {
    let root = dir.path().to_path_buf();
    let datasets_dir = root.join("datasets");
    let tmp_dir = root.join("tmp");
    // `LocalFileSystem` canonicalizes its root, so these must exist up front.
    std::fs::create_dir_all(&datasets_dir).unwrap();
    std::fs::create_dir_all(&tmp_dir).unwrap();
    StorageConfig {
        db_path: Some(root.join("beacon.db")),
        datasets_dir,
        tmp_dir,
        data_dir: root,
        ..Default::default()
    }
}

fn sample_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ])
}

async fn count(ctx: &SessionContext, name: &str) -> i64 {
    let batches = ctx
        .sql(&format!("SELECT count(*) AS c FROM {name}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

/// A managed Lance table lives entirely inside `beacon.db`: create → insert →
/// replace (DELETE) → drop all route through the redb store, exercising Lance's
/// list/put/rename/copy commit path over the single-file backend.
#[tokio::test]
async fn lance_table_lifecycle_over_redb() {
    let dir = tempfile::tempdir().unwrap();
    let stores = ObjectStores::new(&storage_config(&dir)).await.unwrap();
    let warehouse = Arc::new(LanceWarehouse::new(stores.tables.clone()));
    let namespace = beacon_namespace();

    let table = create_lance_table(warehouse.clone(), &namespace, "orders", &sample_schema())
        .await
        .expect("create table on redb");
    let location = table.definition().location.clone();

    let ctx = SessionContext::new();
    ctx.register_table("orders", Arc::new(table)).unwrap();

    ctx.sql("INSERT INTO orders VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(count(&ctx, "orders").await, 3, "three inserted rows visible");

    // DELETE WHERE id = 1 -> keep id <> 1 (atomic overwrite -> new version).
    let keep = ctx
        .sql("SELECT * FROM orders WHERE id <> 1")
        .await
        .unwrap()
        .execute_stream()
        .await
        .unwrap();
    replace_table_contents(&warehouse, &location, keep)
        .await
        .expect("replace (delete) on redb");
    assert_eq!(count(&ctx, "orders").await, 2, "one row removed, two survive");

    // Drop removes every object under the dataset prefix from the redb store.
    drop_lance_table(&warehouse, &location)
        .await
        .expect("drop on redb");
    let prefix = LanceWarehouse::object_path(&location);
    let remaining = stores
        .tables
        .list(Some(&prefix))
        .collect::<Vec<_>>()
        .await
        .len();
    assert_eq!(remaining, 0, "drop reclaims all dataset objects");
}

/// The catalog persists across a store reopen: an object written to the redb
/// tables store is still there after the `ObjectStores` (and its exclusive file
/// lock) are dropped and rebuilt on the same `beacon.db` — the restart property.
#[tokio::test]
async fn catalog_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let config = storage_config(&dir);
    let table_json = Path::from("obs/table.json");
    let body = br#"{"type":"lance","name":"obs"}"#;

    {
        let stores = ObjectStores::new(&config).await.unwrap();
        stores
            .tables
            .put(&table_json, body.to_vec().into())
            .await
            .unwrap();
    } // stores dropped -> RedbStore released the beacon.db lock

    let stores = ObjectStores::new(&config).await.unwrap();
    let got = stores
        .tables
        .get(&table_json)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(got.as_ref(), body, "table.json survived the reopen");
}
