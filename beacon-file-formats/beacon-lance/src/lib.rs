//! Lance integration for beacon managed tables.
//!
//! Provides a columnar, transactionally-versioned table engine for beacon's
//! `CREATE TABLE`, backed by beacon's tables object store:
//! - a [`warehouse`] that routes Lance through that object store (via a
//!   `db://` scheme + [`warehouse::LanceWarehouse`]),
//! - a [`LanceTable`] `TableProvider` wrapper, and
//! - a [`LanceTableDefinition`] for persisting tables as `table.json`.
//!
//! Beacon's statement handler calls [`create_lance_table`] / [`drop_lance_table`]
//! / [`replace_table_contents`] for `CREATE TABLE` / `DROP TABLE` / `DELETE`+
//! `UPDATE`; discovery rebuilds providers via the definition's `build_provider`.
//!
//! Lance's atomic, versioned commits give readers a consistent snapshot with no
//! torn reads; a per-dataset write lock serializes writers.

pub mod alter;
pub mod definition;
pub mod index;
pub mod io;
pub mod mutate;
pub mod provider;
pub mod sink;
pub mod warehouse;

use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};

pub use alter::{alter_table, SchemaChange};
pub use definition::LanceTableDefinition;
pub use index::{create_index, drop_index, list_indices, IndexInfo, ScalarIndexKind};
pub use io::WriteKind;
pub use mutate::{delete_rows, update_rows};
pub use provider::LanceTable;
pub use warehouse::{beacon_namespace, LanceWarehouse, BEACON_NAMESPACE};

/// Create a new, empty Lance table at the warehouse location for
/// `namespace`/`name` and return a ready [`LanceTable`] provider. The empty
/// dataset carries the schema; `CREATE TABLE AS SELECT` inserts rows afterwards.
pub async fn create_lance_table(
    warehouse: Arc<LanceWarehouse>,
    namespace: &[String],
    name: &str,
    arrow_schema: &ArrowSchema,
) -> anyhow::Result<LanceTable> {
    let uri = warehouse.table_uri(namespace, name);
    tracing::info!(namespace = ?namespace, table = name, uri = %uri, "creating Lance table");

    // Store the Lance-writable schema (view types widened) so the provider's
    // schema matches the written dataset.
    let schema = io::lance_compatible_schema(arrow_schema);
    {
        let lock = warehouse.lock(&uri);
        let _guard = lock.lock().await;
        // Create an empty dataset that just establishes the schema; CTAS inserts
        // rows afterwards through the streaming insert path.
        io::write_stream(
            &uri,
            warehouse.session(),
            io::empty_stream(schema.clone()),
            WriteKind::Create,
        )
        .await?;
    }

    let definition = LanceTableDefinition::new(name, namespace.to_vec(), uri);
    Ok(LanceTable::new(definition, schema, warehouse))
}

/// Drop a Lance table by deleting all of its objects from the tables store.
pub async fn drop_lance_table(warehouse: &LanceWarehouse, uri: &str) -> anyhow::Result<()> {
    tracing::info!(uri = %uri, "dropping Lance table");

    let lock = warehouse.lock(uri);
    let _guard = lock.lock().await;

    let prefix = LanceWarehouse::object_path(uri);
    let store = warehouse.store();
    let mut listing = store.list(Some(&prefix));
    let mut locations = Vec::new();
    while let Some(entry) = listing.next().await {
        locations.push(
            entry
                .map_err(|e| anyhow::anyhow!("Failed to list Lance table files: {e}"))?
                .location,
        );
    }
    for location in locations {
        store
            .delete(&location)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to delete Lance table file: {e}"))?;
    }
    Ok(())
}

/// Replace **all** rows of a Lance table with the rows produced by `new_rows`
/// (atomic overwrite → a new dataset version). Used to implement `DELETE` /
/// `UPDATE`: the caller passes the surviving rows (everything that does *not*
/// match the predicate). An empty stream leaves an empty table.
pub async fn replace_table_contents(
    warehouse: &LanceWarehouse,
    uri: &str,
    new_rows: SendableRecordBatchStream,
) -> anyhow::Result<()> {
    tracing::info!(uri = %uri, "replacing Lance table contents");

    let lock = warehouse.lock(uri);
    let _guard = lock.lock().await;
    io::write_stream(uri, warehouse.session(), new_rows, WriteKind::Overwrite).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use tempfile::TempDir;

    fn sample_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    /// A fresh, isolated warehouse over a local-filesystem object store rooted in
    /// `dir`. Each test owns its own warehouse (and `TempDir`), so there is no
    /// shared global state.
    fn test_warehouse(dir: &TempDir) -> Arc<LanceWarehouse> {
        let store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        Arc::new(LanceWarehouse::new(store))
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

    #[tokio::test]
    async fn create_insert_replace_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let warehouse = test_warehouse(&dir);
        let namespace = beacon_namespace();

        let table = create_lance_table(warehouse.clone(), &namespace, "orders", &sample_schema())
            .await
            .expect("table should be created");
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

        // DELETE WHERE id = 1 -> keep id <> 1.
        let keep = ctx
            .sql("SELECT * FROM orders WHERE id <> 1")
            .await
            .unwrap()
            .execute_stream()
            .await
            .unwrap();
        replace_table_contents(&warehouse, &location, keep)
            .await
            .expect("replace should succeed");
        assert_eq!(count(&ctx, "orders").await, 2, "one row removed, two survive");
    }

    #[tokio::test]
    async fn definition_build_provider_rediscovers_rows() {
        use beacon_datafusion_ext::table_ext::TableDefinition;
        use datafusion::execution::object_store::ObjectStoreUrl;

        let dir = tempfile::tempdir().unwrap();
        let warehouse = test_warehouse(&dir);
        let namespace = beacon_namespace();

        let table = create_lance_table(warehouse.clone(), &namespace, "discovered", &sample_schema())
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("discovered", Arc::new(table)).unwrap();
        ctx.sql("INSERT INTO discovered VALUES (7, 'g')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Simulate restart: round-trip the definition through JSON and rebuild.
        let location = warehouse.table_uri(&namespace, "discovered");
        let definition: Arc<dyn TableDefinition> =
            Arc::new(LanceTableDefinition::new("discovered", namespace, location.clone()));
        let json = serde_json::to_string(&definition).unwrap();
        assert!(json.contains("\"lance\""), "typetag tag present: {json}");
        let restored: Arc<dyn TableDefinition> = serde_json::from_str(&json).unwrap();

        // build_provider resolves the warehouse from the session extension, so the
        // discovery context must carry it (as the runtime's does).
        let build_ctx = Arc::new(SessionContext::new_with_config(
            SessionConfig::new().with_extension(warehouse.clone()),
        ));
        let provider = restored
            .build_provider(build_ctx, &ObjectStoreUrl::parse("db://").unwrap())
            .await
            .expect("provider should rebuild from disk");

        let ctx2 = SessionContext::new();
        ctx2.register_table("discovered", provider).unwrap();
        assert_eq!(count(&ctx2, "discovered").await, 1, "row survives discovery");
    }

    #[tokio::test]
    async fn alter_add_and_rename_preserve_data() {
        use datafusion::arrow::array::StringArray;

        let dir = tempfile::tempdir().unwrap();
        let warehouse = test_warehouse(&dir);
        let namespace = beacon_namespace();

        let table = create_lance_table(warehouse.clone(), &namespace, "orders", &sample_schema())
            .await
            .unwrap();
        let location = table.definition().location.clone();
        let ctx = SessionContext::new();
        ctx.register_table("orders", Arc::new(table)).unwrap();
        ctx.sql("INSERT INTO orders VALUES (1, 'a'), (2, 'b')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // ADD COLUMN age INT (existing rows read NULL), then RENAME name -> full_name.
        alter_table(
            &warehouse,
            &location,
            &[SchemaChange::AddColumn {
                name: "age".to_string(),
                data_type: DataType::Int32,
            }],
        )
        .await
        .expect("add column should succeed");
        alter_table(
            &warehouse,
            &location,
            &[SchemaChange::RenameColumn {
                from: "name".to_string(),
                to: "full_name".to_string(),
            }],
        )
        .await
        .expect("rename column should succeed");

        // Reopen to pick up the evolved schema.
        let reopened = LanceTable::open(
            LanceTableDefinition::new("orders", beacon_namespace(), location.clone()),
            warehouse.clone(),
        )
        .await
        .unwrap();
        let ctx2 = SessionContext::new();
        ctx2.register_table("orders", Arc::new(reopened)).unwrap();

        // New column present and NULL for pre-existing rows.
        let batches = ctx2
            .sql("SELECT count(age) AS c FROM orders")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let non_null_age = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(non_null_age, 0, "pre-existing rows read NULL for age");

        // Renamed column keeps its values.
        let batches = ctx2
            .sql("SELECT full_name FROM orders WHERE id = 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let value = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(value, "a", "renamed column keeps its values");
    }

    #[tokio::test]
    async fn create_list_drop_index() {
        let dir = tempfile::tempdir().unwrap();
        let warehouse = test_warehouse(&dir);
        let namespace = beacon_namespace();

        let table = create_lance_table(warehouse.clone(), &namespace, "orders", &sample_schema())
            .await
            .unwrap();
        let location = table.definition().location.clone();
        let ctx = SessionContext::new();
        ctx.register_table("orders", Arc::new(table)).unwrap();
        ctx.sql("INSERT INTO orders VALUES (1, 'a'), (2, 'b')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        create_index(&warehouse, &location, "id", "id_idx", ScalarIndexKind::BTree)
            .await
            .expect("create index should succeed");

        let listed = list_indices(&warehouse, &location).await.expect("list indices");
        assert_eq!(listed.len(), 1, "one index present");
        assert_eq!(listed[0].name, "id_idx");
        assert_eq!(listed[0].columns, vec!["id".to_string()]);

        drop_index(&warehouse, &location, "id_idx")
            .await
            .expect("drop index should succeed");
        assert!(
            list_indices(&warehouse, &location).await.unwrap().is_empty(),
            "no indices after drop"
        );
    }

    #[tokio::test]
    async fn native_update_and_delete() {
        use datafusion::arrow::array::StringArray;

        let dir = tempfile::tempdir().unwrap();
        let warehouse = test_warehouse(&dir);
        let namespace = beacon_namespace();

        let table = create_lance_table(warehouse.clone(), &namespace, "orders", &sample_schema())
            .await
            .unwrap();
        let location = table.definition().location.clone();
        let ctx = SessionContext::new();
        // The provider reopens the latest dataset version on each scan, so it
        // observes the native mutations below without re-registration.
        ctx.register_table("orders", Arc::new(table)).unwrap();
        ctx.sql("INSERT INTO orders VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Native UPDATE name = 'Z' WHERE id = 2.
        update_rows(
            &warehouse,
            &location,
            Some("id = 2"),
            &[("name".to_string(), "'Z'".to_string())],
        )
        .await
        .expect("update should succeed");
        let batches = ctx
            .sql("SELECT name FROM orders WHERE id = 2")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let name = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(name, "Z", "row 2 should be updated");
        assert_eq!(count(&ctx, "orders").await, 3, "UPDATE must not change row count");

        // Native DELETE WHERE id = 1.
        delete_rows(&warehouse, &location, Some("id = 1"))
            .await
            .expect("delete should succeed");
        assert_eq!(count(&ctx, "orders").await, 2, "one row deleted");

        // Native DELETE all.
        delete_rows(&warehouse, &location, None)
            .await
            .expect("delete-all should succeed");
        assert_eq!(count(&ctx, "orders").await, 0, "delete-all empties the table");
    }
}
