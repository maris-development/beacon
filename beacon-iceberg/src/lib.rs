//! Apache Iceberg integration for beacon managed tables.
//!
//! Provides:
//! - a shared, object-store-backed Iceberg [`catalog`],
//! - Arrow <-> Iceberg [`schema_convert`]ersion,
//! - an [`IcebergTable`] `TableProvider` wrapper, and
//! - an [`IcebergTableDefinition`] for persisting tables as `table.json`.
//!
//! Beacon's statement handler calls [`create_iceberg_table`] / [`drop_iceberg_table`]
//! for `CREATE TABLE` / `DROP TABLE`; discovery rebuilds providers via the
//! definition's `build_provider`.

pub mod catalog;
pub mod definition;
pub mod provider;
pub mod schema_convert;

use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion_iceberg::table::write_parquet_data_files;
use datafusion_iceberg::DataFusionTable;
use futures::StreamExt;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::table::Table;
use object_store::{ObjectStore, ObjectStoreExt};

pub use catalog::{
    beacon_namespace, get_catalog, get_warehouse_store, init_catalog, BEACON_NAMESPACE,
};
pub use definition::IcebergTableDefinition;
pub use provider::IcebergTable;

/// Create a new Iceberg table in `catalog` under `namespace` and return a ready
/// [`IcebergTable`] provider. The Arrow schema (typically the schema of a
/// `CREATE TABLE`/CTAS input plan) is converted to an Iceberg schema with fresh
/// field ids.
pub async fn create_iceberg_table(
    catalog: &Arc<dyn Catalog>,
    namespace: &[String],
    name: &str,
    arrow_schema: &ArrowSchema,
) -> anyhow::Result<IcebergTable> {
    let iceberg_schema = schema_convert::arrow_schema_to_iceberg(arrow_schema)?;

    let mut builder = Table::builder();
    builder.with_name(name).with_schema(iceberg_schema);
    let table = builder
        .build(namespace, catalog.clone())
        .await
        .map_err(|error| anyhow::anyhow!("Failed to create Iceberg table '{name}': {error}"))?;

    let definition = IcebergTableDefinition::new(name, namespace.to_vec());
    Ok(IcebergTable::new(definition, DataFusionTable::from(table)))
}

/// Drop an Iceberg table by deleting its `<namespace>/<name>/` directory (both
/// metadata and data) from the warehouse `store`.
///
/// The file catalog's own `drop_table` is unimplemented, so for a path-based
/// warehouse, removing the table's directory is the drop: a later `CREATE TABLE`
/// with the same name then finds no existing metadata and succeeds.
pub async fn drop_iceberg_table(
    store: &Arc<dyn ObjectStore>,
    namespace: &[String],
    name: &str,
) -> anyhow::Result<()> {
    let prefix = object_store::path::Path::from(format!("{}/{}", namespace.join("/"), name));

    let mut listing = store.list(Some(&prefix));
    let mut locations = Vec::new();
    while let Some(entry) = listing.next().await {
        locations.push(
            entry
                .map_err(|error| anyhow::anyhow!("Failed to list Iceberg table files: {error}"))?
                .location,
        );
    }

    for location in locations {
        store
            .delete(&location)
            .await
            .map_err(|error| anyhow::anyhow!("Failed to delete Iceberg table file: {error}"))?;
    }

    Ok(())
}

/// Replace **all** rows of an Iceberg table with the rows produced by `new_rows`
/// (copy-on-write). Used to implement `DELETE`: the caller passes the surviving
/// rows (everything that does *not* match the delete predicate), and this writes
/// them as fresh data files and atomically swaps the table's data files via an
/// Iceberg `replace` transaction.
///
/// An empty `new_rows` stream replaces the table with zero data files (i.e.
/// `DELETE` with no `WHERE`, or a predicate matching every row).
pub async fn replace_table_contents(
    catalog: &Arc<dyn Catalog>,
    namespace: &[String],
    name: &str,
    new_rows: SendableRecordBatchStream,
    task_ctx: &Arc<TaskContext>,
) -> anyhow::Result<()> {
    let identifier = Identifier::new(namespace, name);
    let mut table = match catalog
        .clone()
        .load_tabular(&identifier)
        .await
        .map_err(|error| anyhow::anyhow!("Failed to load Iceberg table '{name}': {error}"))?
    {
        Tabular::Table(table) => table,
        _ => anyhow::bail!("Iceberg identifier '{name}' does not refer to a table"),
    };

    let data_files = write_parquet_data_files(&table, new_rows, task_ctx, None)
        .await
        .map_err(|error| anyhow::anyhow!("Failed to write Iceberg data files: {error}"))?;

    table
        .new_transaction(None)
        .replace(data_files)
        .commit()
        .await
        .map_err(|error| anyhow::anyhow!("Failed to commit Iceberg replace: {error}"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::prelude::SessionContext;

    fn sample_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    #[tokio::test]
    async fn create_insert_select_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = catalog::build_local_file_catalog(dir.path())
            .await
            .expect("catalog should build");

        let namespace = beacon_namespace();
        let schema = sample_schema();
        let table = create_iceberg_table(&catalog, &namespace, "orders", &schema)
            .await
            .expect("table should be created");

        let ctx = SessionContext::new();
        ctx.register_table("orders", Arc::new(table))
            .expect("table should register");

        // Insert two rows through DataFusion.
        ctx.sql("INSERT INTO orders VALUES (1, 'a'), (2, 'b')")
            .await
            .expect("insert should plan")
            .collect()
            .await
            .expect("insert should execute");

        let batches = ctx
            .sql("SELECT count(*) AS c FROM orders")
            .await
            .expect("select should plan")
            .collect()
            .await
            .expect("select should execute");

        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column")
            .value(0);
        assert_eq!(count, 2, "two inserted rows should be visible");
    }

    #[tokio::test]
    async fn definition_serde_round_trip() {
        let definition = IcebergTableDefinition::new("orders", beacon_namespace());
        let boxed: Arc<dyn beacon_datafusion_ext::table_ext::TableDefinition> =
            Arc::new(definition);

        let json = serde_json::to_string(&boxed).expect("definition should serialize");
        assert!(json.contains("\"iceberg\""), "typetag tag present: {json}");

        let restored: Arc<dyn beacon_datafusion_ext::table_ext::TableDefinition> =
            serde_json::from_str(&json).expect("definition should deserialize");
        assert_eq!(restored.table_name(), "orders");
    }

    /// Proves the startup-discovery path: a persisted definition, rebuilt against
    /// the shared (global) catalog via `build_provider`, scans the rows that were
    /// written before "restart".
    #[tokio::test]
    async fn definition_build_provider_rediscovers_rows() {
        use beacon_datafusion_ext::table_ext::TableDefinition;
        use datafusion::execution::object_store::ObjectStoreUrl;

        let dir = tempfile::tempdir().unwrap();
        let catalog = catalog::build_local_file_catalog(dir.path())
            .await
            .expect("catalog should build");
        let namespace = beacon_namespace();

        let table = create_iceberg_table(&catalog, &namespace, "discovered", &sample_schema())
            .await
            .expect("table should be created");
        let ctx = SessionContext::new();
        ctx.register_table("discovered", Arc::new(table)).unwrap();
        ctx.sql("INSERT INTO discovered VALUES (7, 'g')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Simulate a restart: install the catalog as the process-global, then
        // round-trip the definition through JSON and rebuild the provider.
        init_catalog(catalog.clone());
        let definition: Arc<dyn TableDefinition> =
            Arc::new(IcebergTableDefinition::new("discovered", namespace));
        let json = serde_json::to_string(&definition).unwrap();
        let restored: Arc<dyn TableDefinition> = serde_json::from_str(&json).unwrap();

        let provider = restored
            .build_provider(
                Arc::new(SessionContext::new()),
                &ObjectStoreUrl::parse("tables://").unwrap(),
            )
            .await
            .expect("provider should rebuild from catalog");

        let ctx2 = SessionContext::new();
        ctx2.register_table("discovered", provider).unwrap();
        let batches = ctx2
            .sql("SELECT count(*) AS c FROM discovered")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 1, "row written before restart should survive discovery");
    }

    /// Count rows by loading a fresh provider straight from the catalog (the
    /// catalog is the source of truth after a `replace`, so a stale in-session
    /// provider must not be reused).
    async fn count_from_catalog(
        catalog: &Arc<dyn Catalog>,
        namespace: &[String],
        name: &str,
    ) -> i64 {
        let tabular = catalog
            .clone()
            .load_tabular(&Identifier::new(namespace, name))
            .await
            .expect("table should load");
        let table = match tabular {
            Tabular::Table(table) => table,
            _ => panic!("expected a table"),
        };
        let ctx = SessionContext::new();
        ctx.register_table(name, Arc::new(DataFusionTable::from(table)))
            .expect("provider should register");
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
    async fn replace_contents_deletes_matching_and_all_rows() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = catalog::build_local_file_catalog(dir.path())
            .await
            .expect("catalog should build");
        let namespace = beacon_namespace();

        let table = create_iceberg_table(&catalog, &namespace, "orders", &sample_schema())
            .await
            .expect("table should be created");
        let ctx = SessionContext::new();
        ctx.register_table("orders", Arc::new(table)).unwrap();
        ctx.sql("INSERT INTO orders VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // DELETE WHERE id = 1  ->  keep rows where id <> 1.
        let keep = ctx
            .sql("SELECT * FROM orders WHERE id <> 1")
            .await
            .unwrap()
            .execute_stream()
            .await
            .unwrap();
        replace_table_contents(&catalog, &namespace, "orders", keep, &ctx.task_ctx())
            .await
            .expect("replace should succeed");
        assert_eq!(
            count_from_catalog(&catalog, &namespace, "orders").await,
            2,
            "one row removed, two survive"
        );

        // DELETE (all)  ->  keep nothing (empty stream).
        let fresh = SessionContext::new();
        let tabular = catalog
            .clone()
            .load_tabular(&Identifier::new(&namespace, "orders"))
            .await
            .unwrap();
        let table = match tabular {
            Tabular::Table(table) => table,
            _ => panic!("expected a table"),
        };
        fresh
            .register_table("orders", Arc::new(DataFusionTable::from(table)))
            .unwrap();
        let none = fresh
            .sql("SELECT * FROM orders WHERE 1 = 0")
            .await
            .unwrap()
            .execute_stream()
            .await
            .unwrap();
        replace_table_contents(&catalog, &namespace, "orders", none, &fresh.task_ctx())
            .await
            .expect("delete-all replace should succeed");
        assert_eq!(
            count_from_catalog(&catalog, &namespace, "orders").await,
            0,
            "delete-all leaves an empty table"
        );
    }
}
