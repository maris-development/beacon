//! External Iceberg table loading: produce a real on-disk Iceberg table with the
//! managed machinery, then read it back — and append to it — through the external
//! path (`load_external_iceberg_table`), exercising location parsing, the
//! per-location `FileCatalog`, scan self-registration, and writable `INSERT`.

use std::sync::Arc;

use beacon_iceberg::{
    catalog::build_local_file_catalog, create_iceberg_table, load_external_iceberg_table,
    ExternalIcebergTable, ExternalIcebergTableDefinition,
};
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::prelude::SessionContext;

fn sample_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ])
}

async fn count(ctx: &SessionContext, table: &str) -> i64 {
    let batches = ctx
        .sql(&format!("SELECT count(*) AS c FROM {table}"))
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
async fn external_table_reads_and_appends() {
    let dir = tempfile::tempdir().unwrap();

    // Produce a real Iceberg table on local fs at <dir>/sales/orders via the
    // managed machinery: build a file catalog, create the table, insert rows.
    let catalog = build_local_file_catalog(dir.path())
        .await
        .expect("catalog should build");
    let namespace = vec!["sales".to_string()];
    let table = create_iceberg_table(&catalog, &namespace, "orders", &sample_schema())
        .await
        .expect("table should be created");

    let producer = SessionContext::new();
    producer.register_table("orders", Arc::new(table)).unwrap();
    producer
        .sql("INSERT INTO orders VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Now open it as an EXTERNAL table: storage rooted at the same dir, location
    // pointing at <namespace>/<table>. A fresh SessionContext proves the external
    // path is self-contained (no shared registry with the producer).
    let storage = beacon_config::StorageConfig {
        datasets_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let external = load_external_iceberg_table(&storage, "datasets://sales/orders")
        .await
        .expect("external table should load");
    let definition = ExternalIcebergTableDefinition::new("ext_orders", "datasets://sales/orders");

    let reader = SessionContext::new();
    reader
        .register_table("ext_orders", Arc::new(ExternalIcebergTable::new(definition, external)))
        .unwrap();

    assert_eq!(
        count(&reader, "ext_orders").await,
        2,
        "external read should see the two produced rows"
    );

    // INSERT through the external provider commits via the per-location catalog.
    reader
        .sql("INSERT INTO ext_orders VALUES (3, 'c')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        count(&reader, "ext_orders").await,
        3,
        "external INSERT should be visible in the same session"
    );

    // A brand-new load sees the committed append on disk.
    let reloaded = load_external_iceberg_table(&storage, "datasets://sales/orders")
        .await
        .expect("external table should reload");
    let verify = SessionContext::new();
    verify
        .register_table(
            "ext_orders",
            Arc::new(ExternalIcebergTable::new(
                ExternalIcebergTableDefinition::new("ext_orders", "datasets://sales/orders"),
                reloaded,
            )),
        )
        .unwrap();
    assert_eq!(
        count(&verify, "ext_orders").await,
        3,
        "the appended row should be durable on disk"
    );
}

#[tokio::test]
async fn missing_external_table_errors() {
    let dir = tempfile::tempdir().unwrap();
    let storage = beacon_config::StorageConfig {
        datasets_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let result = load_external_iceberg_table(&storage, "datasets://nope/missing").await;
    assert!(result.is_err(), "loading a non-existent table should error");
}
