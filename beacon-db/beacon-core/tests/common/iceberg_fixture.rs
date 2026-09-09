//! Builds a real Apache Iceberg table for the tests to read.
//!
//! Beacon only reads Iceberg, so a test needs a table some *other* system wrote.
//! This is that other system: iceberg-rust's in-memory catalog over a local
//! warehouse directory, with rows inserted through DataFusion. The table is then
//! copied into the runtime's datasets directory, which is also how a real
//! deployment gets one — written elsewhere, mounted here. That copy matters:
//! the metadata keeps the absolute paths of the warehouse it was written in, so
//! reading it from the datasets directory exercises the path rebasing that makes
//! a relocated (or S3-hosted) table readable at all.

#![allow(dead_code)] // each test binary uses a different subset of these helpers.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MemoryCatalog, MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_datafusion::IcebergCatalogProvider;

/// The namespace the fixture table lives in inside its own warehouse.
const NAMESPACE: &str = "ocean";
/// The table name inside that namespace.
const TABLE: &str = "obs";

/// A writable Iceberg table, plus where it lives and where Beacon reads it.
pub struct IcebergFixture {
    catalog: Arc<MemoryCatalog>,
    /// The warehouse the table is written into.
    warehouse: PathBuf,
    /// The directory inside the datasets store the table is published to.
    published: PathBuf,
    ident: TableIdent,
}

impl IcebergFixture {
    /// Write an Iceberg table with 4 rows of `(id, name, value)` into a
    /// warehouse under `root`, then publish it to `datasets_dir/rel`.
    ///
    /// Returns the fixture, so a test can keep writing to the table.
    pub async fn create(root: &Path, datasets_dir: &Path, rel: &str) -> Self {
        let warehouse = root.join("iceberg-warehouse");
        std::fs::create_dir_all(&warehouse).expect("create warehouse");

        let catalog: Arc<MemoryCatalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "fixture",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        warehouse.to_string_lossy().to_string(),
                    )]),
                )
                .await
                .expect("memory catalog should load"),
        );

        let namespace = NamespaceIdent::new(NAMESPACE.to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "value", Type::Primitive(PrimitiveType::Double)).into(),
            ])
            .build()
            .expect("fixture schema should build");

        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(TABLE.to_string())
                    .schema(schema)
                    .build(),
            )
            .await
            .expect("create table");

        let fixture = Self {
            catalog,
            warehouse,
            published: datasets_dir.join(rel),
            ident: TableIdent::new(namespace, TABLE.to_string()),
        };

        fixture
            .insert(
                "VALUES (1, 'argo', 12.5), (2, 'glider', 9.0), (3, 'argo', 7.0), (4, 'buoy', 21.0)",
            )
            .await;
        fixture.publish();
        fixture
    }

    /// The table as Beacon addresses it: a location under the datasets root.
    pub fn location(rel: &str) -> String {
        format!("datasets://{rel}")
    }

    /// Append rows. `values` is everything after the table name, so it can name
    /// columns: `(id, name) VALUES (7, 'x')`.
    ///
    /// The insert runs through iceberg-datafusion against the fixture's own
    /// catalog — nothing here goes through Beacon.
    pub async fn insert(&self, values: &str) {
        let ctx = SessionContext::new();
        ctx.register_catalog(
            "fixture",
            Arc::new(
                IcebergCatalogProvider::try_new(self.catalog.clone() as Arc<dyn Catalog>)
                    .await
                    .expect("iceberg catalog provider"),
            ),
        );
        ctx.sql(&format!("INSERT INTO fixture.{NAMESPACE}.{TABLE} {values}"))
            .await
            .expect("insert should plan")
            .collect()
            .await
            .expect("insert should run");
    }

    /// Add a nullable column, the way another writer evolves a schema.
    pub async fn add_column(&self, name: &str) {
        let table = self.load().await;
        let transaction = Transaction::new(&table);
        let transaction = transaction
            .update_schema()
            .add_column(AddColumn::optional(
                name,
                Type::Primitive(PrimitiveType::Int),
            ))
            .apply(transaction)
            .expect("schema update should apply");
        transaction
            .commit(self.catalog.as_ref() as &dyn Catalog)
            .await
            .expect("schema update should commit");
    }

    async fn load(&self) -> Table {
        self.catalog
            .load_table(&self.ident)
            .await
            .expect("load fixture table")
    }

    /// Copy the warehouse copy of the table over the published one, so Beacon
    /// sees whatever the fixture has written since the last call.
    pub fn publish(&self) {
        copy_dir(&self.warehouse.join(NAMESPACE).join(TABLE), &self.published);
    }
}

/// Recursively copy `from` onto `to`, leaving files already there in place.
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create published dir");
    for entry in std::fs::read_dir(from).expect("read fixture dir") {
        let entry = entry.expect("read fixture entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy fixture file");
        }
    }
}
