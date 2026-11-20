//! Schema evolution for managed Lance tables (`ALTER TABLE`).
//!
//! Maps beacon's `ALTER TABLE` operations onto Lance's native schema-evolution
//! API: `add_columns` (all-null new columns), `drop_columns`, and `alter_columns`
//! (rename / cast). Each change is its own atomic dataset version, so existing
//! data is preserved without a table rebuild — unlike the Iceberg path.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::{ColumnAlteration, NewColumnTransform};

use crate::io::lance_compatible_type;
use crate::warehouse::LanceWarehouse;

/// A single schema change to apply to a Lance table.
#[derive(Debug, Clone)]
pub enum SchemaChange {
    /// Add a new, initially all-null column.
    AddColumn { name: String, data_type: DataType },
    /// Drop an existing column.
    DropColumn { name: String },
    /// Rename a column (values preserved).
    RenameColumn { from: String, to: String },
    /// Cast a column to a new data type.
    AlterColumnType { name: String, data_type: DataType },
}

/// Apply `changes` in order to the Lance table at `uri`. Serialized against
/// concurrent writers via the warehouse's per-dataset lock.
pub async fn alter_table(
    warehouse: &LanceWarehouse,
    uri: &str,
    changes: &[SchemaChange],
) -> anyhow::Result<()> {
    tracing::info!(uri = %uri, changes = changes.len(), "altering Lance table");

    let lock = warehouse.lock(uri);
    let _guard = lock.lock().await;

    let mut dataset = DatasetBuilder::from_uri(uri)
        .with_session(warehouse.session())
        .load()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open Lance dataset '{uri}': {e}"))?;

    for change in changes {
        match change {
            SchemaChange::AddColumn { name, data_type } => {
                // New columns are nullable so existing rows read NULL. Widen Arrow
                // "view" types (Utf8View/BinaryView from DataFusion) to the
                // non-view types Lance can store, matching the write path.
                let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                    name,
                    lance_compatible_type(data_type),
                    true,
                )]));
                dataset
                    .add_columns(NewColumnTransform::AllNulls(schema), None, None)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to add column '{name}': {e}"))?;
            }
            SchemaChange::DropColumn { name } => {
                dataset
                    .drop_columns(&[name.as_str()])
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to drop column '{name}': {e}"))?;
            }
            SchemaChange::RenameColumn { from, to } => {
                let alteration = ColumnAlteration::new(from.clone()).rename(to.clone());
                dataset
                    .alter_columns(&[alteration])
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to rename column '{from}': {e}"))?;
            }
            SchemaChange::AlterColumnType { name, data_type } => {
                let alteration =
                    ColumnAlteration::new(name.clone()).cast_to(lance_compatible_type(data_type));
                dataset
                    .alter_columns(&[alteration])
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to alter column type '{name}': {e}"))?;
            }
        }
    }

    Ok(())
}

/// Add the `fields` that the Lance table at `uri` does not have yet, as
/// nullable all-NULL columns in one dataset version. Returns the added names.
///
/// The latest version is read under the write lock, so a field that another
/// insert added is skipped. When another process changes the dataset at the
/// same time, the dataset is read again once.
pub async fn add_missing_columns(
    warehouse: &LanceWarehouse,
    uri: &str,
    fields: &[Field],
) -> anyhow::Result<Vec<String>> {
    let lock = warehouse.lock(uri);
    let _guard = lock.lock().await;

    let mut retried = false;
    loop {
        let mut dataset = DatasetBuilder::from_uri(uri)
            .with_session(warehouse.session())
            .load()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open Lance dataset '{uri}': {e}"))?;
        let missing: Vec<Field> = fields
            .iter()
            .filter(|field| {
                !dataset
                    .schema()
                    .fields
                    .iter()
                    .any(|existing| &existing.name == field.name())
            })
            .map(|field| Field::new(field.name(), lance_compatible_type(field.data_type()), true))
            .collect();
        if missing.is_empty() {
            return Ok(Vec::new());
        }
        let names: Vec<String> = missing.iter().map(|f| f.name().clone()).collect();
        tracing::info!(uri = %uri, columns = ?names, "adding Lance columns");

        let schema = Arc::new(ArrowSchema::new(missing));
        match dataset
            .add_columns(NewColumnTransform::AllNulls(schema), None, None)
            .await
        {
            Ok(()) => return Ok(names),
            Err(e) if !retried && is_concurrent_change(&e) => retried = true,
            Err(e) => {
                anyhow::bail!("Failed to add the columns {names:?} to '{uri}': {e}")
            }
        }
    }
}

/// `true` when another writer changed the dataset during `add_columns`.
fn is_concurrent_change(error: &lance::Error) -> bool {
    match error {
        lance::Error::CommitConflict { .. }
        | lance::Error::RetryableCommitConflict { .. }
        | lance::Error::IncompatibleTransaction { .. } => true,
        // Lance gives a column that exists only as an invalid-input message.
        other => {
            let message = other.to_string();
            message.contains("already exists") || message.contains("Type conflicts")
        }
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn add_missing_columns_skips_existing() {
        let warehouse = LanceWarehouse::new(Arc::new(InMemory::new()));
        let uri = warehouse.table_uri(&crate::beacon_namespace(), "t");
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "a",
            DataType::Int64,
            true,
        )]));
        crate::io::write_stream(
            &uri,
            warehouse.session(),
            crate::io::empty_stream(schema),
            crate::io::WriteKind::Create,
        )
        .await
        .unwrap();

        let x = [Field::new("x", DataType::Float64, true)];
        let first = add_missing_columns(&warehouse, &uri, &x).await.unwrap();
        let second = add_missing_columns(&warehouse, &uri, &x).await.unwrap();

        assert_eq!(first, vec!["x".to_string()]);
        assert!(second.is_empty());
        let dataset = DatasetBuilder::from_uri(&uri)
            .with_session(warehouse.session())
            .load()
            .await
            .unwrap();
        let names: Vec<&str> = dataset
            .schema()
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(names, vec!["a", "x"]);
    }
}
