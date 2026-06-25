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
                // New columns are nullable so existing rows read NULL.
                let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                    name,
                    data_type.clone(),
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
                let alteration = ColumnAlteration::new(name.clone()).cast_to(data_type.clone());
                dataset
                    .alter_columns(&[alteration])
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to alter column type '{name}': {e}"))?;
            }
        }
    }

    Ok(())
}
