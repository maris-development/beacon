//! Schema evolution for managed Lance tables (`ALTER TABLE`).
//!
//! Maps beacon's `ALTER TABLE` operations onto Lance's native schema-evolution
//! API: `add_columns` (all-null new columns), `drop_columns`, and `alter_columns`
//! (rename / cast). Each change is its own atomic dataset version, so existing
//! data is preserved without a table rebuild — unlike the Iceberg path.

use std::sync::Arc;

use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::error::ArrowError;
use beacon_datafusion_ext::type_widening::{
    ArrowTypeWideningStrategy, DefaultArrowTypeWidening, LabeledSchema,
};
use lance::dataset::{ColumnAlteration, NewColumnTransform};
use lance::index::DatasetIndexExt;
use lance_file::version::ConcreteFileVersion;

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

    let mut dataset = crate::io::dataset_builder(uri, warehouse.session())
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
    let widening = DefaultArrowTypeWidening::new();
    widen_table(warehouse, uri, fields, &[], &widening).await
}

/// Add the `new_columns` that the Lance table at `uri` does not have yet, then
/// widen the columns of `widen_sources` (column name and source type). Returns
/// the added names.
///
/// The latest version is read under the write lock. A column that another
/// insert added already is skipped. The new type of a column is the `widening`
/// merge of its current type and the source type, so a column that another
/// insert widened is never narrowed. Every type change is checked before the
/// first change: a column with an index, or a type change that Lance refuses,
/// gives an error and the table stays as it is. When another process changes
/// the dataset at the same time, the dataset is read again once.
pub async fn widen_table(
    warehouse: &LanceWarehouse,
    uri: &str,
    new_columns: &[Field],
    widen_sources: &[(String, DataType)],
    widening: &dyn ArrowTypeWideningStrategy,
) -> anyhow::Result<Vec<String>> {
    let lock = warehouse.lock(uri);
    let _guard = lock.lock().await;

    let mut retried = false;
    loop {
        let mut dataset = crate::io::dataset_builder(uri, warehouse.session())
            .load()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open Lance dataset '{uri}': {e}"))?;
        let missing: Vec<Field> = new_columns
            .iter()
            .filter(|field| dataset.schema().field(field.name()).is_none())
            .map(|field| Field::new(field.name(), lance_compatible_type(field.data_type()), true))
            .collect();
        let casts = pending_type_changes(&dataset, widen_sources, widening).await?;
        if missing.is_empty() && casts.is_empty() {
            return Ok(Vec::new());
        }
        let names: Vec<String> = missing.iter().map(|f| f.name().clone()).collect();
        let changed: Vec<String> = casts.iter().map(|c| c.path.clone()).collect();

        let result = async {
            if !missing.is_empty() {
                tracing::info!(uri = %uri, columns = ?names, "adding Lance columns");
                let schema = Arc::new(ArrowSchema::new(missing));
                dataset
                    .add_columns(NewColumnTransform::AllNulls(schema), None, None)
                    .await
                    .map_err(|e| (format!("add the columns {names:?} to"), e))?;
            }
            if !casts.is_empty() {
                tracing::info!(uri = %uri, columns = ?changed, "changing Lance column types");
                dataset
                    .alter_columns(&casts)
                    .await
                    .map_err(|e| (format!("change the types of the columns {changed:?} of"), e))?;
            }
            Ok::<_, (String, lance::Error)>(())
        }
        .await;
        match result {
            Ok(()) => return Ok(names),
            Err((_, e)) if !retried && is_concurrent_change(&e) => retried = true,
            Err((action, e)) => anyhow::bail!("Failed to {action} '{uri}': {e}"),
        }
    }
}

/// The alterations that `dataset` needs for `widen_sources`. Errors when a
/// changing column has an index or Lance refuses the new type.
async fn pending_type_changes(
    dataset: &lance::Dataset,
    widen_sources: &[(String, DataType)],
    widening: &dyn ArrowTypeWideningStrategy,
) -> anyhow::Result<Vec<ColumnAlteration>> {
    let version = dataset.manifest().data_storage_format.lance_file_format();
    let mut pending = Vec::new();
    for (name, source_type) in widen_sources {
        let field = dataset
            .schema()
            .field(name)
            .ok_or_else(|| anyhow::anyhow!("The Lance table does not have the column '{name}'"))?;
        let current = field.data_type();
        let one = |data_type: &DataType| {
            LabeledSchema::unlabeled(Arc::new(ArrowSchema::new(vec![Field::new(
                name,
                data_type.clone(),
                true,
            )])))
        };
        let merged = widening
            .merge_schemas(&[one(&current), one(source_type)])
            .map_err(|error| match error {
                ArrowError::SchemaError(message) => anyhow::anyhow!(message),
                other => anyhow::anyhow!(other),
            })?;
        let data_type = lance_compatible_type(merged.field(0).data_type());
        if current.equals_datatype(&data_type) {
            continue;
        }
        if !lance_accepts_cast(&current, &data_type, version) {
            anyhow::bail!("Lance cannot change column '{name}' from {current} to {data_type}");
        }
        pending.push((field.id, name, data_type));
    }
    if pending.is_empty() {
        return Ok(Vec::new());
    }

    let indices = dataset
        .load_indices()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load the indexes: {e}"))?;
    for (id, name, data_type) in &pending {
        if let Some(index) = indices.iter().find(|index| index.fields.contains(id)) {
            anyhow::bail!(
                "Column '{name}' has the index '{}'; drop it before BY NAME changes the type to {data_type}",
                index.name
            );
        }
    }
    Ok(pending
        .into_iter()
        .map(|(_, name, data_type)| ColumnAlteration::new(name.clone()).cast_to(data_type))
        .collect())
}

/// `true` when Lance `alter_columns` accepts a cast from `from` to `to` on a
/// dataset with the data file `version`. The same rule as in Lance.
fn lance_accepts_cast(from: &DataType, to: &DataType, version: ConcreteFileVersion) -> bool {
    can_cast_types(from, to)
        && is_upcast_downcast(from, to, !matches!(version, ConcreteFileVersion::V1))
}

/// The type pairs that Lance can rewrite: one family for each pair.
/// Copy of `lance::dataset::versions::is_upcast_downcast` (Lance 11.0.0).
fn is_upcast_downcast(from: &DataType, to: &DataType, dictionary_materialization: bool) -> bool {
    use DataType::*;
    match (from, to) {
        (_, Dictionary(_, _)) if !dictionary_materialization => false,
        (Dictionary(_, from_value), _) => {
            is_upcast_downcast(from_value, to, dictionary_materialization)
        }
        (_, Dictionary(_, to_value)) => {
            is_upcast_downcast(from, to_value, dictionary_materialization)
        }
        (from, to) if from.is_integer() => to.is_integer(),
        (from, to) if from.is_floating() => to.is_floating(),
        (from, to) if from.is_temporal() => to.is_temporal(),
        (Boolean, to) => matches!(to, Boolean),
        (Utf8 | LargeUtf8, to) => matches!(to, Utf8 | LargeUtf8),
        (Binary | LargeBinary, to) => matches!(to, Binary | LargeBinary),
        (Decimal128(_, _) | Decimal256(_, _), to) => {
            matches!(to, Decimal128(_, _) | Decimal256(_, _))
        }
        (List(from_field) | LargeList(from_field) | FixedSizeList(from_field, _), to) => match to {
            List(to_field) | LargeList(to_field) | FixedSizeList(to_field, _) => {
                is_upcast_downcast(
                    from_field.data_type(),
                    to_field.data_type(),
                    dictionary_materialization,
                )
            }
            _ => false,
        },
        _ => false,
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
        let dataset = crate::io::dataset_builder(&uri, warehouse.session())
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

    const DEFAULT: DefaultArrowTypeWidening = DefaultArrowTypeWidening::new();

    #[tokio::test]
    async fn widen_table_adds_then_changes_types() {
        let warehouse = LanceWarehouse::new(Arc::new(InMemory::new()));
        let uri = warehouse.table_uri(&crate::beacon_namespace(), "t");
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int64, true),
        ]));
        crate::io::write_stream(
            &uri,
            warehouse.session(),
            crate::io::empty_stream(schema),
            crate::io::WriteKind::Create,
        )
        .await
        .unwrap();
        let open = || async {
            crate::io::dataset_builder(&uri, warehouse.session())
                .load()
                .await
                .unwrap()
        };

        let refused = widen_table(
            &warehouse,
            &uri,
            &[Field::new("x", DataType::Utf8, true)],
            &[
                ("a".to_string(), DataType::Int64),
                ("b".to_string(), DataType::Float64),
            ],
            &DEFAULT,
        )
        .await
        .unwrap_err();
        assert_eq!(
            refused.to_string(),
            "Lance cannot change column 'b' from Int64 to Float64"
        );
        assert_eq!(open().await.schema().fields.len(), 2, "nothing changed");

        let added = widen_table(
            &warehouse,
            &uri,
            &[Field::new("x", DataType::Utf8, true)],
            &[("a".to_string(), DataType::Int64)],
            &DEFAULT,
        )
        .await
        .unwrap();
        assert_eq!(added, vec!["x".to_string()]);
        let dataset = open().await;
        assert_eq!(
            dataset.schema().field("a").unwrap().data_type(),
            DataType::Int64
        );
        assert!(dataset.schema().field("x").is_some());

        // A narrower source after another widening does not narrow the column.
        let version = dataset.version().version;
        let added = widen_table(
            &warehouse,
            &uri,
            &[],
            &[("a".to_string(), DataType::Int16)],
            &DEFAULT,
        )
        .await
        .unwrap();
        assert!(added.is_empty());
        let dataset = open().await;
        assert_eq!(
            dataset.schema().field("a").unwrap().data_type(),
            DataType::Int64
        );
        assert_eq!(dataset.version().version, version, "no new version");
    }
}
