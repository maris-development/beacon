//! Iceberg schema evolution (`ALTER TABLE`) via table rebuild.
//!
//! iceberg-rust 0.10 resolves a table's schema from its *current snapshot*, and
//! every new snapshot inherits the prior snapshot's schema-id — so a schema
//! change committed as metadata is never surfaced by the reader once a table has
//! data. To make `ALTER` take effect we **rebuild** the table: stage the
//! transformed rows in a temp table, drop the original, recreate it under the new
//! schema, and copy the data back. The recreated table's first snapshot then
//! references the new schema.
//!
//! This is a full rewrite per `ALTER` and does not preserve snapshot history.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use datafusion::catalog::TableProvider;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, cast, col, lit};
use datafusion::prelude::SessionContext;
use datafusion_iceberg::DataFusionTable;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::spec::types::{PrimitiveType, Type};
use object_store::ObjectStore;

use crate::{create_iceberg_table, drop_iceberg_table};

/// An engine-agnostic schema change. Beacon's statement handler maps parsed
/// `ALTER TABLE` operations to these; column types are Arrow types.
#[derive(Debug, Clone)]
pub enum SchemaChange {
    /// Add a new (nullable) column.
    AddColumn {
        name: String,
        data_type: ArrowDataType,
    },
    /// Remove a column by name.
    DropColumn { name: String },
    /// Rename a column.
    RenameColumn { from: String, to: String },
    /// Change a column's type (only Iceberg-legal promotions are allowed).
    AlterColumnType {
        name: String,
        data_type: ArrowDataType,
    },
}

/// Apply `changes` to an Iceberg table by rebuilding it under the new schema.
/// `store` is the warehouse object store, used to delete table directories.
pub async fn alter_table_schema(
    catalog: &Arc<dyn Catalog>,
    store: &Arc<dyn ObjectStore>,
    namespace: &[String],
    name: &str,
    changes: &[SchemaChange],
) -> anyhow::Result<()> {
    // Provider over the current table; its Arrow schema is the source shape.
    let original = load_provider(catalog, namespace, name).await?;
    let original_schema = original.schema();
    let original_names: Vec<String> = original_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();

    // Validate the changes and compute the post-change Arrow schema.
    let new_schema = apply_changes_to_schema(&original_schema, changes)?;
    // Projection mapping current columns -> new columns.
    let transform = build_transform_exprs(&original_names, changes);

    // Stage the transformed rows in a temp table (so the original is untouched
    // until the data is safely written).
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let temp_name = format!("{name}__altertmp_{suffix}");

    let staged = async {
        let temp = create_iceberg_table(catalog, namespace, &temp_name, &new_schema).await?;
        let ctx = SessionContext::new();
        ctx.register_table(name, Arc::new(original))
            .map_err(|e| anyhow::anyhow!("register source failed: {e}"))?;
        let temp_provider: Arc<dyn TableProvider> = Arc::new(temp);
        rewrite_into(&ctx, name, &temp_provider, Some(transform)).await?;
        anyhow::Ok(())
    }
    .await;

    if let Err(error) = staged {
        // Best-effort cleanup of a partial temp table.
        let _ = drop_iceberg_table(store, namespace, &temp_name).await;
        return Err(error);
    }

    // Swap: drop the original, recreate it under the new schema, copy temp back.
    drop_iceberg_table(store, namespace, name).await?;
    let dest = create_iceberg_table(catalog, namespace, name, &new_schema).await?;

    let temp_reloaded = load_provider(catalog, namespace, &temp_name).await?;
    let ctx = SessionContext::new();
    ctx.register_table(&temp_name, Arc::new(temp_reloaded))
        .map_err(|e| anyhow::anyhow!("register temp failed: {e}"))?;
    let dest_provider: Arc<dyn TableProvider> = Arc::new(dest);
    rewrite_into(&ctx, &temp_name, &dest_provider, None).await?;

    // Clean up the temp table.
    drop_iceberg_table(store, namespace, &temp_name).await?;

    Ok(())
}

/// Load a table from the catalog as a DataFusion provider.
async fn load_provider(
    catalog: &Arc<dyn Catalog>,
    namespace: &[String],
    name: &str,
) -> anyhow::Result<DataFusionTable> {
    use iceberg_rust::catalog::identifier::Identifier;
    use iceberg_rust::catalog::tabular::Tabular;

    let tabular = catalog
        .clone()
        .load_tabular(&Identifier::new(namespace, name))
        .await
        .map_err(|error| anyhow::anyhow!("Failed to load Iceberg table '{name}': {error}"))?;
    match tabular {
        Tabular::Table(table) => Ok(DataFusionTable::from(table)),
        _ => anyhow::bail!("Iceberg identifier '{name}' does not refer to a table"),
    }
}

/// Read `source` (optionally projected via `exprs`) and append into `dest`.
async fn rewrite_into(
    ctx: &SessionContext,
    source: &str,
    dest: &Arc<dyn TableProvider>,
    exprs: Option<Vec<Expr>>,
) -> anyhow::Result<()> {
    let df = ctx
        .table(source)
        .await
        .map_err(|e| anyhow::anyhow!("open '{source}' failed: {e}"))?;
    let df = match exprs {
        Some(exprs) => df
            .select(exprs)
            .map_err(|e| anyhow::anyhow!("projection failed: {e}"))?,
        None => df,
    };
    let state = ctx.state();
    let physical = state
        .create_physical_plan(df.logical_plan())
        .await
        .map_err(|e| anyhow::anyhow!("plan failed: {e}"))?;
    let insert = dest
        .insert_into(&state, physical, InsertOp::Append)
        .await
        .map_err(|e| anyhow::anyhow!("insert plan failed: {e}"))?;
    datafusion::physical_plan::collect(insert, ctx.task_ctx())
        .await
        .map_err(|e| anyhow::anyhow!("rewrite failed: {e}"))?;
    Ok(())
}

/// Validate `changes` against `schema` and return the resulting Arrow schema.
fn apply_changes_to_schema(
    schema: &ArrowSchema,
    changes: &[SchemaChange],
) -> anyhow::Result<ArrowSchema> {
    // (name, type, nullable)
    let mut cols: Vec<(String, ArrowDataType, bool)> = schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
        .collect();
    let position = |cols: &[(String, ArrowDataType, bool)], name: &str| {
        cols.iter().position(|(n, _, _)| n == name)
    };

    for change in changes {
        match change {
            SchemaChange::AddColumn { name, data_type } => {
                if position(&cols, name).is_some() {
                    anyhow::bail!("Cannot add column '{name}': it already exists");
                }
                // Added columns must be nullable: existing rows have no value.
                cols.push((name.clone(), data_type.clone(), true));
            }
            SchemaChange::DropColumn { name } => {
                let idx = position(&cols, name)
                    .ok_or_else(|| anyhow::anyhow!("Cannot drop column '{name}': not found"))?;
                cols.remove(idx);
            }
            SchemaChange::RenameColumn { from, to } => {
                if position(&cols, to).is_some() {
                    anyhow::bail!("Cannot rename to '{to}': a column with that name exists");
                }
                let idx = position(&cols, from)
                    .ok_or_else(|| anyhow::anyhow!("Cannot rename column '{from}': not found"))?;
                cols[idx].0 = to.clone();
            }
            SchemaChange::AlterColumnType { name, data_type } => {
                let idx = position(&cols, name)
                    .ok_or_else(|| anyhow::anyhow!("Cannot alter column '{name}': not found"))?;
                let old_iceberg = arrow_to_iceberg_type(&cols[idx].1)?;
                let new_iceberg = arrow_to_iceberg_type(data_type)?;
                if !is_allowed_promotion(&old_iceberg, &new_iceberg) {
                    anyhow::bail!(
                        "Cannot change type of column '{name}': only safe promotions are allowed \
                         (int->bigint, float->double, decimal widening)"
                    );
                }
                cols[idx].1 = data_type.clone();
            }
        }
    }

    Ok(ArrowSchema::new(
        cols.into_iter()
            .map(|(name, data_type, nullable)| Field::new(name, data_type, nullable))
            .collect::<Vec<_>>(),
    ))
}

/// Build the projection mapping current columns to post-change columns: existing
/// columns pass through (renamed/cast as needed), added columns are typed NULLs,
/// dropped columns are omitted. Output order matches the evolved schema.
fn build_transform_exprs(original_names: &[String], changes: &[SchemaChange]) -> Vec<Expr> {
    let mut entries: Vec<(String, Expr)> = original_names
        .iter()
        .map(|name| (name.clone(), col(name)))
        .collect();

    for change in changes {
        match change {
            SchemaChange::AddColumn { name, data_type } => {
                entries.push((
                    name.clone(),
                    cast(lit(ScalarValue::Null), data_type.clone()),
                ));
            }
            SchemaChange::DropColumn { name } => {
                entries.retain(|(out_name, _)| out_name != name);
            }
            SchemaChange::RenameColumn { from, to } => {
                if let Some(entry) = entries.iter_mut().find(|(out_name, _)| out_name == from) {
                    entry.0 = to.clone();
                }
            }
            SchemaChange::AlterColumnType { name, data_type } => {
                if let Some(entry) = entries.iter_mut().find(|(out_name, _)| out_name == name) {
                    entry.1 = cast(entry.1.clone(), data_type.clone());
                }
            }
        }
    }

    entries
        .into_iter()
        .map(|(name, expr)| expr.alias(name))
        .collect()
}

/// Convert an Arrow type to an Iceberg field type.
fn arrow_to_iceberg_type(data_type: &ArrowDataType) -> anyhow::Result<Type> {
    Type::try_from(data_type)
        .map_err(|error| anyhow::anyhow!("Unsupported column type {data_type}: {error}"))
}

/// Returns true if changing a column from `old` to `new` is an Iceberg-legal
/// promotion (or a no-op): `int->long`, `float->double`, decimal precision
/// increase at the same scale.
pub fn is_allowed_promotion(old: &Type, new: &Type) -> bool {
    match (old, new) {
        (Type::Primitive(a), Type::Primitive(b)) => match (a, b) {
            (PrimitiveType::Int, PrimitiveType::Long) => true,
            (PrimitiveType::Float, PrimitiveType::Double) => true,
            (
                PrimitiveType::Decimal {
                    precision: p_old,
                    scale: s_old,
                },
                PrimitiveType::Decimal {
                    precision: p_new,
                    scale: s_new,
                },
            ) => s_old == s_new && p_new >= p_old,
            (a, b) => a == b,
        },
        _ => old == new,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn promotion_rules() {
        let int = Type::Primitive(PrimitiveType::Int);
        let long = Type::Primitive(PrimitiveType::Long);
        let float = Type::Primitive(PrimitiveType::Float);
        let double = Type::Primitive(PrimitiveType::Double);

        assert!(is_allowed_promotion(&int, &long));
        assert!(is_allowed_promotion(&float, &double));
        assert!(is_allowed_promotion(&int, &int));
        assert!(!is_allowed_promotion(&long, &int));
        assert!(!is_allowed_promotion(&int, &double));

        let dec_5_2 = Type::Primitive(PrimitiveType::Decimal {
            precision: 5,
            scale: 2,
        });
        let dec_8_2 = Type::Primitive(PrimitiveType::Decimal {
            precision: 8,
            scale: 2,
        });
        let dec_8_3 = Type::Primitive(PrimitiveType::Decimal {
            precision: 8,
            scale: 3,
        });
        assert!(is_allowed_promotion(&dec_5_2, &dec_8_2));
        assert!(!is_allowed_promotion(&dec_8_2, &dec_5_2));
        assert!(!is_allowed_promotion(&dec_5_2, &dec_8_3));
    }
}
