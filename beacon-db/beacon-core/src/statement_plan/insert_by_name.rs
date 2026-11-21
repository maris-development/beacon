//! Column mapping for `INSERT INTO t BY NAME <query>`.
//!
//! [`map_by_name`] matches the source columns to the target columns by exact,
//! case-sensitive name. [`by_name_projection`] turns the mapping into a
//! projection that gives rows with the target schema. [`insert_by_name`] runs
//! the insert.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use anyhow::{anyhow, bail};
use arrow::compute::{can_cast_types, CastOptions};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use arrow::error::ArrowError;
use beacon_datafusion_ext::table_ext::TableDefinition;
use beacon_datafusion_ext::type_widening::{
    session_widening, ArrowTypeWideningStrategy, LabeledSchema,
};
use datafusion::common::ScalarValue;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;

/// The label of the source schema in a widening rule error.
const SOURCE_LABEL: &str = "source query";

/// The value of one target column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ColumnSource {
    /// The source column at `index`, with the target type.
    Source { index: usize },
    /// The source column at `index`, cast to `to`. A `lenient` cast gives NULL
    /// for a value that does not cast; any other cast gives an error.
    Cast {
        index: usize,
        to: DataType,
        lenient: bool,
    },
    /// The source does not have the column: NULL of type `to`.
    Null { to: DataType },
}

/// How the source columns of a `BY NAME` insert fill the target columns.
#[derive(Debug, Clone)]
pub(crate) struct ByNameMapping {
    /// Source columns that the target does not have, in source order.
    /// The caller adds them at the end of the target.
    pub new_columns: Vec<Field>,
    /// Target columns that get a wider type: the column name and the new type.
    /// The caller changes them after it adds `new_columns`.
    pub type_changes: Vec<(String, DataType)>,
    /// One entry for each column of the widened target, in target order.
    pub columns: Vec<ColumnSource>,
    target_names: Vec<String>,
    target_types: Vec<DataType>,
    source_types: Vec<DataType>,
}

/// The Lance column name for the source column `name`: each `.` becomes `_`.
pub fn lance_column_name(name: &str) -> String {
    name.replace('.', "_")
}

/// `schema` as a Lance table sees it: each name changed by
/// [`lance_column_name`] and each type by `lance_compatible_type`.
pub(crate) fn lance_source_schema(schema: &Schema) -> Schema {
    let fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .map(|f| {
            Arc::new(
                f.as_ref()
                    .clone()
                    .with_name(lance_column_name(f.name()))
                    .with_data_type(beacon_lance::lance_compatible_type(f.data_type())),
            )
        })
        .collect();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

/// Map the `source` columns to the `target` columns by name.
///
/// With `can_widen` (a Lance target), the source first becomes its
/// [`lance_source_schema`]. Source columns that `target` does not have then go
/// into [`ByNameMapping::new_columns`], and a matched column whose `widening`
/// type is wider than the target type goes into
/// [`ByNameMapping::type_changes`]. Without `can_widen`, extra source columns
/// give an error that names `table`, and every cast goes to the target type.
pub(crate) fn map_by_name(
    source: &Schema,
    target: &Schema,
    can_widen: bool,
    table: &str,
    widening: &dyn ArrowTypeWideningStrategy,
) -> anyhow::Result<ByNameMapping> {
    let renamed;
    let source = if can_widen {
        renamed = lance_source_schema(source);
        &renamed
    } else {
        source
    };
    let mut source_index = HashMap::with_capacity(source.fields().len());
    for (index, field) in source.fields().iter().enumerate() {
        if source_index.insert(field.name().as_str(), index).is_some() {
            bail!(
                "The source has the column '{}' more than one time",
                field.name()
            );
        }
    }

    let target_names: HashSet<&str> = target.fields().iter().map(|f| f.name().as_str()).collect();
    let extra: Vec<&FieldRef> = source
        .fields()
        .iter()
        .filter(|f| !target_names.contains(f.name().as_str()))
        .collect();
    if !extra.is_empty() && !can_widen {
        bail!(
            "Table '{table}' does not have the columns {}; only Lance tables add columns",
            extra
                .iter()
                .map(|f| format!("'{}'", f.name()))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let new_columns: Vec<Field> = extra
        .iter()
        .map(|f| {
            Field::new(
                f.name(),
                beacon_lance::lance_compatible_type(f.data_type()),
                true,
            )
        })
        .collect();

    // A type pair that cannot cast stops the statement before the widening rule.
    let mut matched_target = Vec::new();
    let mut matched_source = Vec::new();
    for field in target.fields() {
        if let Some(&index) = source_index.get(field.name().as_str()) {
            let from = source.field(index).data_type();
            let to = field.data_type();
            if from != to && !can_cast_types(from, to) {
                bail!("Column '{}': cannot cast {from} to {to}", field.name());
            }
            matched_target.push(Arc::clone(field));
            matched_source.push(Arc::new(Field::new(field.name(), from.clone(), true)));
        }
    }
    let merged = widening
        .merge_schemas(&[
            LabeledSchema::new(Arc::new(Schema::new(matched_target)), table),
            LabeledSchema::new(Arc::new(Schema::new(matched_source)), SOURCE_LABEL),
        ])
        .map_err(|error| match error {
            ArrowError::SchemaError(message) => anyhow!(message),
            other => anyhow!(other),
        })?;

    let widened: Vec<&Field> = target
        .fields()
        .iter()
        .map(|f| f.as_ref())
        .chain(new_columns.iter())
        .collect();

    let mut type_changes = Vec::new();
    let mut columns = Vec::with_capacity(widened.len());
    for field in &widened {
        let target_type = field.data_type();
        let column = match source_index.get(field.name().as_str()) {
            None => ColumnSource::Null {
                to: target_type.clone(),
            },
            Some(&index) => {
                let from = source.field(index).data_type();
                let widened_type = match merged.field_with_name(field.name()) {
                    Ok(merged) if can_widen => {
                        Some(beacon_lance::lance_compatible_type(merged.data_type()))
                    }
                    Ok(merged) => Some(merged.data_type().clone()),
                    // A new column: the merge did not see it.
                    Err(_) => None,
                };
                let (to, lenient) = match widened_type {
                    None => (target_type.clone(), false),
                    Some(widened) if widened.equals_datatype(target_type) => (
                        target_type.clone(),
                        widening.casts_leniently(from, target_type),
                    ),
                    Some(widened) if can_widen => {
                        type_changes.push((field.name().clone(), widened.clone()));
                        (widened, false)
                    }
                    Some(_) => (target_type.clone(), false),
                };
                if from == &to {
                    ColumnSource::Source { index }
                } else if can_cast_types(from, &to) {
                    ColumnSource::Cast { index, to, lenient }
                } else {
                    bail!("Column '{}': cannot cast {from} to {to}", field.name());
                }
            }
        };
        columns.push(column);
    }

    for (field, column) in widened.iter().zip(&columns) {
        if !field.is_nullable() && matches!(column, ColumnSource::Null { .. }) {
            bail!(
                "Column '{}' is not nullable and the source does not have it",
                field.name()
            );
        }
    }

    let target_names = widened.iter().map(|f| f.name().clone()).collect();
    let target_types = widened.iter().map(|f| f.data_type().clone()).collect();
    Ok(ByNameMapping {
        new_columns,
        type_changes,
        columns,
        target_names,
        target_types,
        source_types: source
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect(),
    })
}

/// Project `child` to `target` with `mapping`.
///
/// `target` is the widened target schema. The output has its field names,
/// types and metadata. A value that does not cast gives an error.
///
/// An output field is nullable when the source column or the target field is
/// nullable. A nullable source column on a non-nullable target column keeps
/// its NULL values. The insert must reject them.
pub(crate) fn by_name_projection(
    child: Arc<dyn ExecutionPlan>,
    mapping: &ByNameMapping,
    target: SchemaRef,
) -> anyhow::Result<Arc<dyn ExecutionPlan>> {
    if mapping.columns.len() != target.fields().len() {
        bail!(
            "The BY NAME mapping has {} columns, but the target has {}",
            mapping.columns.len(),
            target.fields().len()
        );
    }
    let input = child.schema();
    let mut exprs = Vec::with_capacity(target.fields().len());
    for (column, target_field) in mapping.columns.iter().zip(target.fields()) {
        let lenient = matches!(column, ColumnSource::Cast { lenient: true, .. });
        let (expr, produced): (Arc<dyn PhysicalExpr>, FieldRef) = match column {
            ColumnSource::Source { index } | ColumnSource::Cast { index, .. } => {
                let field = input
                    .fields()
                    .get(*index)
                    .ok_or_else(|| anyhow!("The source does not have a column at index {index}"))?;
                (
                    Arc::new(Column::new(field.name(), *index)),
                    Arc::clone(field),
                )
            }
            ColumnSource::Null { to } => {
                let literal = Literal::new(ScalarValue::try_from(to)?);
                let field = literal.return_field(&input)?;
                (Arc::new(literal), field)
            }
        };
        exprs.push(ProjectionExpr {
            expr: fit_to_field(expr, &produced, target_field, lenient),
            alias: target_field.name().clone(),
        });
    }
    Ok(Arc::new(ProjectionExec::try_new(exprs, child)?))
}

/// Wrap `expr` in a cast unless it already gives `target`. A `lenient` cast
/// gives NULL for a value that does not cast; any other cast is strict.
fn fit_to_field(
    expr: Arc<dyn PhysicalExpr>,
    produced: &FieldRef,
    target: &FieldRef,
    lenient: bool,
) -> Arc<dyn PhysicalExpr> {
    if produced.data_type() == target.data_type()
        && produced.is_nullable() == target.is_nullable()
        && produced.metadata() == target.metadata()
    {
        expr
    } else {
        // The target field sets the output type and metadata, not the nullability.
        let options = lenient.then(|| CastOptions {
            safe: true,
            ..CastOptions::default()
        });
        Arc::new(CastExpr::new_with_target_field(
            expr,
            Arc::clone(target),
            options,
        ))
    }
}

/// Run a planned `BY NAME` insert into `table` and return its count stream.
///
/// With `lance`, the table first gets the `new_columns` and `type_changes` of
/// the plan-time mapping. The catalog then gets a new provider, because a
/// provider keeps the schema that it had at creation.
///
/// The projection comes from a new mapping against the current table schema.
/// Another statement can change the schema between plan and execution, and a
/// column that it added then gets NULL.
pub(crate) async fn insert_by_name(
    session: &Arc<SessionContext>,
    table: &TableReference,
    child: Arc<dyn ExecutionPlan>,
    mapping: &ByNameMapping,
    lance: Option<&beacon_lance::LanceTable>,
) -> anyhow::Result<SendableRecordBatchStream> {
    let widens = !mapping.new_columns.is_empty() || !mapping.type_changes.is_empty();
    if let Some(lance) = lance.filter(|_| widens) {
        let warehouse = session
            .state()
            .config()
            .get_extension::<beacon_lance::LanceWarehouse>()
            .ok_or_else(|| anyhow!("Lance warehouse is unavailable"))?;
        beacon_lance::widen_table(
            &warehouse,
            &lance.definition().location,
            &mapping.new_columns,
            &mapping.type_changes,
        )
        .await?;
        let fresh = lance
            .definition()
            .build_provider(session.clone())
            .await
            .and_then(|fresh| Ok(session.register_table(table.clone(), fresh)?));
        if let Err(error) = fresh {
            tracing::warn!(table = %table, error = %error, "reload after BY NAME widening failed");
            bail!("The table '{table}' is widened, but it did not reload: {error}");
        }
    }
    let provider = session.table_provider(table.clone()).await?;
    let source = match lance {
        Some(_) => Arc::new(lance_source_schema(&child.schema())),
        None => child.schema(),
    };
    let widening = session_widening(&session.state());
    let mapping = map_by_name(
        &source,
        &provider.schema(),
        false,
        &table.to_string(),
        widening.strategy.as_ref(),
    )?;
    let projected = by_name_projection(child, &mapping, provider.schema())?;
    super::actions::insert_into(session, table, InsertOp::Append, projected).await
}

impl fmt::Display for ByNameMapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let add: Vec<String> = self
            .new_columns
            .iter()
            .map(|c| format!("{}: {}", c.name(), c.data_type()))
            .collect();
        let widen: Vec<String> = self
            .type_changes
            .iter()
            .map(|(name, to)| {
                let from = self
                    .target_names
                    .iter()
                    .position(|n| n == name)
                    .map(|at| self.target_types[at].to_string())
                    .unwrap_or_default();
                format!("{name}: {from} -> {to}")
            })
            .collect();
        let mut cast = Vec::new();
        let mut null = Vec::new();
        for (name, column) in self.target_names.iter().zip(&self.columns) {
            match column {
                ColumnSource::Source { .. } => {}
                ColumnSource::Cast { index, to, lenient } => {
                    let mode = if *lenient { " lenient" } else { "" };
                    cast.push(format!("{name}: {}->{to}{mode}", self.source_types[*index]))
                }
                ColumnSource::Null { .. } => null.push(name.clone()),
            }
        }
        let groups: Vec<String> = [
            ("add", add),
            ("widen", widen),
            ("cast", cast),
            ("null", null),
        ]
        .into_iter()
        .filter(|(_, items)| !items.is_empty())
        .map(|(label, items)| format!("{label}=[{}]", items.join(", ")))
        .collect();
        write!(f, "{}", groups.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use arrow::record_batch::RecordBatch;
    use beacon_datafusion_ext::type_widening::{
        DefaultArrowTypeWidening, NumpyArrowTypeWidening, TypeConflict,
    };
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::collect;

    const DEFAULT: DefaultArrowTypeWidening = DefaultArrowTypeWidening::new();

    fn schema(fields: Vec<Field>) -> Schema {
        Schema::new(fields)
    }

    fn nullable(name: &str, data_type: DataType) -> Field {
        Field::new(name, data_type, true)
    }

    #[test]
    fn same_columns_other_order() {
        let source = schema(vec![
            nullable("b", DataType::Utf8),
            nullable("a", DataType::Int64),
        ]);
        let target = schema(vec![
            nullable("a", DataType::Int64),
            nullable("b", DataType::Utf8),
        ]);
        let mapping = map_by_name(&source, &target, false, "t", &DEFAULT).unwrap();
        assert!(mapping.new_columns.is_empty());
        assert_eq!(
            mapping.columns,
            vec![
                ColumnSource::Source { index: 1 },
                ColumnSource::Source { index: 0 }
            ]
        );
    }

    #[test]
    fn missing_column_is_null() {
        let source = schema(vec![nullable("a", DataType::Int64)]);
        let target = schema(vec![
            nullable("a", DataType::Int64),
            nullable("b", DataType::Float64),
        ]);
        let mapping = map_by_name(&source, &target, false, "t", &DEFAULT).unwrap();
        assert_eq!(
            mapping.columns,
            vec![
                ColumnSource::Source { index: 0 },
                ColumnSource::Null {
                    to: DataType::Float64
                }
            ]
        );
    }

    #[test]
    fn new_column_widens() {
        let source = schema(vec![
            nullable("a", DataType::Int64),
            nullable("x", DataType::Float32),
        ]);
        let target = schema(vec![nullable("a", DataType::Int64)]);
        let mapping = map_by_name(&source, &target, true, "t", &DEFAULT).unwrap();
        assert_eq!(mapping.new_columns, vec![nullable("x", DataType::Float32)]);
        assert_eq!(
            mapping.columns,
            vec![
                ColumnSource::Source { index: 0 },
                ColumnSource::Source { index: 1 }
            ]
        );
    }

    #[test]
    fn utf8view_new_column_is_lance_compatible() {
        let source = schema(vec![Field::new("s", DataType::Utf8View, false)]);
        let target = schema(vec![]);
        let mapping = map_by_name(&source, &target, true, "t", &DEFAULT).unwrap();
        assert_eq!(mapping.new_columns, vec![nullable("s", DataType::Utf8)]);
        // The projection casts the Utf8View column to the Utf8 target field.
        assert_eq!(mapping.columns, vec![ColumnSource::Source { index: 0 }]);
    }

    #[test]
    fn type_difference_casts() {
        let source = schema(vec![nullable("a", DataType::Int32)]);
        let target = schema(vec![nullable("a", DataType::Float64)]);
        let mapping = map_by_name(&source, &target, false, "t", &DEFAULT).unwrap();
        assert_eq!(
            mapping.columns,
            vec![ColumnSource::Cast {
                index: 0,
                to: DataType::Float64,
                lenient: false
            }]
        );
        assert_eq!(mapping.to_string(), "cast=[a: Int32->Float64]");
    }

    #[test]
    fn case_differs_is_a_new_column() {
        let source = schema(vec![nullable("Temp", DataType::Float64)]);
        let target = schema(vec![nullable("temp", DataType::Float64)]);
        let mapping = map_by_name(&source, &target, true, "t", &DEFAULT).unwrap();
        assert_eq!(
            mapping.new_columns,
            vec![nullable("Temp", DataType::Float64)]
        );
        assert_eq!(
            mapping.columns,
            vec![
                ColumnSource::Null {
                    to: DataType::Float64
                },
                ColumnSource::Source { index: 0 }
            ]
        );
        assert_eq!(mapping.to_string(), "add=[Temp: Float64], null=[temp]");
    }

    #[test]
    fn no_widen_lists_extra_columns() {
        let source = schema(vec![
            nullable("a", DataType::Int64),
            nullable("x", DataType::Int64),
            nullable("y", DataType::Int64),
        ]);
        let target = schema(vec![nullable("a", DataType::Int64)]);
        let error = map_by_name(&source, &target, false, "t", &DEFAULT).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Table 't' does not have the columns 'x', 'y'; only Lance tables add columns"
        );
    }

    #[test]
    fn uncastable_pair_fails() {
        let fields = Fields::from(vec![nullable("f", DataType::Int64)]);
        let source = schema(vec![nullable("a", DataType::Struct(fields))]);
        let target = schema(vec![nullable("a", DataType::Int64)]);
        let error = map_by_name(&source, &target, false, "t", &DEFAULT).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Column 'a': cannot cast Struct(\"f\": Int64) to Int64"
        );
    }

    #[test]
    fn missing_non_nullable_fails() {
        let source = schema(vec![nullable("a", DataType::Int64)]);
        let target = schema(vec![
            nullable("a", DataType::Int64),
            Field::new("b", DataType::Int64, false),
        ]);
        let error = map_by_name(&source, &target, false, "t", &DEFAULT).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Column 'b' is not nullable and the source does not have it"
        );
    }

    #[test]
    fn dotted_names_are_renamed() {
        assert_eq!(lance_column_name("temp._FillValue"), "temp__FillValue");
        let source = schema(vec![
            nullable("temp", DataType::Float64),
            nullable("temp.units", DataType::Utf8),
        ]);
        let target = schema(vec![
            nullable("temp", DataType::Float64),
            nullable("temp_units", DataType::Utf8),
        ]);
        let mapping = map_by_name(&source, &target, true, "t", &DEFAULT).unwrap();
        assert!(mapping.new_columns.is_empty());
        assert_eq!(
            mapping.columns,
            vec![
                ColumnSource::Source { index: 0 },
                ColumnSource::Source { index: 1 }
            ]
        );

        let added = map_by_name(&source, &schema(vec![]), true, "t", &DEFAULT).unwrap();
        assert_eq!(
            added.new_columns,
            vec![
                nullable("temp", DataType::Float64),
                nullable("temp_units", DataType::Utf8)
            ]
        );

        let clash = schema(vec![
            nullable("a.b", DataType::Int64),
            nullable("a_b", DataType::Int64),
        ]);
        let error = map_by_name(&clash, &target, true, "t", &DEFAULT).unwrap_err();
        assert_eq!(
            error.to_string(),
            "The source has the column 'a_b' more than one time"
        );

        let error = map_by_name(&source, &target, false, "t", &DEFAULT).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Table 't' does not have the columns 'temp.units'; only Lance tables add columns"
        );
    }

    #[test]
    fn wider_source_changes_the_type() {
        let source = schema(vec![nullable("c", DataType::Int64)]);
        let target = schema(vec![nullable("c", DataType::Int32)]);
        let mapping = map_by_name(&source, &target, true, "t", &DEFAULT).unwrap();
        assert_eq!(
            mapping.type_changes,
            vec![("c".to_string(), DataType::Int64)]
        );
        assert_eq!(mapping.columns, vec![ColumnSource::Source { index: 0 }]);
        assert_eq!(mapping.to_string(), "widen=[c: Int32 -> Int64]");

        // A table that does not widen keeps its type: a strict cast.
        let kept = map_by_name(&source, &target, false, "t", &DEFAULT).unwrap();
        assert!(kept.type_changes.is_empty());
        assert_eq!(
            kept.columns,
            vec![ColumnSource::Cast {
                index: 0,
                to: DataType::Int32,
                lenient: false
            }]
        );
    }

    #[test]
    fn narrower_source_casts() {
        let source = schema(vec![nullable("c", DataType::Int32)]);
        let target = schema(vec![nullable("c", DataType::Int64)]);
        let mapping = map_by_name(&source, &target, true, "t", &DEFAULT).unwrap();
        assert!(mapping.type_changes.is_empty());
        assert_eq!(
            mapping.columns,
            vec![ColumnSource::Cast {
                index: 0,
                to: DataType::Int64,
                lenient: false
            }]
        );
    }

    #[test]
    fn conflict_fail_errors() {
        let source = schema(vec![nullable("c", DataType::Utf8)]);
        let target = schema(vec![nullable("c", DataType::Int64)]);
        let error = map_by_name(&source, &target, true, "t", &DEFAULT).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Incompatible types for field 'c': Int64 in 't' vs Utf8 in 'source query'"
        );
    }

    #[test]
    fn conflict_keep_first_casts_leniently() {
        let keep_first = DefaultArrowTypeWidening::keeping_first_type();
        let source = schema(vec![nullable("c", DataType::Utf8)]);
        let target = schema(vec![nullable("c", DataType::Int64)]);
        let mapping = map_by_name(&source, &target, true, "t", &keep_first).unwrap();
        assert!(mapping.type_changes.is_empty());
        assert_eq!(
            mapping.columns,
            vec![ColumnSource::Cast {
                index: 0,
                to: DataType::Int64,
                lenient: true
            }]
        );
    }

    #[test]
    fn numpy_rule_is_used() {
        let numpy = NumpyArrowTypeWidening {
            on_conflict: TypeConflict::Fail,
        };
        let source = schema(vec![nullable("c", DataType::Float32)]);
        let target = schema(vec![nullable("c", DataType::Int64)]);
        let mapping = map_by_name(&source, &target, true, "t", &numpy).unwrap();
        assert_eq!(
            mapping.type_changes,
            vec![("c".to_string(), DataType::Float64)]
        );
        assert_eq!(
            mapping.columns,
            vec![ColumnSource::Cast {
                index: 0,
                to: DataType::Float64,
                lenient: false
            }]
        );
    }

    #[tokio::test]
    async fn lenient_cast_gives_null() {
        let keep_first = DefaultArrowTypeWidening::keeping_first_type();
        let source = Arc::new(schema(vec![nullable("a", DataType::Utf8)]));
        let batch = RecordBatch::try_new(
            source.clone(),
            vec![Arc::new(StringArray::from(vec!["7", "abc"]))],
        )
        .unwrap();
        let child = MemorySourceConfig::try_new_exec(&[vec![batch]], source.clone(), None).unwrap();
        let target = Arc::new(schema(vec![nullable("a", DataType::Int64)]));
        let mapping = map_by_name(&source, &target, false, "t", &keep_first).unwrap();
        let plan = by_name_projection(child, &mapping, target).unwrap();
        let batches = collect(plan, Arc::new(TaskContext::default()))
            .await
            .unwrap();
        let a = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a.value(0), 7);
        assert!(a.is_null(1));
    }

    #[test]
    fn duplicate_source_column_fails() {
        let source = schema(vec![
            nullable("a", DataType::Int64),
            nullable("a", DataType::Utf8),
        ]);
        let target = schema(vec![nullable("a", DataType::Int64)]);
        let error = map_by_name(&source, &target, true, "t", &DEFAULT).unwrap_err();
        assert_eq!(
            error.to_string(),
            "The source has the column 'a' more than one time"
        );
    }

    #[tokio::test]
    async fn projection_casts_and_nulls() {
        let source = Arc::new(schema(vec![
            nullable("b", DataType::Utf8),
            nullable("a", DataType::Int32),
        ]));
        let batch = RecordBatch::try_new(
            source.clone(),
            vec![
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )
        .unwrap();
        let child = MemorySourceConfig::try_new_exec(&[vec![batch]], source.clone(), None).unwrap();
        let target = Arc::new(schema(vec![
            nullable("a", DataType::Float64),
            nullable("b", DataType::Utf8),
            nullable("c", DataType::Int64),
        ]));
        let mapping = map_by_name(&source, &target, false, "t", &DEFAULT).unwrap();
        assert_eq!(mapping.to_string(), "cast=[a: Int32->Float64], null=[c]");
        let plan = by_name_projection(child, &mapping, target.clone()).unwrap();
        assert_eq!(plan.schema(), target);

        let batches = collect(plan, Arc::new(TaskContext::default()))
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.schema(), target);
        assert_eq!(batch.num_rows(), 1);
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(a.value(0), 1.0);
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(b.value(0), "x");
        assert_eq!(batch.column(2).data_type(), &DataType::Int64);
        assert!(batch.column(2).is_null(0));
    }

    #[tokio::test]
    async fn projection_cast_failure_is_an_error() {
        let source = Arc::new(schema(vec![nullable("a", DataType::Int64)]));
        let batch = RecordBatch::try_new(
            source.clone(),
            vec![Arc::new(Int64Array::from(vec![i64::MAX]))],
        )
        .unwrap();
        let child = MemorySourceConfig::try_new_exec(&[vec![batch]], source.clone(), None).unwrap();
        let target = Arc::new(schema(vec![nullable("a", DataType::Int32)]));
        let mapping = map_by_name(&source, &target, false, "t", &DEFAULT).unwrap();
        let plan = by_name_projection(child, &mapping, target).unwrap();
        let result = collect(plan, Arc::new(TaskContext::default())).await;
        assert!(result.is_err(), "a failed cast must not become NULL");
    }

    #[tokio::test]
    async fn projection_keeps_source_nullability_on_not_null_target() {
        let source = Arc::new(schema(vec![nullable("a", DataType::Int64)]));
        let batch = RecordBatch::try_new(
            source.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        let child = MemorySourceConfig::try_new_exec(&[vec![batch]], source.clone(), None).unwrap();
        let target = Arc::new(schema(vec![Field::new("a", DataType::Int64, false)]));
        let mapping = map_by_name(&source, &target, false, "t", &DEFAULT).unwrap();
        assert_eq!(mapping.columns, vec![ColumnSource::Source { index: 0 }]);
        let plan = by_name_projection(child, &mapping, target).unwrap();
        assert!(plan.schema().field(0).is_nullable());

        // The projection does not reject the NULL; the insert must do that.
        let batches = collect(plan, Arc::new(TaskContext::default()))
            .await
            .unwrap();
        assert!(batches[0].schema().field(0).is_nullable());
        assert_eq!(batches[0].column(0).null_count(), 1);
    }
}
