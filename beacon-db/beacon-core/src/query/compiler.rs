//! Compiler that turns a JSON [`QueryBody`] into a DataFusion `LogicalPlan`.
//!
//! SQL-form client queries (`InnerQuery::Sql`) are not handled here — they go
//! straight to DataFusion's SQL parser in `Runtime::plan_client_query`; only the
//! JSON form is "compiled".

use datafusion::{
    arrow::datatypes::{Field, Schema},
    common::{
        tree_node::{Transformed, TransformedResult, TreeNode},
        DFSchema,
    },
    execution::SessionState,
    logical_expr::{ExprSchemable, LogicalPlan},
    prelude::{Expr, SessionContext},
};

use crate::query::{QueryBody, Select};

/// Compile a JSON query body into a DataFusion `LogicalPlan`.
pub async fn compile_json_query(
    query_body: QueryBody,
    session: &SessionContext,
) -> anyhow::Result<LogicalPlan> {
    // The runtime settings are published as a SessionConfig extension; fall back to
    // defaults if absent (e.g. a bare session in a unit test).
    let settings = crate::settings::SqlSettings::from_session(session);
    let enable_pushdown_projection = settings.enable_pushdown_projection;
    let from = query_body
        .from
        .unwrap_or_else(|| crate::query::from::From::Table(settings.default_table.clone()));

    let filters: Vec<_> = query_body
        .filter
        .into_iter()
        .chain(query_body.filters.into_iter().flatten())
        .collect();

    let session_state = session.state();

    let mut filter_columns = vec![];
    for filter in &filters {
        filter.collect_columns(&mut filter_columns);
    }
    let aliases = filter_aliases(&query_body.select, &filter_columns, &session_state)?;

    let mut builder = if enable_pushdown_projection {
        let mut all_columns = vec![];
        for select in &query_body.select {
            select.collect_columns(&mut all_columns);
        }
        // A filter and a distinct can read a column that the select leaves out.
        // The select already collects the columns behind an alias.
        all_columns.extend(
            filter_columns
                .into_iter()
                .filter(|column| !aliases.iter().any(|(alias, _)| alias == column)),
        );
        if let Some(distinct) = &query_body.distinct {
            for select in distinct.on.iter().chain(&distinct.select) {
                select.collect_columns(&mut all_columns);
            }
        }

        from.init_builder(session, Some(&all_columns)).await?
    } else {
        from.init_builder(session, None).await?
    };

    // Filter before the projection, so the filter sees every source column and every alias.
    let df_schema = builder.schema().clone();
    let schema = filter_schema(&df_schema, &aliases)?;
    for filter in &filters {
        let predicate = resolve_aliases(filter.parse(&session_state, &schema)?, &aliases)?;
        builder = builder.filter(predicate)?;
    }

    let select_exprs = query_body
        .select
        .iter()
        .map(|s| s.to_expr(&session.state()))
        .collect::<anyhow::Result<Vec<_>>>()?;

    if let Some(distinct) = query_body.distinct {
        // The distinct reads the source columns and selects its own output, so it runs before
        // the top-level select, which is optional here.
        let on_exprs = distinct
            .on
            .iter()
            .map(|s| s.to_expr(&session.state()))
            .collect::<anyhow::Result<Vec<_>>>()?;

        let distinct_exprs = distinct
            .select
            .iter()
            .map(|s| s.to_expr(&session.state()))
            .collect::<anyhow::Result<Vec<_>>>()?;

        builder = builder.distinct_on(on_exprs, distinct_exprs, None)?;
        if !select_exprs.is_empty() {
            builder = builder.project(select_exprs)?;
        }
    } else {
        builder = builder.project(select_exprs)?;
    }

    // Sort last, so the rows come out in the requested order.
    if let Some(sort_by) = query_body.sort_by {
        builder = builder.sort(sort_by.iter().map(|s| s.to_expr()))?;
    }

    let offset = query_body.offset.unwrap_or(0);
    builder = builder.limit(offset, query_body.limit)?;

    let plan = builder.build()?;
    Ok(plan)
}

/// The select aliases that a filter names, each with the unaliased expression it stands for.
///
/// Only the aliases a filter names are planned here, so a query with no alias filter
/// keeps its select as it is.
fn filter_aliases(
    select: &[Select],
    filter_columns: &[String],
    session_state: &SessionState,
) -> anyhow::Result<Vec<(String, Expr)>> {
    let mut aliases: Vec<(String, Expr)> = vec![];
    for item in select {
        let alias = match item {
            Select::Column { alias, .. }
            | Select::Function { alias, .. }
            | Select::Literal { alias, .. } => alias,
            Select::ColumnName(_) => &None,
        };
        let Some(alias) = alias else { continue };
        if !filter_columns.contains(alias) || aliases.iter().any(|(name, _)| name == alias) {
            continue;
        }
        if let Expr::Alias(aliased) = item.to_expr(session_state)? {
            aliases.push((aliased.name, *aliased.expr));
        }
    }
    Ok(aliases)
}

/// The schema a filter reads: the source columns plus the aliases, typed by their expressions.
///
/// An alias replaces a source column of the same name.
fn filter_schema(source: &DFSchema, aliases: &[(String, Expr)]) -> anyhow::Result<Schema> {
    let mut fields: Vec<Field> = source
        .fields()
        .iter()
        .filter(|field| !aliases.iter().any(|(alias, _)| alias == field.name()))
        .map(|field| field.as_ref().clone())
        .collect();
    for (alias, expr) in aliases {
        fields.push(Field::new(alias, expr.get_type(source)?, true));
    }
    Ok(Schema::new(fields))
}

/// Replaces each column reference to an alias with the expression behind the alias.
fn resolve_aliases(predicate: Expr, aliases: &[(String, Expr)]) -> anyhow::Result<Expr> {
    if aliases.is_empty() {
        return Ok(predicate);
    }
    // A bottom-up pass does not visit a substituted expression again.
    let resolved = predicate
        .transform_up(|expr| {
            if let Expr::Column(column) = &expr {
                let target = aliases
                    .iter()
                    .find(|(alias, _)| column.relation.is_none() && *alias == column.name);
                if let Some((_, base)) = target {
                    return Ok(Transformed::yes(base.clone()));
                }
            }
            Ok(Transformed::no(expr))
        })
        .data()?;
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use datafusion::arrow::array::{Float64Array, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use serde_json::json;

    fn session() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("depth", DataType::Int32, true),
            Field::new("temperature", DataType::Float64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![0, 5, 20])),
                Arc::new(Float64Array::from(vec![10.0, 12.5, 4.0])),
                Arc::new(StringArray::from(vec!["a", "b", "b"])),
            ],
        )
        .unwrap();
        let session = SessionContext::new();
        session
            .register_table(
                "t",
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .unwrap();
        session
    }

    async fn plan(session: &SessionContext, body: serde_json::Value) -> LogicalPlan {
        let body: QueryBody = serde_json::from_value(body).expect("body should deserialize");
        compile_json_query(body, session)
            .await
            .expect("query should compile")
    }

    /// The `depth` values of the result, which has `depth` (or its alias) as its first column.
    async fn depths(body: serde_json::Value) -> Vec<i32> {
        let session = session();
        let plan = plan(&session, body).await;
        let batches = session
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("expected Int32")
                    .clone();
                column.into_iter().flatten().collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn a_filter_can_name_a_column_alias() {
        let rows = depths(json!({
            "select": [{"column": "depth", "alias": "DEPTH"}],
            "from": "t",
            "filters": [
                {"column": "DEPTH", "gt_eq": 0, "lt_eq": 10},
                {"is_not_null": {"column": "DEPTH"}}
            ],
            "sort_by": [{"Asc": "DEPTH"}]
        }))
        .await;
        assert_eq!(rows, vec![0, 5]);
    }

    /// The number literal takes the type of the column behind the alias, so the filter does
    /// not cast the column.
    #[tokio::test]
    async fn an_alias_filter_coerces_to_the_base_column_type() {
        let session = session();
        let plan = plan(
            &session,
            json!({
                "select": [{"column": "depth", "alias": "DEPTH"}],
                "from": "t",
                "filter": {"column": "DEPTH", "gt": 3}
            }),
        )
        .await;
        let display = plan.display_indent().to_string();
        assert!(display.contains("t.depth > Int32(3)"), "{display}");
    }

    #[tokio::test]
    async fn a_filter_can_name_a_function_alias() {
        let rows = depths(json!({
            "select": ["depth", {"function": "upper", "args": ["name"], "alias": "NAME"}],
            "from": "t",
            "filter": {"column": "NAME", "eq": "B"},
            "sort_by": [{"Asc": "depth"}]
        }))
        .await;
        assert_eq!(rows, vec![5, 20]);
    }

    /// An alias with the name of a source column hides that column from the filter.
    #[tokio::test]
    async fn an_alias_wins_over_a_source_column_of_the_same_name() {
        let rows = depths(json!({
            "select": [{"column": "depth", "alias": "temperature"}],
            "from": "t",
            "filter": {"column": "temperature", "gt": 6}
        }))
        .await;
        assert_eq!(
            rows,
            vec![20],
            "the filter reads depth, not the source temperature"
        );
    }

    #[tokio::test]
    async fn a_filter_can_still_name_an_unselected_source_column() {
        let rows = depths(json!({
            "select": [{"column": "depth", "alias": "DEPTH"}],
            "from": "t",
            "filter": {"column": "temperature", "lt": 5}
        }))
        .await;
        assert_eq!(rows, vec![20]);
    }
}
