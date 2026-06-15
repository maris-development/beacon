//! Compiler that turns a JSON [`QueryBody`] into a DataFusion `LogicalPlan`.
//!
//! SQL-form client queries (`InnerQuery::Sql`) are not handled here — they go
//! straight to DataFusion's SQL parser in `Runtime::plan_client_query`; only the
//! JSON form is "compiled".

use datafusion::{logical_expr::LogicalPlan, prelude::SessionContext};

use crate::query::QueryBody;

/// Compile a JSON query body into a DataFusion `LogicalPlan`.
pub async fn compile_json_query(
    query_body: QueryBody,
    session: &SessionContext,
) -> anyhow::Result<LogicalPlan> {
    let mut builder = if beacon_config::CONFIG.sql.enable_pushdown_projection {
        let mut all_columns = vec![];
        for select in &query_body.select {
            let mut select_cols = vec![];
            select.collect_columns(&mut select_cols);
            all_columns.extend(select_cols);
        }

        query_body
            .from
            .unwrap_or_default()
            .init_builder(session, Some(&all_columns))
            .await?
    } else {
        query_body
            .from
            .unwrap_or_default()
            .init_builder(session, None)
            .await?
    };

    let session_state = session.state();

    builder = builder.project(
        query_body
            .select
            .iter()
            .map(|s| s.to_expr(&session.state()))
            .collect::<anyhow::Result<Vec<_>>>()?,
    )?;

    let df_schema = builder.schema().clone();
    let schema = df_schema.as_arrow();
    if let Some(filter) = query_body.filter {
        builder = builder.filter(filter.parse(&session_state, schema)?)?;
    }

    if let Some(filters) = query_body.filters {
        for filter in filters {
            builder = builder.filter(filter.parse(&session_state, schema)?)?;
        }
    }

    if let Some(sort_by) = query_body.sort_by {
        builder = builder.sort(sort_by.iter().map(|s| s.to_expr()))?;
    }

    if let Some(distinct) = query_body.distinct {
        let on_exprs = distinct
            .on
            .iter()
            .map(|s| s.to_expr(&session.state()))
            .collect::<anyhow::Result<Vec<_>>>()?;

        let select_exprs = distinct
            .select
            .iter()
            .map(|s| s.to_expr(&session.state()))
            .collect::<anyhow::Result<Vec<_>>>()?;

        builder = builder.distinct_on(on_exprs, select_exprs, None)?;
    }

    let offset = query_body.offset.unwrap_or(0);
    builder = builder.limit(offset, query_body.limit)?;

    let plan = builder.build()?;
    Ok(plan)
}
