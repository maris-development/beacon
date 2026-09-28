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

    let mut builder = if enable_pushdown_projection {
        let mut all_columns = vec![];
        for select in &query_body.select {
            select.collect_columns(&mut all_columns);
        }
        // A filter and a distinct can read a column that the select leaves out.
        for filter in &filters {
            filter.collect_columns(&mut all_columns);
        }
        if let Some(distinct) = &query_body.distinct {
            for select in distinct.on.iter().chain(&distinct.select) {
                select.collect_columns(&mut all_columns);
            }
        }

        from.init_builder(session, Some(&all_columns)).await?
    } else {
        from.init_builder(session, None).await?
    };

    let session_state = session.state();

    // Filter before the projection, so the filter sees every source column.
    let df_schema = builder.schema().clone();
    let schema = df_schema.as_arrow();
    for filter in &filters {
        builder = builder.filter(filter.parse(&session_state, schema)?)?;
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
