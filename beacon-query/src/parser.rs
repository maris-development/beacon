use datafusion::{logical_expr::LogicalPlan, prelude::SessionContext};

use crate::{plan::BeaconQueryPlan, InnerQuery, QueryBody};

use super::Query;

pub struct Parser;

impl Parser {
    pub async fn parse(session: &SessionContext, query: Query) -> anyhow::Result<BeaconQueryPlan> {
        let datafusion_logical_plan = Self::parse_to_logical_plan(session, query.inner).await?;
        Ok(BeaconQueryPlan::new(
            datafusion_logical_plan,
            query.output.format,
        ))
    }

    pub async fn parse_to_logical_plan(
        session: &SessionContext,
        inner_query: InnerQuery,
    ) -> anyhow::Result<LogicalPlan> {
        let datafusion_logical_plan = match inner_query {
            //ToDO: Implement SQL queries
            InnerQuery::Sql(_sql) => {
                tracing::error!("SQL queries are not supported yet");
                anyhow::bail!("SQL queries are not supported yet")
            }
            InnerQuery::Json(query_body) => {
                let datafusion_plan = Self::parse_json_query(query_body, session).await?;
                datafusion_plan
            }
        };

        Ok(datafusion_logical_plan)
    }

    pub async fn parse_json_query(
        query_body: QueryBody,
        session: &SessionContext,
    ) -> anyhow::Result<LogicalPlan> {
        let mut builder = query_body
            .from
            .unwrap_or_default()
            .init_builder(&session)
            .await?;
        let session_state = session.state();

        builder = builder.project(
            query_body
                .select
                .iter()
                .map(|s| s.to_expr(&session.state()))
                .collect::<anyhow::Result<Vec<_>>>()?,
        )?;

        if let Some(filter) = query_body.filter {
            builder = builder.filter(filter.to_expr(&session_state)?)?;
        }

        if let Some(filters) = query_body.filters {
            for filter in filters {
                builder = builder.filter(filter.to_expr(&session_state)?)?;
            }
        }

        if let Some(sort_by) = query_body.sort_by {
            builder = builder.sort(sort_by.iter().map(|s| s.to_expr()))?;
        }

        let plan = builder.build()?;
        Ok(plan)
    }
}
