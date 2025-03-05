use datafusion::{logical_expr::LogicalPlan, prelude::SessionContext};

use crate::{plan::BeaconQueryPlan, QueryBody};

use super::Query;

pub struct Parser;

impl Parser {
    pub async fn parse(session: &SessionContext, query: Query) -> anyhow::Result<BeaconQueryPlan> {
        let datafusion_logical_plan = match query.inner {
            super::InnerQuery::Sql(_) => {
                tracing::error!("SQL queries are not supported yet");
                anyhow::bail!("SQL queries are not supported yet")
            }
            super::InnerQuery::Json(query_body) => {
                let datafusion_plan = Self::parse_json_query(query_body, session).await?;
                datafusion_plan
            }
        };

        Ok(BeaconQueryPlan::new(datafusion_logical_plan, query.output))
    }

    async fn parse_json_query(
        query_body: QueryBody,
        session: &SessionContext,
    ) -> anyhow::Result<LogicalPlan> {
        let mut builder = query_body.from.init_builder(&session).await?;

        builder = builder.project(
            query_body
                .select
                .iter()
                .map(|s| s.to_expr(&session.state()))
                .collect::<anyhow::Result<Vec<_>>>()?,
        )?;

        if let Some(filter) = query_body.filter {
            builder = builder.filter(filter.to_expr()?)?;
        }

        if let Some(sort_by) = query_body.sort_by {
            builder = builder.sort(sort_by.iter().map(|s| s.to_expr()))?;
        }

        let plan = builder.build()?;

        Ok(plan)
    }
}
