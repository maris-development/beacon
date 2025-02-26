use datafusion::{
    logical_expr::{LogicalPlan, LogicalPlanBuilder},
    prelude::{DataFrame, SessionContext},
};

use super::{InnerQuery, Query};

pub struct Parser;

impl Parser {
    pub async fn parse(session: &SessionContext, query: InnerQuery) -> anyhow::Result<LogicalPlan> {
        match query {
            super::InnerQuery::Sql(_) => anyhow::bail!("SQL queries are not supported yet"),
            super::InnerQuery::Json(query_body) => {
                let mut builder = query_body.from.init_builder(&session).await?;

                builder = builder.project(query_body.select.iter().map(|s| s.to_expr()))?;

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
    }
}
