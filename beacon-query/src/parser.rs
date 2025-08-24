use std::sync::Arc;

use beacon_data_lake::DataLake;
use datafusion::{
    datasource::file_format::{csv::CsvFormatFactory, format_as_file_type, FileFormat},
    logical_expr::{Analyze, LogicalPlan, LogicalPlanBuilder},
    prelude::{SQLOptions, SessionContext},
};

use crate::{
    output::{Output, OutputFormat, QueryOutputFile},
    plan::ParsedPlan,
    InnerQuery, QueryBody,
};

use super::Query;

pub struct Parser;

impl Parser {
    pub async fn parse(
        session: &SessionContext,
        data_lake: &DataLake,
        query: Query,
    ) -> anyhow::Result<ParsedPlan> {
        let datafusion_logical_plan =
            Self::parse_to_logical_plan(session, data_lake, query.inner).await?;

        let (plan, output_file) = query
            .output
            .parse(session, data_lake, datafusion_logical_plan)
            .await?;

        Ok(ParsedPlan::new(plan, output_file))
    }

    pub async fn parse_to_logical_plan(
        session: &SessionContext,
        data_lake: &DataLake,
        inner_query: InnerQuery,
    ) -> anyhow::Result<LogicalPlan> {
        let datafusion_logical_plan = match inner_query {
            InnerQuery::Sql(sql) => {
                if beacon_config::CONFIG.enable_sql {
                    let sql_options = SQLOptions::new()
                        .with_allow_ddl(false)
                        .with_allow_dml(false)
                        .with_allow_statements(false);
                    let logical_plan = session
                        .sql_with_options(&sql, sql_options)
                        .await?
                        .into_parts()
                        .1;
                    logical_plan
                } else {
                    // Return an error if SQL queries are not enabled
                    tracing::warn!("SQL queries are not enabled");
                    return Err(anyhow::anyhow!("SQL queries are not enabled"));
                }
            }
            InnerQuery::Json(query_body) => {
                let datafusion_plan =
                    Self::parse_json_query(query_body, session, data_lake).await?;
                datafusion_plan
            }
        };

        Ok(datafusion_logical_plan)
    }

    pub async fn parse_json_query(
        query_body: QueryBody,
        session: &SessionContext,
        data_lake: &DataLake,
    ) -> anyhow::Result<LogicalPlan> {
        let mut builder = query_body
            .from
            .unwrap_or_default()
            .init_builder(&session, data_lake)
            .await?;

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
            builder = builder.filter(filter.parse(&session_state, &schema)?)?;
        }

        if let Some(filters) = query_body.filters {
            for filter in filters {
                builder = builder.filter(filter.parse(&session_state, &schema)?)?;
            }
        }

        if let Some(sort_by) = query_body.sort_by {
            builder = builder.sort(sort_by.iter().map(|s| s.to_expr()))?;
        }

        let offset = query_body.offset.unwrap_or(0);
        builder = builder.limit(offset, query_body.limit)?;

        let plan = builder.build()?;
        Ok(plan)
    }
}
