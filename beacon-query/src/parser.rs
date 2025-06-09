use std::sync::Arc;

use beacon_output::{Output, TempOutputFile};
use beacon_sources::{
    netcdf_format::NetCDFFileFormatFactory,
    odv_format::{OdvFileFormatFactory, OdvFormat},
};
use datafusion::{
    datasource::file_format::{csv::CsvFormatFactory, format_as_file_type, FileFormat},
    logical_expr::{Analyze, LogicalPlan, LogicalPlanBuilder},
    prelude::{SQLOptions, SessionContext},
};

use crate::{output::QueryOutputFile, plan::ParsedPlan, InnerQuery, QueryBody};

use super::Query;

pub struct Parser;

impl Parser {
    pub async fn parse(session: &SessionContext, query: Query) -> anyhow::Result<ParsedPlan> {
        let datafusion_logical_plan = Self::parse_to_logical_plan(session, query.inner).await?;
        let (plan, output_file) = Self::parse_output(datafusion_logical_plan, query.output).await?;

        Ok(ParsedPlan::new(plan, output_file))
    }

    pub async fn parse_to_logical_plan(
        session: &SessionContext,
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

        let plan = builder.build()?;
        Ok(plan)
    }

    pub async fn parse_output(
        input_plan: LogicalPlan,
        output: Output,
    ) -> anyhow::Result<(LogicalPlan, QueryOutputFile)> {
        match output.format {
            beacon_output::OutputFormat::Csv => {
                let temp_output = TempOutputFile::new("beacon", ".csv")?;
                let format = Arc::new(CsvFormatFactory::new());
                let file_type = format_as_file_type(format);

                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    temp_output.object_store_path().to_string(),
                    file_type,
                    Default::default(),
                    vec![],
                )?;

                Ok((plan.build()?, QueryOutputFile::Csv(temp_output.file)))
            }
            beacon_output::OutputFormat::Ipc => {
                let temp_output = TempOutputFile::new("beacon", ".arrow")?;
                let format = Arc::new(
                    datafusion::datasource::file_format::arrow::ArrowFormatFactory::default(),
                );
                let file_type = format_as_file_type(format);
                let path = temp_output.object_store_path();
                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    path.to_string(),
                    file_type,
                    Default::default(),
                    vec![],
                )?;

                Ok((plan.build()?, QueryOutputFile::Ipc(temp_output.file)))
            }
            beacon_output::OutputFormat::Parquet => {
                let temp_output = TempOutputFile::new("beacon", ".parquet")?;
                let path = temp_output.object_store_path();
                let format = Arc::new(
                    datafusion::datasource::file_format::parquet::ParquetFormatFactory::default(),
                );
                let file_type = format_as_file_type(format);

                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    path.to_string(),
                    file_type,
                    Default::default(),
                    vec![],
                )?;

                Ok((plan.build()?, QueryOutputFile::Parquet(temp_output.file)))
            }
            beacon_output::OutputFormat::Json => {
                let temp_output = TempOutputFile::new("beacon", ".json")?;
                let path = temp_output.object_store_path();
                let format = Arc::new(
                    datafusion::datasource::file_format::json::JsonFormatFactory::default(),
                );
                let file_type = format_as_file_type(format);

                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    path.to_string(),
                    file_type,
                    Default::default(),
                    vec![],
                )?;

                Ok((plan.build()?, QueryOutputFile::Json(temp_output.file)))
            }
            beacon_output::OutputFormat::Odv(odv_options) => {
                let temp_output = TempOutputFile::new("beacon", ".zip")?;
                let path = temp_output.object_store_path();
                let format = Arc::new(OdvFileFormatFactory::new(Some(odv_options)));
                let file_type = format_as_file_type(format);
                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    path.to_string(),
                    file_type,
                    Default::default(),
                    vec![],
                )?;

                Ok((plan.build()?, QueryOutputFile::Odv(temp_output.file)))
            }
            beacon_output::OutputFormat::NetCDF => {
                let temp_output = TempOutputFile::new("beacon", ".nc")?;
                let path = temp_output.object_store_path();
                let format = Arc::new(NetCDFFileFormatFactory);
                let file_type = format_as_file_type(format);

                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    path.to_string(),
                    file_type,
                    Default::default(),
                    vec![],
                )?;

                Ok((plan.build()?, QueryOutputFile::NetCDF(temp_output.file)))
            }
        }
    }
}
