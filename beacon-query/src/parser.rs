use std::sync::Arc;

use beacon_data_lake::{
    prelude::geoparquet::{GeoParquetFormatFactory, GeoParquetOptions},
    DataLake,
};
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
        datalake: &DataLake,
        query: Query,
    ) -> anyhow::Result<ParsedPlan> {
        let datafusion_logical_plan = Self::parse_to_logical_plan(session, query.inner).await?;
        let (plan, output_file) =
            Self::parse_output(datalake, datafusion_logical_plan, query.output).await?;

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

        let offset = query_body.offset.unwrap_or(0);
        builder = builder.limit(offset, query_body.limit)?;

        let plan = builder.build()?;
        Ok(plan)
    }

    pub async fn parse_output(
        datalake: &DataLake,
        input_plan: LogicalPlan,
        output: Output,
    ) -> anyhow::Result<(LogicalPlan, QueryOutputFile)> {
        match output.format {
            OutputFormat::Csv => {
                let temp_output = datalake.try_create_temp_output_file("csv");
                let format = Arc::new(CsvFormatFactory::new());
                let file_type = format_as_file_type(format);

                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    temp_output.get_object_path().to_string(),
                    file_type,
                    Default::default(),
                    vec![],
                )?;

                Ok((
                    plan.build()?,
                    QueryOutputFile::Csv(temp_output.into_temp_file()),
                ))
            }
            OutputFormat::Ipc => {
                let temp_output = datalake.try_create_temp_output_file("arrow");
                let format = Arc::new(
                    datafusion::datasource::file_format::arrow::ArrowFormatFactory::default(),
                );
                let file_type = format_as_file_type(format);
                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    temp_output.get_object_path().to_string(),
                    file_type,
                    Default::default(),
                    vec![],
                )?;

                Ok((
                    plan.build()?,
                    QueryOutputFile::Ipc(temp_output.into_temp_file()),
                ))
            }
            OutputFormat::Parquet => {
                let temp_output = datalake.try_create_temp_output_file("parquet");
                let format = Arc::new(
                    datafusion::datasource::file_format::parquet::ParquetFormatFactory::default(),
                );
                let file_type = format_as_file_type(format);

                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    temp_output.get_object_path().to_string(),
                    file_type,
                    Default::default(),
                    vec![],
                )?;

                Ok((
                    plan.build()?,
                    QueryOutputFile::Parquet(temp_output.into_temp_file()),
                ))
            }
            OutputFormat::Json => {
                todo!()
            }
            // OutputFormat::Odv(odv_options) => {
            //     let temp_output = TempOutputFile::new("beacon", ".zip")?;
            //     let path = temp_output.object_store_path();
            //     let format = Arc::new(OdvFileFormatFactory::new(Some(odv_options)));
            //     let file_type = format_as_file_type(format);
            //     let plan = LogicalPlanBuilder::copy_to(
            //         input_plan,
            //         path.to_string(),
            //         file_type,
            //         Default::default(),
            //         vec![],
            //     )?;

            //     Ok((plan.build()?, QueryOutputFile::Odv(temp_output.file)))
            // }
            OutputFormat::NetCDF => {
                // let temp_output = datalake.try_create_temp_output_file("nc");
                // let path = temp_output.get_object_path();
                // let format = Arc::new(NetCDFFileFormatFactory);
                // let file_type = format_as_file_type(format);

                // let plan = LogicalPlanBuilder::copy_to(
                //     input_plan,
                //     path.to_string(),
                //     file_type,
                //     Default::default(),
                //     vec![],
                // )?;

                // Ok((plan.build()?, QueryOutputFile::NetCDF(temp_output.file)))
                todo!()
            }
            OutputFormat::GeoParquet {
                longitude_column,
                latitude_column,
            } => {
                let temp_output = datalake.try_create_temp_output_file("parquet");
                let format = Arc::new(GeoParquetFormatFactory::new(GeoParquetOptions {
                    longitude_column,
                    latitude_column,
                }));
                let file_type = format_as_file_type(format);

                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    temp_output.get_object_path().to_string(),
                    file_type,
                    Default::default(),
                    vec![],
                )?;

                Ok((
                    plan.build()?,
                    QueryOutputFile::GeoParquet(temp_output.into_temp_file()),
                ))
            }
        }
    }
}
