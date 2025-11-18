//! Output file handling for Beacon Query results.
//!
//! This module defines output formats, temporary file management, and utilities
//! for exporting query results in various formats.

use std::sync::Arc;

use beacon_arrow_odv::writer::OdvOptions;
use beacon_data_lake::DataLake;
use beacon_formats::{
    arrow::ArrowFormatFactory,
    arrow_stream::{stream::DeferredBatchStream, ArrowBatchesStreamFormatFactory},
    csv::CsvFormatFactory,
    geo_parquet::{GeoParquetFormatFactory, GeoParquetOptions},
    netcdf::{NetCDFFormatFactory, NetcdfOptions},
    odv_ascii::OdvFileFormatFactory,
    parquet::ParquetFormatFactory,
};
use datafusion::{
    datasource::file_format::format_as_file_type,
    logical_expr::{LogicalPlan, LogicalPlanBuilder},
    prelude::SessionContext,
};
use tempfile::NamedTempFile;
use utoipa::ToSchema;

/// Represents the output configuration for a query.
#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct Output {
    /// The desired output format.
    #[serde(flatten)]
    #[serde(default)]
    pub format: OutputFormat,
}

impl Output {
    /// Parses the logical plan and prepares an output file in the specified format.
    ///
    /// # Arguments
    /// * `_session_context` - DataFusion session context (unused).
    /// * `data_lake` - DataLake instance for temporary file creation.
    /// * `input_plan` - The logical plan to export.
    ///
    /// # Returns
    /// Tuple of the new logical plan and the output file wrapper.
    pub async fn parse(
        &self,
        _session_context: &SessionContext,
        data_lake: &DataLake,
        input_plan: LogicalPlan,
    ) -> datafusion::error::Result<(LogicalPlan, QueryOutput)> {
        let temp_output = data_lake.try_create_temp_output_file(".tmp");

        match &self.format {
            OutputFormat::Csv => {
                let ff = format_as_file_type(Arc::new(CsvFormatFactory));
                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    temp_output.output_url(),
                    ff,
                    Default::default(),
                    vec![],
                )?;
                Ok((
                    plan.build()?,
                    QueryOutput::Csv(temp_output.into_temp_file()),
                ))
            }
            OutputFormat::Ipc => {
                let ff = format_as_file_type(Arc::new(ArrowFormatFactory));
                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    temp_output.output_url(),
                    ff,
                    Default::default(),
                    vec![],
                )?;
                Ok((
                    plan.build()?,
                    QueryOutput::Ipc(temp_output.into_temp_file()),
                ))
            }
            OutputFormat::Parquet => {
                let ff = format_as_file_type(Arc::new(ParquetFormatFactory));
                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    temp_output.output_url(),
                    ff,
                    Default::default(),
                    vec![],
                )?;
                Ok((
                    plan.build()?,
                    QueryOutput::Parquet(temp_output.into_temp_file()),
                ))
            }
            OutputFormat::NetCDF => {
                let options = NetcdfOptions::default();
                let object_resolver = DataLake::netcdf_object_resolver();
                let sink_resolver = DataLake::netcdf_sink_resolver();
                let ff = format_as_file_type(Arc::new(NetCDFFormatFactory::new(
                    options,
                    object_resolver,
                    sink_resolver,
                )));
                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    temp_output.output_url(),
                    ff,
                    Default::default(),
                    vec![],
                )?;
                Ok((
                    plan.build()?,
                    QueryOutput::NetCDF(temp_output.into_temp_file()),
                ))
            }
            OutputFormat::GeoParquet {
                longitude_column,
                latitude_column,
            } => {
                let ff = format_as_file_type(Arc::new(GeoParquetFormatFactory::new(
                    GeoParquetOptions {
                        longitude_column: longitude_column.clone(),
                        latitude_column: latitude_column.clone(),
                    },
                )));
                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    temp_output.output_url(),
                    ff,
                    Default::default(),
                    vec![],
                )?;
                Ok((
                    plan.build()?,
                    QueryOutput::GeoParquet(temp_output.into_temp_file()),
                ))
            }
            OutputFormat::Odv(odv_options) => {
                let ff = format_as_file_type(Arc::new(OdvFileFormatFactory::new(Some(
                    odv_options.clone(),
                ))));
                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    temp_output.output_url(),
                    ff,
                    Default::default(),
                    vec![],
                )?;
                Ok((
                    plan.build()?,
                    QueryOutput::Odv(temp_output.into_temp_file()),
                ))
            }
            OutputFormat::ArrowStream => {
                let (ff, stream) = ArrowBatchesStreamFormatFactory::new();
                let plan = LogicalPlanBuilder::copy_to(
                    input_plan,
                    temp_output.output_url(),
                    format_as_file_type(Arc::new(ff)),
                    Default::default(),
                    vec![],
                )?;
                Ok((plan.build()?, QueryOutput::ArrowStream(stream)))
            }
        }
    }
}

/// Supported output formats for query results.
#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// CSV format.
    Csv,
    /// Arrow IPC format.
    #[serde(alias = "arrow")]
    Ipc,
    /// Parquet format.
    Parquet,
    // Json,
    // Odv(OdvOptions),
    NetCDF,
    /// GeoParquet format with optional longitude/latitude columns.
    GeoParquet {
        /// Name of the longitude column, if any.
        longitude_column: Option<String>,
        /// Name of the latitude column, if any.
        latitude_column: Option<String>,
    },
    Odv(OdvOptions),
    /// Default arrow stream
    #[serde(alias = "arrow_stream")]
    ArrowStream,
}

impl Default for OutputFormat {
    fn default() -> Self {
        OutputFormat::ArrowStream
    }
}

/// Wrapper for temporary output files in various formats.
#[derive(Debug)]
pub enum QueryOutput {
    /// CSV output file.
    Csv(NamedTempFile),
    /// Arrow IPC output file.
    Ipc(NamedTempFile),
    /// JSON output file.
    Json(NamedTempFile),
    /// Parquet output file.
    Parquet(NamedTempFile),
    /// NetCDF output file.
    NetCDF(NamedTempFile),
    /// ODV output file.
    Odv(NamedTempFile),
    /// GeoParquet output file.
    GeoParquet(NamedTempFile),
    /// Default arrow stream
    ArrowStream(DeferredBatchStream),
}

impl QueryOutput {
    /// Returns the size of the output file in bytes.
    pub fn size(&self) -> anyhow::Result<u64> {
        match self {
            QueryOutput::Csv(file) => Ok(file.path().metadata()?.len()),
            QueryOutput::Ipc(file) => Ok(file.path().metadata()?.len()),
            QueryOutput::Json(file) => Ok(file.path().metadata()?.len()),
            QueryOutput::Parquet(file) => Ok(file.path().metadata()?.len()),
            QueryOutput::NetCDF(file) => Ok(file.path().metadata()?.len()),
            QueryOutput::Odv(file) => Ok(file.path().metadata()?.len()),
            QueryOutput::GeoParquet(file) => Ok(file.path().metadata()?.len()),
            // ToDo: implement size calculation for ArrowStream
            QueryOutput::ArrowStream(_) => Ok(0),
        }
    }
}
