//! Output file handling for Beacon Query results.
//!
//! This module defines output formats, temporary file management, and utilities
//! for exporting query results in various formats.

use std::sync::Arc;

use beacon_arrow_odv::writer::OdvOptions;
use beacon_data_lake::DataLake;
use beacon_formats::{
    arrow::ArrowFormatFactory,
    arrow_stream::stream::DeferredBatchStream,
    csv::CsvFormatFactory,
    geo_parquet::{GeoParquetFormatFactory, GeoParquetOptions},
    netcdf::{NetCDFFormatFactory, NetcdfOptions},
    odv_ascii::OdvFileFormatFactory,
    parquet::ParquetFormatFactory,
};
use datafusion::{
    common::file_options::file_type::FileType,
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
        let file_type = self.format.file_type();
        let temp_output = data_lake.try_create_temp_output_file(".tmp");
        let plan = LogicalPlanBuilder::copy_to(
            input_plan,
            temp_output.output_url(),
            file_type,
            Default::default(),
            vec![],
        )?;

        Ok((
            plan.build()?,
            self.format.output_file(temp_output.into_temp_file()),
        ))
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

impl OutputFormat {
    /// Wraps a temporary file in the appropriate output file enum variant.
    pub fn output_file(&self, temp_file: NamedTempFile) -> QueryOutput {
        match self {
            OutputFormat::Csv => QueryOutput::Csv(temp_file),
            OutputFormat::Ipc => QueryOutput::Ipc(temp_file),
            OutputFormat::Parquet => QueryOutput::Parquet(temp_file),
            OutputFormat::GeoParquet { .. } => QueryOutput::GeoParquet(temp_file),
            OutputFormat::NetCDF => QueryOutput::NetCDF(temp_file),
            OutputFormat::Odv(_) => QueryOutput::Odv(temp_file),
        }
    }

    /// Returns the DataFusion file type for this output format.
    pub fn file_type(&self) -> Arc<dyn FileType> {
        match self {
            OutputFormat::Csv => format_as_file_type(Arc::new(CsvFormatFactory)),
            OutputFormat::Ipc => format_as_file_type(Arc::new(ArrowFormatFactory)),
            OutputFormat::Parquet => format_as_file_type(Arc::new(ParquetFormatFactory)),
            OutputFormat::GeoParquet {
                longitude_column,
                latitude_column,
            } => format_as_file_type(Arc::new(GeoParquetFormatFactory::new(GeoParquetOptions {
                longitude_column: longitude_column.clone(),
                latitude_column: latitude_column.clone(),
            }))),
            OutputFormat::NetCDF => {
                let options = NetcdfOptions::default();
                let object_resolver = DataLake::netcdf_object_resolver();
                let sink_resolver = DataLake::netcdf_sink_resolver();

                format_as_file_type(Arc::new(NetCDFFormatFactory::new(
                    options,
                    object_resolver,
                    sink_resolver,
                )))
            }
            OutputFormat::Odv(options) => {
                format_as_file_type(Arc::new(OdvFileFormatFactory::new(Some(options.clone()))))
            }
        }
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
