//! Output file handling for Beacon Query results.
//!
//! This module defines output formats, temporary file management, and utilities
//! for exporting query results in various formats.

use crate::query::temp_object::TempObject;
use crate::query_result::{OutputFileKind, QueryOutputFile};
use std::sync::Arc;

use beacon_arrow_csv::datafusion::DEFAULT_CSV_EXTENSION;
use beacon_arrow_geoparquet::datafusion::{
    GeoParquetFormatFactory, GeoParquetOptions, GEOPARQUET_EXTENSION,
};
use beacon_arrow_ipc::datafusion::{ArrowFormatFactory, DEFAULT_ARROW_EXTENSION};
use beacon_arrow_netcdf::datafusion::NETCDF_EXTENSION;
use beacon_arrow_netcdf::datafusion::{options::NetcdfOptions, NetCDFFormatFactory, NetcdfConfig};
use beacon_arrow_odv::datafusion::OdvFileFormatFactory;
use beacon_arrow_odv::writer::OdvOptions;
use beacon_arrow_parquet::datafusion::ParquetFormatFactory;
use datafusion::catalog::Session;
use datafusion::prelude::SessionContext;
use datafusion::{
    common::file_options::file_type::FileType,
    datasource::file_format::format_as_file_type,
    error::DataFusionError,
    logical_expr::{LogicalPlan, LogicalPlanBuilder},
};
use utoipa::ToSchema;

/// Represents the output configuration for a query.
#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct Output {
    /// The desired output format.
    pub format: OutputFormat,
}

impl Output {
    /// Parses the logical plan and prepares an output file in the specified format.
    ///
    /// # Arguments
    /// * `datasets_root` - Local root of the datasets store (used by the NetCDF writer's
    ///   sibling reader path; ignored by other formats).
    /// * `tmp_dir` - Directory the temporary output file is created in (the tmp store root),
    ///   also where the NetCDF writer emits its file.
    /// * `tmp_store_url` - The URL the tmp store is registered under; the COPY target is
    ///   `<tmp_store_url><object_name>`. Read from the session's `ObjectStoreUrls`
    ///   extension by the caller so it matches the URL the store was registered at.
    /// * `input_plan` - The logical plan to export.
    ///
    /// # Returns
    /// Tuple of the new logical plan and the output file wrapper.
    pub async fn parse(
        &self,
        tmp_dir: &std::path::Path,
        tmp_store_url: &datafusion::execution::object_store::ObjectStoreUrl,
        input_plan: LogicalPlan,
    ) -> datafusion::error::Result<(LogicalPlan, QueryOutputFile)> {
        let kind = self.format.file_kind();
        let file_type = self.format.file_type(tmp_dir).await;

        // Reserve a unique name under `tmp_dir` (the tmp store's root). The COPY
        // target is `<tmp_store_url><object_name>`; object-store writers resolve that
        // under the tmp store, and the native NetCDF/ODV sinks reconstruct
        // `output_dir.join(<object_name>)` — with `output_dir == tmp_dir` (see the
        // call site in `runtime.rs`), both land at `temp.path()`. One name, no
        // path↔URL pair to keep in sync.
        let temp = TempObject::create_in(tmp_dir, kind.suggested_extension())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let location = format!("{}{}", tmp_store_url.as_str(), temp.object_path());
        let plan = LogicalPlanBuilder::copy_to(
            input_plan,
            location,
            file_type,
            Default::default(),
            vec![],
        )?;

        Ok((plan.build()?, QueryOutputFile::new(kind, temp)))
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
    /// Flat (record-oriented) NetCDF format.
    NetCDF,
    /// Multi-dimensional (nd-array) NetCDF format. The named columns become the
    /// output dimensions; must not be empty.
    #[serde(alias = "nd_netcdf")]
    NdNetCDF {
        /// Columns to use as dimensions for the ND NetCDF output.
        dimension_columns: Vec<String>, // Cannot be empty
    },
    /// GeoParquet format with optional longitude/latitude columns.
    GeoParquet {
        /// Name of the longitude column, if any.
        longitude_column: Option<String>,
        /// Name of the latitude column, if any.
        latitude_column: Option<String>,
    },
    /// Ocean Data View (ODV) archive, configured by [`OdvOptions`].
    Odv(OdvOptions),
}

impl OutputFormat {
    /// The [`OutputFileKind`] the result file is tagged with (drives the download
    /// transport's MIME type / filename; both NetCDF variants share one kind).
    pub fn file_kind(&self) -> OutputFileKind {
        match self {
            OutputFormat::Csv => OutputFileKind::Csv,
            OutputFormat::Ipc => OutputFileKind::Ipc,
            OutputFormat::Parquet => OutputFileKind::Parquet,
            OutputFormat::GeoParquet { .. } => OutputFileKind::GeoParquet,
            OutputFormat::NetCDF => OutputFileKind::NetCDF,
            OutputFormat::NdNetCDF { .. } => OutputFileKind::NetCDF,
            OutputFormat::Odv(_) => OutputFileKind::Odv,
        }
    }

    /// Returns the DataFusion file type for this output format.
    ///
    /// `datasets_root` (the datasets store's local root) and `output_dir` (where
    /// the NetCDF writer emits its file) are used by the NetCDF writers; both are
    /// ignored by the other formats.
    pub fn file_type(&self, session: &SessionContext) -> anyhow::Result<Arc<dyn FileType>> {
        match self {
            OutputFormat::Csv => Ok(format_as_file_type(
                session
                    .state()
                    .get_file_format_factory(DEFAULT_CSV_EXTENSION)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "CSV format factory not registered under extension '{}'",
                            DEFAULT_CSV_EXTENSION
                        )
                    })?,
            )),
            OutputFormat::Ipc => Ok(format_as_file_type(
                session
                    .state()
                    .get_file_format_factory(DEFAULT_ARROW_EXTENSION)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Arrow IPC format factory not registered under extension '{}'",
                            DEFAULT_ARROW_EXTENSION
                        )
                    })?,
            )),
            OutputFormat::Parquet => Ok(format_as_file_type(
                session
                    .state()
                    .get_file_format_factory("parquet")
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Parquet format factory not registered under extension 'parquet'"
                        )
                    })?,
            )),
            OutputFormat::GeoParquet {
                longitude_column,
                latitude_column,
            } => Ok(format_as_file_type(
                session
                    .state()
                    .get_file_format_factory(GEOPARQUET_EXTENSION)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                        "GeoParquet format factory not registered under extension 'geo_parquet'"
                    )
                    })?,
            )),
            OutputFormat::NetCDF => {
                let options = NetcdfOptions::default();

                // Writing NetCDF: the reader cache / statistics config is
                // irrelevant here, so the defaults suffice.
                Ok(format_as_file_type(
                    session
                        .state()
                        .get_file_format_factory(NETCDF_EXTENSION)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "NetCDF format factory not registered under extension '{}'",
                                NETCDF_EXTENSION
                            )
                        })?,
                ))
            }
            OutputFormat::NdNetCDF { dimension_columns } => {
                let mut options = NetcdfOptions::default();
                options.unique_value_columns = dimension_columns.clone();
                options.write_dimensions = Some(dimension_columns.clone());

                Ok(format_as_file_type(
                    session
                        .state()
                        .get_file_format_factory(NETCDF_EXTENSION)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "NetCDF format factory not registered under extension '{}'",
                                NETCDF_EXTENSION
                            )
                        })?,
                ))
            }
            OutputFormat::Odv(options) => Ok(format_as_file_type(Arc::new(
                OdvFileFormatFactory::new(Some(options.clone())),
            ))),
        }
    }
}
