//! Output file handling for Beacon Query results.
//!
//! This module defines output formats, temporary file management, and utilities
//! for exporting query results in various formats.

use crate::query::temp_object::TempObject;
use crate::query_result::{OutputFileKind, QueryOutputFile};
use std::sync::Arc;

use beacon_arrow_csv::datafusion::DEFAULT_CSV_EXTENSION;
use beacon_arrow_geoparquet::datafusion::{GeoParquetFormatFactory, GeoParquetOptions};
use beacon_arrow_ipc::datafusion::{ArrowFormatFactory, DEFAULT_ARROW_EXTENSION};
use beacon_arrow_netcdf::datafusion::NETCDF_EXTENSION;
use beacon_arrow_netcdf::datafusion::{NetCDFFormatFactory, NetcdfConfig};
use beacon_arrow_odv::datafusion::OdvFileFormatFactory;
use beacon_arrow_odv::writer::OdvOptions;
use beacon_arrow_parquet::datafusion::ParquetFormatFactory;
use beacon_arrow_zarr::datafusion::{ZarrFormatFactory, ZARR_EXTENSION};
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
    #[schema(value_type = OutputFormatSchema)]
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
        session: &SessionContext,
        tmp_dir: &std::path::Path,
        tmp_store_url: &datafusion::execution::object_store::ObjectStoreUrl,
        input_plan: LogicalPlan,
    ) -> datafusion::error::Result<(LogicalPlan, QueryOutputFile)> {
        let kind = self.format.file_kind();
        let file_type = self
            .format
            .file_type(session)
            .map_err(|e| DataFusionError::External(e.into()))?;

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

pub use beacon_arrow_netcdf::datafusion::options::NetcdfWriterOptions as NetcdfOutputOptions;

/// Supported output formats for query results.
// `remote = "Self"` makes the derived serde code inherent, for the trait impls below.
#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(remote = "Self", rename_all = "lowercase")]
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
    /// Flat (record-oriented) NetCDF format. A NetCDF-4 file *is* HDF5, so `hdf5`/`h5` are
    /// accepted as aliases for this format. The plain name gives the default options.
    #[serde(alias = "hdf5", alias = "h5")]
    NetCDF(NetcdfOutputOptions),
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
    /// Flat Zarr v3 store: each column is a 1-D array over one `obs`
    /// dimension. A store is a directory, so it is returned as a zip archive.
    Zarr,
}

/// Format names that select the flat NetCDF output.
const NETCDF_NAMES: [&str; 3] = ["netcdf", "hdf5", "h5"];

impl<'de> serde::Deserialize<'de> for OutputFormat {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FormatVisitor;

        impl<'de> serde::de::Visitor<'de> for FormatVisitor {
            type Value = OutputFormat;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an output format name, or an object with one output format key")
            }

            fn visit_str<E: serde::de::Error>(self, name: &str) -> Result<OutputFormat, E> {
                // The derived code reads a newtype variant only from an object.
                if NETCDF_NAMES.contains(&name) {
                    return Ok(OutputFormat::NetCDF(NetcdfOutputOptions::default()));
                }
                OutputFormat::deserialize(serde::de::value::StrDeserializer::new(name))
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<OutputFormat, A::Error> {
                OutputFormat::deserialize(serde::de::value::MapAccessDeserializer::new(map))
            }
        }

        deserializer.deserialize_any(FormatVisitor)
    }
}

/// The OpenAPI schema of [`OutputFormat`]: the derived variants, plus the plain NetCDF names.
struct OutputFormatSchema;

impl utoipa::PartialSchema for OutputFormatSchema {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        use utoipa::openapi::{schema::Type, ObjectBuilder, RefOr, Schema};

        let mut schema = <OutputFormat as utoipa::PartialSchema>::schema();
        if let RefOr::T(Schema::OneOf(one_of)) = &mut schema {
            let names = ObjectBuilder::new()
                .schema_type(Type::String)
                .enum_values(Some(NETCDF_NAMES))
                .description(Some(
                    "Flat NetCDF format with the default options. `hdf5` and `h5` are aliases.",
                ))
                .build();
            // Put the plain names next to the object form of the same format.
            let at = one_of
                .items
                .iter()
                .position(|item| {
                    matches!(item, RefOr::T(Schema::Object(object)) if object.properties.contains_key("netcdf"))
                })
                .unwrap_or(one_of.items.len());
            one_of.items.insert(at, Schema::Object(names).into());
        }
        schema
    }
}

impl utoipa::ToSchema for OutputFormatSchema {
    fn name() -> std::borrow::Cow<'static, str> {
        <OutputFormat as utoipa::ToSchema>::name()
    }

    fn schemas(
        schemas: &mut Vec<(
            String,
            utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>,
        )>,
    ) {
        <OutputFormat as utoipa::ToSchema>::schemas(schemas);
    }
}

impl serde::Serialize for OutputFormat {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            OutputFormat::NetCDF(options) if *options == NetcdfOutputOptions::default() => {
                serializer.serialize_str("netcdf")
            }
            _ => OutputFormat::serialize(self, serializer),
        }
    }
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
            OutputFormat::NetCDF(_) => OutputFileKind::NetCDF,
            OutputFormat::NdNetCDF { .. } => OutputFileKind::NetCDF,
            OutputFormat::Odv(_) => OutputFileKind::Odv,
            OutputFormat::Zarr => OutputFileKind::Zarr,
        }
    }

    /// Returns the DataFusion file type for this output format.
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
            } => Ok(format_as_file_type(Arc::new(GeoParquetFormatFactory::new(
                GeoParquetOptions {
                    longitude_column: longitude_column.clone(),
                    latitude_column: latitude_column.clone(),
                },
            )))),
            OutputFormat::NetCDF(options) => {
                let registered = session
                    .state()
                    .get_file_format_factory(NETCDF_EXTENSION)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "NetCDF format factory not registered under extension '{}'",
                            NETCDF_EXTENSION
                        )
                    })?;
                if *options == NetcdfOutputOptions::default() {
                    return Ok(format_as_file_type(registered));
                }
                // The COPY sink reads its options from the factory, so clone the registered one.
                let mut factory = registered
                    .downcast_ref::<NetCDFFormatFactory>()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "the format factory under extension '{}' is not the NetCDF factory",
                            NETCDF_EXTENSION
                        )
                    })?
                    .clone();
                factory.options.writer = options.clone();
                Ok(format_as_file_type(Arc::new(factory)))
            }
            OutputFormat::NdNetCDF { dimension_columns } => {
                let registered = session
                    .state()
                    .get_file_format_factory(NETCDF_EXTENSION)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "NetCDF format factory not registered under extension '{}'",
                            NETCDF_EXTENSION
                        )
                    })?;
                // The COPY sink reads its write dimensions from the factory, so clone the
                // registered one and set them there.
                let mut factory = registered
                    .downcast_ref::<NetCDFFormatFactory>()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "the format factory under extension '{}' is not the NetCDF factory",
                            NETCDF_EXTENSION
                        )
                    })?
                    .clone();
                factory.options.unique_value_columns = dimension_columns.clone();
                factory.options.write_dimensions = Some(dimension_columns.clone());

                Ok(format_as_file_type(Arc::new(factory)))
            }
            OutputFormat::Odv(options) => Ok(format_as_file_type(Arc::new(
                OdvFileFormatFactory::new(Some(options.clone())),
            ))),
            OutputFormat::Zarr => {
                let registered = session
                    .state()
                    .get_file_format_factory(ZARR_EXTENSION)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Zarr format factory not registered under extension '{}'",
                            ZARR_EXTENSION
                        )
                    })?;
                // The registered factory carries the output directory; clone it to set zipping.
                let mut factory = registered
                    .downcast_ref::<ZarrFormatFactory>()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "the format factory under extension '{}' is not the Zarr factory",
                            ZARR_EXTENSION
                        )
                    })?
                    .clone();
                // A response carries one file, so the store directory is zipped.
                factory.write_options.zip_output = true;

                Ok(format_as_file_type(Arc::new(factory)))
            }
        }
    }
}
