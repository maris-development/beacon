//! Defines the `From` and `FromFormat` enums for specifying data sources in queries.
//!
//! Provides methods to initialize logical plan builders and convert formats to DataFusion table sources.

use std::sync::Arc;

use beacon_data_lake::{
    files::collection::FileCollection, table::table_formats::NetCDFFileFormat, DataLake,
};
use beacon_formats::netcdf::NetcdfOptions;
use beacon_formats::zarr::statistics::ZarrStatisticsSelection;
use beacon_formats::{
    arrow::ArrowFormat, csv::CsvFormat, odv_ascii::OdvFormat, parquet::ParquetFormat,
};
use datafusion::catalog::SchemaProvider;
use datafusion::{
    datasource::{file_format::FileFormat, listing::ListingTableUrl, provider_as_source},
    logical_expr::{LogicalPlanBuilder, TableSource},
    prelude::SessionContext,
};
use utoipa::ToSchema;

/// Specifies the source of data for a query.
///
/// Can be either a named table or a file format with associated paths.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
#[serde(deny_unknown_fields)]
pub enum From {
    /// Reference to a registered table by name.
    #[serde(untagged)]
    Table(String),
    /// Reference to a file format and its configuration.
    #[serde(untagged)]
    Format {
        #[serde(flatten)]
        format: FromFormat,
    },
}

impl Default for From {
    /// Returns the default table as specified in the configuration.
    fn default() -> Self {
        From::Table(beacon_config::CONFIG.default_table.clone())
    }
}

impl From {
    /// Initializes a [`LogicalPlanBuilder`] for the specified data source.
    ///
    /// # Arguments
    /// * `session_context` - The DataFusion session context.
    /// * `data_lake` - The data lake instance for resolving file paths.
    ///
    /// # Returns
    /// * `LogicalPlanBuilder` for the specified source.
    pub async fn init_builder(
        &self,
        session_context: &SessionContext,
        data_lake: &DataLake,
        projection: Option<&Vec<String>>,
    ) -> datafusion::error::Result<LogicalPlanBuilder> {
        match self {
            From::Table(name) => {
                // Use a registered table.
                let table = data_lake.table(name).await?;
                if let Some(mut table) = table {
                    if let (Some(projection), Some(file_collection)) =
                        (projection, table.as_any().downcast_ref::<FileCollection>())
                    {
                        let projected_table =
                            file_collection.with_pushdown_projection(projection.clone())?;
                        table = Arc::new(projected_table);
                    }
                    let source = provider_as_source(table);

                    return LogicalPlanBuilder::scan(name, source, None);
                }

                let table = session_context.table(name).await?;
                Ok(LogicalPlanBuilder::new(table.into_parts().1))
            }
            From::Format { format } => {
                // Use a file format as a table source.
                let table_source = format.as_table_source(session_context, data_lake).await?;
                Ok(LogicalPlanBuilder::scan("tmp", table_source, None)?)
            }
        }
    }
}

/// Supported file formats for loading data.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
#[serde(deny_unknown_fields)]
pub enum FromFormat {
    /// CSV format with optional delimiter and file paths.
    #[serde(rename = "csv")]
    Csv {
        /// Optional delimiter character (defaults to ',').
        delimiter: Option<char>,
        /// List of file paths.
        paths: Vec<String>,
    },
    /// Parquet format with file paths.
    #[serde(rename = "parquet")]
    Parquet { paths: Vec<String> },
    /// Arrow format with file paths.
    #[serde(rename = "arrow")]
    Arrow { paths: Vec<String> },
    /// NetCDF format with file paths (not yet implemented).
    #[serde(rename = "netcdf")]
    NetCDF { paths: Vec<String> },
    #[serde(rename = "odv")]
    Odv { paths: Vec<String> },
    #[serde(rename = "zarr")]
    Zarr {
        paths: Vec<String>,
        statistics_columns: Option<Vec<String>>,
    },
}

impl FromFormat {
    /// Converts the format to a DataFusion [`TableSource`].
    ///
    /// # Arguments
    /// * `session_context` - The DataFusion session context.
    /// * `data_lake` - The data lake instance for resolving file paths.
    ///
    /// # Returns
    /// * `Arc<dyn TableSource>` for the specified format.
    pub async fn as_table_source(
        &self,
        session_context: &SessionContext,
        data_lake: &DataLake,
    ) -> datafusion::error::Result<Arc<dyn TableSource>> {
        let file_format = self.file_format(session_context).await?;
        let urls = self.listing_table_urls(data_lake)?;

        // Create a FileCollection as the table provider.
        let table =
            Arc::new(FileCollection::new(&session_context.state(), file_format, urls).await?);

        Ok(provider_as_source(table))
    }

    /// Returns the corresponding [`FileFormat`] for the variant.
    async fn file_format(
        &self,
        _session_context: &SessionContext,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        match self {
            FromFormat::Csv { delimiter, .. } => Ok(Arc::new(CsvFormat::new(
                delimiter.unwrap_or(',') as u8,
                10_000,
            ))),
            FromFormat::Parquet { .. } => Ok(Arc::new(ParquetFormat::new())),
            FromFormat::Arrow { .. } => Ok(Arc::new(ArrowFormat::new())),
            FromFormat::NetCDF { .. } => Ok(Arc::new(beacon_formats::netcdf::NetcdfFormat::new(
                beacon_object_storage::get_datasets_object_store().await,
                NetcdfOptions::default(),
            ))),
            FromFormat::Odv { .. } => Ok(Arc::new(OdvFormat::new())),
            FromFormat::Zarr {
                statistics_columns, ..
            } => {
                let zarr_selections = statistics_columns
                    .clone()
                    .map(|columns| Arc::new(ZarrStatisticsSelection { columns }));
                let zarr_format = beacon_formats::zarr::ZarrFormat::default()
                    .with_zarr_statistics(zarr_selections);

                Ok(Arc::new(zarr_format))
            }
        }
    }

    /// Resolves file paths to [`ListingTableUrl`]s using the data lake.
    fn listing_table_urls(
        &self,
        data_lake: &DataLake,
    ) -> datafusion::error::Result<Vec<ListingTableUrl>> {
        let paths = match self {
            FromFormat::Csv { paths, .. }
            | FromFormat::Parquet { paths }
            | FromFormat::Arrow { paths }
            | FromFormat::NetCDF { paths }
            | FromFormat::Odv { paths }
            | FromFormat::Zarr { paths, .. } => paths,
        };

        let mut listing_table_urls = Vec::with_capacity(paths.len());
        for path in paths {
            let url = data_lake.try_create_listing_url(path.to_string())?;
            listing_table_urls.push(url);
        }
        Ok(listing_table_urls)
    }
}
