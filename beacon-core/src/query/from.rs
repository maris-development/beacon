//! Defines the `From` and `FromFormat` enums for specifying data sources in queries.
//!
//! Provides methods to initialize logical plan builders and convert formats to DataFusion table sources.

use std::sync::Arc;

use beacon_arrow_csv::datafusion::CsvFormat;
use beacon_arrow_odv::datafusion::OdvFormat;
use beacon_datafusion_ext::file_collection::FileCollection;
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

impl From {
    /// Initializes a [`LogicalPlanBuilder`] for the specified data source.
    ///
    /// # Arguments
    /// * `session_context` - The DataFusion session context.
    ///
    /// # Returns
    /// * `LogicalPlanBuilder` for the specified source.
    pub async fn init_builder(
        &self,
        session_context: &SessionContext,
        projection: Option<&Vec<String>>,
    ) -> datafusion::error::Result<LogicalPlanBuilder> {
        match self {
            From::Table(name) => {
                // Use a registered table from the catalog.
                if let Ok(mut table) = session_context.table_provider(name.as_str()).await {
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
                let table_source = format.as_table_source(session_context).await?;
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
    #[serde(rename = "tiff")]
    Tiff { paths: Vec<String> },
    #[serde(rename = "zarr")]
    Zarr { paths: Vec<String> },
    #[serde(rename = "bbf")]
    Bbf { paths: Vec<String> },
}

impl FromFormat {
    /// Converts the format to a DataFusion [`TableSource`].
    ///
    /// # Arguments
    /// * `session_context` - The DataFusion session context.
    ///
    /// # Returns
    /// * `Arc<dyn TableSource>` for the specified format.
    pub async fn as_table_source(
        &self,
        session_context: &SessionContext,
    ) -> datafusion::error::Result<Arc<dyn TableSource>> {
        let file_format = self.file_format(session_context).await?;
        let urls = self.listing_table_urls()?;

        // Create a FileCollection as the table provider.
        let table =
            Arc::new(FileCollection::new(&session_context.state(), file_format, urls).await?);

        Ok(provider_as_source(table))
    }

    /// Returns the corresponding [`FileFormat`] for the variant.
    ///
    /// Formats that have a factory registered on the session are built through
    /// that factory, so the JSON `FROM` path shares the runtime's configured
    /// format (and, for store-backed formats, its reader cache) instead of
    /// constructing a default of its own. CSV (carries a per-query delimiter the
    /// factory does not accept) and ODV (no registered factory) are built
    /// directly.
    async fn file_format(
        &self,
        session_context: &SessionContext,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        match self {
            FromFormat::Csv { delimiter, .. } => Ok(Arc::new(CsvFormat::new(
                delimiter.unwrap_or(',') as u8,
                10_000,
            )) as Arc<dyn FileFormat>),
            FromFormat::Odv { .. } => Ok(Arc::new(OdvFormat::new()) as Arc<dyn FileFormat>),
            FromFormat::Parquet { .. } => file_format_from_session(session_context, "parquet"),
            FromFormat::Arrow { .. } => file_format_from_session(session_context, "arrow"),
            FromFormat::NetCDF { .. } => file_format_from_session(session_context, "nc"),
            FromFormat::Tiff { .. } => file_format_from_session(session_context, "tiff"),
            FromFormat::Zarr { .. } => file_format_from_session(session_context, "zarr"),
            FromFormat::Bbf { .. } => file_format_from_session(session_context, "bbf"),
        }
    }

    /// Resolves file paths to [`ListingTableUrl`]s under the datasets store.
    fn listing_table_urls(&self) -> datafusion::error::Result<Vec<ListingTableUrl>> {
        let paths = match self {
            FromFormat::Csv { paths, .. }
            | FromFormat::Parquet { paths }
            | FromFormat::Arrow { paths }
            | FromFormat::NetCDF { paths }
            | FromFormat::Tiff { paths }
            | FromFormat::Odv { paths }
            | FromFormat::Zarr { paths, .. } => paths,
            FromFormat::Bbf { paths } => paths,
        };

        let mut listing_table_urls = Vec::with_capacity(paths.len());
        for path in paths {
            let url = beacon_data_lake::create_listing_url(path.to_string())?;
            listing_table_urls.push(url);
        }
        Ok(listing_table_urls)
    }
}

/// Build a [`FileFormat`] from the factory registered on the session under
/// `format` (its `get_ext` identity), with default options. This shares the
/// runtime's configured factory rather than constructing a default format.
fn file_format_from_session(
    session_context: &SessionContext,
    format: &str,
) -> datafusion::error::Result<Arc<dyn FileFormat>> {
    let state = session_context.state();
    let factory = state.get_file_format_factory(format).ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(format!(
            "file format '{format}' is not registered on the session"
        ))
    })?;
    factory.create(&state, &std::collections::HashMap::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::datasource::file_format::FileFormatFactory;
    use std::collections::HashMap;

    /// (variant, the factory it should resolve) for every store/listing format
    /// that `file_format` builds via a session-registered factory.
    async fn factory_cases() -> Vec<(FromFormat, Arc<dyn FileFormatFactory>)> {
        let store = Arc::new(
            beacon_object_storage::local_datasets_store(
                beacon_config::DATASETS_DIR_PATH.to_path_buf(),
            )
            .await
            .expect("local datasets store"),
        );
        vec![
            (
                FromFormat::Parquet { paths: vec![] },
                Arc::new(beacon_arrow_parquet::datafusion::ParquetFormatFactory),
            ),
            (
                FromFormat::Arrow { paths: vec![] },
                Arc::new(beacon_arrow_ipc::datafusion::ArrowFormatFactory),
            ),
            (
                FromFormat::NetCDF { paths: vec![] },
                Arc::new(beacon_arrow_netcdf::datafusion::NetCDFFormatFactory::new(
                    store.clone(),
                    Default::default(),
                    Default::default(),
                )),
            ),
            (
                FromFormat::Tiff { paths: vec![] },
                Arc::new(beacon_arrow_tiff::datafusion::TiffFormatFactory::new(
                    Default::default(),
                )),
            ),
            (
                FromFormat::Zarr { paths: vec![] },
                Arc::new(beacon_arrow_zarr::datafusion::ZarrFormatFactory),
            ),
            (
                FromFormat::Bbf { paths: vec![] },
                Arc::new(beacon_arrow_bbf::datafusion::BBFFormatFactory::new(
                    Default::default(),
                )),
            ),
        ]
    }

    fn register(ctx: &SessionContext, factory: Arc<dyn FileFormatFactory>) {
        let state_ref = ctx.state_ref();
        let mut state = state_ref.write();
        state
            .register_file_format(factory, true)
            .expect("register file format");
    }

    /// Each factory-backed `FromFormat` resolves the factory registered on the
    /// session and builds *that* factory's format (same concrete type), rather
    /// than constructing a default of its own.
    #[tokio::test]
    async fn from_formats_resolve_the_registered_factory() {
        let ctx = SessionContext::new();
        let cases = factory_cases().await;
        for (_, factory) in &cases {
            register(&ctx, factory.clone());
        }

        for (variant, factory) in &cases {
            let expected = factory
                .create(&ctx.state(), &HashMap::new())
                .expect("factory create");
            let actual = variant
                .file_format(&ctx)
                .await
                .expect("file_format should resolve from the session");
            assert_eq!(
                actual.as_any().type_id(),
                expected.as_any().type_id(),
                "{variant:?} resolved an unexpected file format",
            );
        }
    }

    /// A beacon-specific factory-backed format errors when its factory is not
    /// registered on the session — proving `file_format` resolves from the
    /// session rather than constructing a default. (Parquet/Arrow/CSV are
    /// DataFusion built-ins registered on every `SessionContext`, so they are
    /// excluded here.)
    #[tokio::test]
    async fn factory_backed_format_errors_when_not_registered() {
        let ctx = SessionContext::new();
        for variant in [
            FromFormat::NetCDF { paths: vec![] },
            FromFormat::Tiff { paths: vec![] },
            FromFormat::Zarr { paths: vec![] },
            FromFormat::Bbf { paths: vec![] },
        ] {
            assert!(
                variant.file_format(&ctx).await.is_err(),
                "{variant:?} should fail without a registered factory",
            );
        }
    }

    /// CSV (per-query delimiter) and ODV (no registered factory) are built
    /// directly, so they resolve even on a bare session.
    #[tokio::test]
    async fn csv_and_odv_build_directly() {
        let ctx = SessionContext::new();
        assert!(FromFormat::Csv {
            delimiter: Some(';'),
            paths: vec![],
        }
        .file_format(&ctx)
        .await
        .is_ok());
        assert!(FromFormat::Odv { paths: vec![] }
            .file_format(&ctx)
            .await
            .is_ok());
    }
}
