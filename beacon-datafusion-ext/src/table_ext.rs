use std::sync::Arc;

use beacon_common::listing_url::parse_listing_table_url;
use datafusion::common::DataFusionError;
use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    catalog::TableProvider,
    common::{arrow_datafusion_err, config_datafusion_err},
    datasource::listing::{ListingOptions, ListingTable, ListingTableConfig},
    execution::object_store::ObjectStoreUrl,
    prelude::SessionContext,
};

#[typetag::serde(tag = "type")]
#[async_trait::async_trait]
pub trait TableDefinition {
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
        data_store_url: &ObjectStoreUrl,
    ) -> anyhow::Result<Arc<dyn TableProvider>>;
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ListingTableDefinition {
    pub name: String,
    pub location: String, // The URL of the physical file, e.g., "/path/to/file.parquet"
    pub file_type: String, // The file format, e.g., "parquet", "csv", etc.
    pub schema: SchemaRef, // Optional schema; if not provided, it will be inferred
    pub definition: Option<String>, // Optional additional sql definition for the table
    pub partition_cols: Vec<String>, // Optional list of partition columns
    pub options: std::collections::HashMap<String, String>, // Optional additional options for the listing table
    pub if_not_exists: bool, // Whether to create the table only if it does not already exist
}

type PartitionCols = Vec<(String, DataType)>;

/// Resolve the user-provided schema and partition columns for CREATE EXTERNAL TABLE.
///
/// When no schema is provided, partition columns are represented as Dictionary(UInt16, Utf8)
/// to match DataFusion's listing-table defaults.
fn resolve_schema_and_partition_cols(
    schema: &SchemaRef,
    table_partition_cols: &[String],
) -> datafusion::error::Result<(Option<SchemaRef>, PartitionCols)> {
    if schema.fields().is_empty() {
        let partition_cols = dictionary_partition_cols(table_partition_cols);
        return Ok((None, partition_cols));
    }

    let schema: SchemaRef = Arc::new(schema.as_ref().to_owned());
    let partition_cols = partition_cols_from_schema(&schema, table_partition_cols)?;
    let projected_schema = project_out_partition_columns(&schema, table_partition_cols)?;

    Ok((Some(projected_schema), partition_cols))
}

/// Build default partition column types used when schema is inferred from files.
fn dictionary_partition_cols(partition_cols: &[String]) -> PartitionCols {
    partition_cols
        .iter()
        .map(|name| {
            (
                name.clone(),
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            )
        })
        .collect()
}

/// Resolve partition column types from an explicit schema.
fn partition_cols_from_schema(
    schema: &SchemaRef,
    partition_cols: &[String],
) -> datafusion::error::Result<PartitionCols> {
    partition_cols
        .iter()
        .map(|col| {
            schema
                .field_with_name(col)
                .map(|f| (f.name().to_owned(), f.data_type().to_owned()))
                .map_err(|e| arrow_datafusion_err!(e))
        })
        .collect()
}

/// Exclude partition columns from the file schema for partitioned external tables.
fn project_out_partition_columns(
    schema: &SchemaRef,
    partition_cols: &[String],
) -> datafusion::error::Result<SchemaRef> {
    let mut project_idx = Vec::new();
    for i in 0..schema.fields().len() {
        if !partition_cols.contains(schema.field(i).name()) {
            project_idx.push(i);
        }
    }

    Ok(Arc::new(schema.project(&project_idx)?))
}

/// Apply a default glob for folder locations when no explicit glob was provided.
fn maybe_apply_default_glob(
    mut listing_table_url: datafusion::datasource::listing::ListingTableUrl,
    options: &ListingOptions,
    file_type: &str,
) -> datafusion::error::Result<datafusion::datasource::listing::ListingTableUrl> {
    if listing_table_url.is_folder() && listing_table_url.get_glob().is_none() {
        let file_glob = inferred_file_glob(options, file_type);
        listing_table_url = listing_table_url.with_glob(file_glob.as_ref())?;
    }

    Ok(listing_table_url)
}

/// Infer a listing glob from the format extension, falling back to file_type.
fn inferred_file_glob(options: &ListingOptions, file_type: &str) -> String {
    match options.format.compression_type() {
        Some(compression) => match options.format.get_ext_with_compression(&compression) {
            Ok(ext) => format!("*.{ext}"),
            Err(_) => fallback_file_glob(file_type),
        },
        None => fallback_file_glob(file_type),
    }
}

/// Build the fallback glob for file_type values used by CREATE EXTERNAL TABLE.
fn fallback_file_glob(file_type: &str) -> String {
    format!("**/*.{}", file_type.to_lowercase())
}

#[async_trait::async_trait]
#[typetag::serde(name = "listing_table")]
impl TableDefinition for ListingTableDefinition {
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
        data_store_url: &ObjectStoreUrl,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let session_state = context.state();
        let file_format_factory = session_state
            .get_file_format_factory(self.file_type.as_str())
            .ok_or(config_datafusion_err!(
                "Unable to create table with format {}! Could not find FileFormat.",
                self.file_type
            ))?;

        let file_format = file_format_factory.create(&session_state, &self.options)?;

        let (provided_schema, table_partition_cols) =
            resolve_schema_and_partition_cols(&self.schema, &self.partition_cols)?;

        let mut listing_table_url = parse_listing_table_url(data_store_url, &self.location)?;

        let options = ListingOptions::new(file_format)
            .with_file_extension("") // file extension is not needed for listing table factory since the file format will handle it in `infer_schema` and `infer_partition_schema`
            .with_session_config_options(session_state.config())
            .with_table_partition_cols(table_partition_cols);

        options
            .validate_partitions(&session_state, &listing_table_url)
            .await?;

        let resolved_schema = match provided_schema {
            None => {
                listing_table_url =
                    maybe_apply_default_glob(listing_table_url, &options, &self.file_type)?;

                options
                    .infer_schema(&session_state, &listing_table_url)
                    .await?
            }
            Some(s) => s,
        };

        let config = ListingTableConfig::new(listing_table_url).with_schema(resolved_schema);
        let provider = ListingTable::try_new(config)?.with_cache(
            session_state
                .runtime_env()
                .cache_manager
                .get_file_statistic_cache(),
        );
        let table = provider.with_definition(self.definition.clone());
        Ok(Arc::new(table))
    }
}
