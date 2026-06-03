//! Shared building blocks for listing-backed providers: the rebuild spec, the
//! listing-table builder, and schema/partition/glob helpers used by both the
//! external-table and materialized-view definitions.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{Constraints, arrow_datafusion_err};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::execution::SessionState;
use datafusion::prelude::Expr;

/// Inputs needed to rebuild the inner [`ListingTable`] for an external table.
///
/// Captured once at creation so that the table can re-infer its schema and
/// re-list its files on demand (manual `REFRESH`) or in response to storage
/// events, without going through the table manager.
#[derive(Clone, Debug)]
pub struct ExternalTableRebuild {
    pub(crate) listing_table_url: ListingTableUrl,
    pub(crate) options: ListingOptions,
    /// `None` means infer the schema from the current files on every rebuild;
    /// `Some` pins an explicit schema provided at creation time.
    pub(crate) provided_schema: Option<SchemaRef>,
    pub(crate) constraints: Constraints,
    pub(crate) column_defaults: HashMap<String, Expr>,
    pub(crate) definition_sql: Option<String>,
}

/// Build a fresh [`ListingTable`] from a rebuild spec.
///
/// When the schema is inferred and the location currently lists no files the
/// schema falls back to empty, so a table whose objects are all deleted reports
/// an empty schema rather than failing.
pub(crate) async fn build_listing_table(
    state: &SessionState,
    spec: &ExternalTableRebuild,
) -> datafusion::error::Result<ListingTable> {
    let resolved_schema = match &spec.provided_schema {
        Some(schema) => Arc::clone(schema),
        None => match spec
            .options
            .infer_schema(state, &spec.listing_table_url)
            .await
        {
            Ok(schema) => schema,
            Err(error) => {
                tracing::debug!(%error, "no objects to infer external table schema from; using empty schema");
                Arc::new(Schema::empty())
            }
        },
    };

    let config = ListingTableConfig::new(spec.listing_table_url.clone())
        .with_listing_options(spec.options.clone())
        .with_schema(resolved_schema);

    let table = ListingTable::try_new(config)?
        .with_cache(state.runtime_env().cache_manager.get_file_statistic_cache())
        .with_definition(spec.definition_sql.clone())
        .with_constraints(spec.constraints.clone())
        .with_column_defaults(spec.column_defaults.clone());

    Ok(table)
}

/// Partition column declarations represented as `(name, data_type)` tuples.
type PartitionCols = Vec<(String, DataType)>;

/// Resolve the user-provided schema and partition columns for CREATE EXTERNAL TABLE.
///
/// When no schema is provided, partition columns are represented as Dictionary(UInt16, Utf8)
/// to match DataFusion's listing-table defaults.
pub(crate) fn resolve_schema_and_partition_cols(
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
pub(crate) fn maybe_apply_default_glob(
    mut listing_table_url: ListingTableUrl,
    options: &ListingOptions,
    file_type: &str,
) -> datafusion::error::Result<ListingTableUrl> {
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
