use std::any::Any;
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use beacon_common::listing_url::parse_listing_table_url;
use datafusion::common::{
    Constraints, DataFusionError, Statistics, ToDFSchema, arrow_datafusion_err,
    config_datafusion_err, plan_err,
};
use datafusion::datasource::TableType;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::execution::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{CreateExternalTable, LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion::sql::TableReference;
use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    catalog::{Session, TableProvider, TableProviderFactory},
};

type PartitionCols = Vec<(String, DataType)>;

/// A `TableProviderFactory` capable of creating new `ListingTable`s
#[derive(Debug)]
pub struct ListingTableFactoryExt {
    store_url: ObjectStoreUrl,
}

impl ListingTableFactoryExt {
    /// Creates a new `ListingTableFactoryExt`
    pub fn new(store_url: ObjectStoreUrl) -> Self {
        Self { store_url }
    }
}

#[async_trait::async_trait]
impl TableProviderFactory for ListingTableFactoryExt {
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let session_state = as_session_state(state)?;
        let file_format_factory = session_state
            .get_file_format_factory(cmd.file_type.as_str())
            .ok_or(config_datafusion_err!(
                "Unable to create table with format {}! Could not find FileFormat.",
                cmd.file_type
            ))?;

        let file_format = file_format_factory.create(session_state, &cmd.options)?;

        let (provided_schema, table_partition_cols) = resolve_schema_and_partition_cols(cmd)?;

        let mut listing_table_url = parse_listing_table_url(&self.store_url, &cmd.location)?;

        println!("Creating listing table with URL: {listing_table_url}");

        let options = ListingOptions::new(file_format)
            .with_file_extension("") // file extension is not needed for listing table factory since the file format will handle it in `infer_schema` and `infer_partition_schema`
            .with_session_config_options(session_state.config())
            .with_table_partition_cols(table_partition_cols);

        options
            .validate_partitions(session_state, &listing_table_url)
            .await?;

        let resolved_schema = match provided_schema {
            None => {
                listing_table_url = maybe_apply_default_glob(listing_table_url, &options, cmd)?;

                let schema = options
                    .infer_schema(session_state, &listing_table_url)
                    .await?;
                let df_schema = Arc::clone(&schema).to_dfschema()?;
                let column_refs: HashSet<_> = cmd
                    .order_exprs
                    .iter()
                    .flat_map(|sort| sort.iter())
                    .flat_map(|s| s.expr.column_refs())
                    .collect();

                for column in &column_refs {
                    if !df_schema.has_column(column) {
                        return plan_err!("Column {column} is not in schema");
                    }
                }

                schema
            }
            Some(s) => s,
        };

        let config = ListingTableConfig::new(listing_table_url)
            .with_listing_options(options.with_file_sort_order(cmd.order_exprs.clone()))
            .with_schema(resolved_schema);
        let provider = ListingTable::try_new(config)?
            .with_cache(state.runtime_env().cache_manager.get_file_statistic_cache());
        let table = provider
            .with_definition(cmd.definition.clone())
            .with_constraints(cmd.constraints.clone())
            .with_column_defaults(cmd.column_defaults.clone());
        Ok(Arc::new(table))
    }
}

/// Downcast a generic DataFusion session reference into SessionState.
fn as_session_state(state: &dyn Session) -> datafusion::error::Result<&SessionState> {
    state
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or(config_datafusion_err!(
            "Expected SessionState when creating external listing table"
        ))
}

/// Resolve the user-provided schema and partition columns for CREATE EXTERNAL TABLE.
///
/// When no schema is provided, partition columns are represented as Dictionary(UInt16, Utf8)
/// to match DataFusion's listing-table defaults.
fn resolve_schema_and_partition_cols(
    cmd: &CreateExternalTable,
) -> datafusion::error::Result<(Option<SchemaRef>, PartitionCols)> {
    if cmd.schema.fields().is_empty() {
        let partition_cols = dictionary_partition_cols(&cmd.table_partition_cols);
        return Ok((None, partition_cols));
    }

    let schema: SchemaRef = Arc::new(cmd.schema.as_ref().to_owned().into());
    let partition_cols = partition_cols_from_schema(&schema, &cmd.table_partition_cols)?;
    let projected_schema = project_out_partition_columns(&schema, &cmd.table_partition_cols)?;

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
    cmd: &CreateExternalTable,
) -> datafusion::error::Result<datafusion::datasource::listing::ListingTableUrl> {
    if listing_table_url.is_folder() && listing_table_url.get_glob().is_none() {
        let file_glob = inferred_file_glob(options, &cmd.file_type);
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
    format!("*.{}", file_type.to_lowercase())
}

#[derive(Debug)]
pub struct ListingTableExt {
    listing_table: ListingTable,
    table_ref: TableReference,
    session_ref: Arc<SessionState>,
    has_explicit_schema: bool,
    should_track: bool,
    refresh_on_read: bool,
    refresh_delay_ms: Option<u64>,
}

impl ListingTableExt {
    pub fn new(
        listing_table: ListingTable,
        table_ref: TableReference,
        session_ref: Arc<SessionState>,
        has_explicit_schema: bool,
        should_track: bool,
        refresh_on_read: bool,
        refresh_delay_ms: Option<u64>,
    ) -> Self {
        Self {
            listing_table,
            table_ref,
            session_ref,
            has_explicit_schema,
            should_track,
            refresh_on_read,
            refresh_delay_ms,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for ListingTableExt {
    /// Returns the table provider as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        self.listing_table.schema()
    }

    /// Get a reference to the constraints of the table.
    /// Returns:
    /// - `None` for tables that do not support constraints.
    /// - `Some(&Constraints)` for tables supporting constraints.
    /// Therefore, a `Some(&Constraints::empty())` return value indicates that
    /// this table supports constraints, but there are no constraints.
    fn constraints(&self) -> Option<&Constraints> {
        self.listing_table.constraints()
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        self.listing_table.table_type()
    }

    /// Get the create statement used to create this table, if available.
    fn get_table_definition(&self) -> Option<&str> {
        self.listing_table.get_table_definition()
    }

    /// Get the [`LogicalPlan`] of this table, if available.
    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        self.listing_table.get_logical_plan()
    }

    /// Get the default value for a column, if available.
    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        self.listing_table.get_column_default(_column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.listing_table
            .scan(state, projection, filters, limit)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        self.listing_table.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.listing_table.statistics()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        dictionary_partition_cols, fallback_file_glob, partition_cols_from_schema,
        project_out_partition_columns,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use std::sync::Arc;

    #[test]
    fn dictionary_partition_cols_uses_dictionary_utf8_type() {
        let cols = dictionary_partition_cols(&["year".to_string(), "month".to_string()]);

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].0, "year");
        assert_eq!(
            cols[0].1,
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
        );
    }

    #[test]
    fn partition_cols_from_schema_resolves_declared_types() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, true),
            Field::new("year", DataType::Utf8, true),
        ]));

        let cols = partition_cols_from_schema(&schema, &["year".to_string()]).unwrap();
        assert_eq!(cols, vec![("year".to_string(), DataType::Utf8)]);
    }

    #[test]
    fn project_out_partition_columns_removes_partition_fields() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, true),
            Field::new("year", DataType::Utf8, true),
            Field::new("month", DataType::Utf8, true),
        ]));

        let projected =
            project_out_partition_columns(&schema, &["year".to_string(), "month".to_string()])
                .unwrap();

        assert_eq!(projected.fields().len(), 1);
        assert_eq!(projected.field(0).name(), "value");
    }

    #[test]
    fn fallback_file_glob_lowercases_file_type() {
        assert_eq!(fallback_file_glob("ATLAS"), "*.atlas");
        assert_eq!(fallback_file_glob("ParQuet"), "*.parquet");
    }
}
