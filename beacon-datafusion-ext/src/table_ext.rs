//! Serializable table definitions used to rebuild DataFusion table providers.
//!
//! This module contains persisted definitions for listing tables and SQL view tables,
//! together with helper logic that normalizes schema and partition metadata during
//! provider creation.

use std::borrow::Cow;
use std::sync::Arc;

use beacon_common::listing_url::parse_listing_table_url;
use datafusion::catalog::Session;
use datafusion::common::{Constraints, DataFusionError, Statistics, not_impl_err};
use datafusion::datasource::{TableType, ViewTable};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{Expr, SQLOptions};
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
/// A serializable table definition that can materialize a DataFusion provider.
pub trait TableDefinition {
    /// Builds a concrete [`TableProvider`] from this definition.
    ///
    /// Implementations use the provided session context and store URL to resolve
    /// formats, schemas, and physical locations.
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
        data_store_url: &ObjectStoreUrl,
    ) -> anyhow::Result<Arc<dyn TableProvider>>;
}

#[derive(Clone, Debug)]
pub struct ExternalTable {
    definition: ExternalTableDefinition,
    inner: ListingTable,
}

impl ExternalTable {
    pub fn new(definition: ExternalTableDefinition, inner: ListingTable) -> Self {
        Self { definition, inner }
    }

    pub fn definition(&self) -> &ExternalTableDefinition {
        &self.definition
    }
}

#[async_trait::async_trait]
impl TableProvider for ExternalTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    /// Get the create statement used to create this table, if available.
    fn get_table_definition(&self) -> Option<&str> {
        self.definition.definition.as_deref()
    }

    /// Get the [`LogicalPlan`] of this table, if available.
    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    /// Get the default value for a column, if available.
    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner
            .scan(state, projection, filters, limit)
            .await
            .map_err(|e| DataFusionError::Execution(format!("ExternalTable scan error: {e}")))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Insert into ExternalTable is not supported")
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
/// Persisted configuration for an External Table.
pub struct ExternalTableDefinition {
    /// Logical table name.
    pub name: String,
    /// Data location, relative to the object-store base URL, optionally with a glob.
    pub location: String,
    /// File format identifier, such as `parquet`, `csv`, or `json`.
    pub file_type: String,
    /// Optional explicit schema; empty means infer from files.
    pub schema: SchemaRef,
    /// Optional SQL text associated with the table definition.
    pub definition: Option<String>,
    /// Partition column names encoded in folder paths.
    pub partition_cols: Vec<String>,
    /// Additional file-format specific options.
    pub options: std::collections::HashMap<String, String>,
    /// If true, creation should no-op when the target table already exists.
    pub if_not_exists: bool,
}

/// Converts a concrete listing table path into the location format expected by `parse_listing_table_url`.
fn listing_location_from_table_path(
    table_path: &datafusion::datasource::listing::ListingTableUrl,
) -> anyhow::Result<String> {
    let full = table_path.as_str();
    let store_url = table_path.object_store().to_string();

    let mut location = full
        .strip_prefix(&store_url)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "ListingTable path '{}' does not start with object store URL '{}'",
                full,
                store_url
            )
        })?
        .to_string();

    if table_path.scheme() != "file" {
        location = location.trim_start_matches('/').to_string();
    }

    if let Some(glob) = table_path.get_glob() {
        if !location.is_empty() && !location.ends_with('/') {
            location.push('/');
        }
        location.push_str(&glob.to_string());
    }

    anyhow::ensure!(
        !location.is_empty(),
        "Derived empty location from ListingTable path '{}'",
        full
    );

    Ok(location)
}

/// Best-effort file type inference for serialized listing table definitions.
fn infer_file_type(options: &ListingOptions) -> String {
    let from_file_extension = options
        .file_extension
        .trim()
        .trim_start_matches('.')
        .to_string();
    if !from_file_extension.is_empty() {
        return from_file_extension;
    }

    if let Some(compression) = options.format.compression_type()
        && let Ok(ext) = options.format.get_ext_with_compression(&compression)
    {
        let inferred = ext.trim().trim_start_matches('.').to_string();
        if !inferred.is_empty() {
            return inferred;
        }
    }

    options
        .format
        .get_ext()
        .trim()
        .trim_start_matches('.')
        .to_string()
}

/// Partition column declarations represented as `(name, data_type)` tuples.
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
impl TableDefinition for ExternalTableDefinition {
    /// Builds a [`ListingTable`] provider from the persisted listing-table definition.
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

        let config = ListingTableConfig::new(listing_table_url)
            .with_listing_options(options)
            .with_schema(resolved_schema);
        let provider = ListingTable::try_new(config)?.with_cache(
            session_state
                .runtime_env()
                .cache_manager
                .get_file_statistic_cache(),
        );
        let table = provider.with_definition(self.definition.clone());
        Ok(Arc::new(ExternalTable::new(self.clone(), table)))
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
/// Persisted configuration for a SQL-defined [`ViewTable`].
pub struct ViewTableDefinition {
    /// Logical view name.
    pub name: String,
    /// SQL query used to materialize the view.
    pub definition: String,
}

impl ViewTableDefinition {
    /// Builds a serializable view definition from an existing [`ViewTable`].
    ///
    /// Returns an error when the input view has no persisted SQL definition.
    pub fn try_from_view(table: &ViewTable) -> anyhow::Result<Self> {
        match table.definition() {
            Some(def) => Ok(Self {
                name: "view".to_string(), // Name is not used for view tables, so we can set a default value here
                definition: def.clone(),
            }),
            None => Err(anyhow::anyhow!(
                "ViewTableDefinition requires a SQL definition to be created from a ViewTable without a definition"
            )),
        }
    }

    pub async fn into_view_table(self, context: Arc<SessionContext>) -> anyhow::Result<ViewTable> {
        let options = SQLOptions::new().with_allow_ddl(true);
        let df = context.sql_with_options(&self.definition, options).await?;
        Ok(ViewTable::new(
            df.logical_plan().clone(),
            Some(self.definition),
        ))
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "view_table")]
impl TableDefinition for ViewTableDefinition {
    /// Compiles the stored SQL and returns a DataFusion [`ViewTable`] provider.
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
        _data_store_url: &ObjectStoreUrl,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        // Compile the SQL definition into a DataFusion logical plan and use it to create a ViewTable provider.
        let state = context.state();
        let plan = state.create_logical_plan(&self.definition).await?;

        Ok(Arc::new(ViewTable::new(
            plan,
            Some(self.definition.clone()),
        )))
    }
}

#[cfg(test)]
/// Unit tests covering listing-table and view-table definition behavior.
mod tests {
    use super::{ExternalTableDefinition, TableDefinition, ViewTableDefinition};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::ViewTable;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::prelude::SessionContext;
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Creates a unique temporary directory for table fixture data.
    fn create_temp_dir(prefix: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        dir.push(format!("{prefix}_{}_{}", std::process::id(), ts));
        fs::create_dir_all(&dir).expect("temporary directory should be created");
        dir
    }

    /// Converts an absolute local path into a location relative to `file://` root.
    fn to_store_relative_location(path: &std::path::Path) -> String {
        path.to_string_lossy().trim_start_matches('/').to_string()
    }

    #[tokio::test]
    /// Verifies listing providers can infer schema from folder-backed files.
    async fn listing_table_definition_build_provider_infers_schema_from_folder() {
        let root = create_temp_dir("table_ext_listing_infer");
        let data_file = root.join("part-0.csv");
        fs::write(&data_file, "1\n2\n").expect("csv fixture should be written");

        let definition = ExternalTableDefinition {
            name: "t_csv".to_string(),
            location: format!("{}/", to_store_relative_location(&root)),
            file_type: "csv".to_string(),
            schema: Arc::new(Schema::empty()),
            definition: Some("SELECT value FROM t_csv".to_string()),
            partition_cols: vec![],
            options: HashMap::new(),
            if_not_exists: false,
        };

        let context = Arc::new(SessionContext::new());
        let store_url = ObjectStoreUrl::parse("file://").unwrap();

        let provider = definition
            .build_provider(context, &store_url)
            .await
            .expect("listing provider should be built from inferred schema");

        let schema = provider.schema();
        assert_eq!(schema.fields().len(), 1);
        assert!(!schema.field(0).name().is_empty());

        fs::remove_dir_all(&root).expect("temporary directory should be cleaned up");
    }

    #[tokio::test]
    /// Verifies explicit schemas and partition metadata produce a valid provider schema.
    async fn listing_table_definition_build_provider_projects_partition_columns() {
        let root = create_temp_dir("table_ext_listing_partition");
        let partitioned = root.join("year=2026");
        fs::create_dir_all(&partitioned).expect("partition folder should be created");
        fs::write(partitioned.join("part-0.csv"), "1\n")
            .expect("partition csv fixture should be written");

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, true),
            Field::new("year", DataType::Utf8, true),
        ]));

        let definition = ExternalTableDefinition {
            name: "t_partitioned".to_string(),
            location: format!("{}/", to_store_relative_location(&root)),
            file_type: "csv".to_string(),
            schema,
            definition: None,
            partition_cols: vec!["year".to_string()],
            options: HashMap::new(),
            if_not_exists: false,
        };

        let context = Arc::new(SessionContext::new());
        let store_url = ObjectStoreUrl::parse("file://").unwrap();

        let provider = definition
            .build_provider(context, &store_url)
            .await
            .expect("listing provider should be built from explicit schema");

        let schema = provider.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "value");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).name(), "year");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

        fs::remove_dir_all(&root).expect("temporary directory should be cleaned up");
    }

    #[tokio::test]
    /// Verifies unknown file formats fail provider construction with a clear error.
    async fn listing_table_definition_build_provider_rejects_unknown_file_type() {
        let definition = ExternalTableDefinition {
            name: "t_bad".to_string(),
            location: "tmp".to_string(),
            file_type: "not_a_real_format".to_string(),
            schema: Arc::new(Schema::empty()),
            definition: None,
            partition_cols: vec![],
            options: HashMap::new(),
            if_not_exists: false,
        };

        let context = Arc::new(SessionContext::new());
        let store_url = ObjectStoreUrl::parse("file://").unwrap();

        let err = definition
            .build_provider(context, &store_url)
            .await
            .expect_err("unknown file type should fail");
        let msg = err.to_string();

        assert!(msg.contains("Could not find FileFormat"), "error: {msg}");
    }

    #[tokio::test]
    /// Verifies extracting a definition from a view succeeds when SQL is present.
    async fn view_table_definition_try_from_view_reads_definition() {
        let context = SessionContext::new();
        let plan = context
            .state()
            .create_logical_plan("SELECT 1 AS x")
            .await
            .unwrap();
        let view = ViewTable::new(plan, Some("SELECT 1 AS x".to_string()));

        let definition = ViewTableDefinition::try_from_view(&view).unwrap();
        assert_eq!(definition.name, "view");
        assert_eq!(definition.definition, "SELECT 1 AS x");
    }

    #[tokio::test]
    /// Verifies extracting a definition from a view fails when SQL is absent.
    async fn view_table_definition_try_from_view_requires_definition() {
        let context = SessionContext::new();
        let plan = context
            .state()
            .create_logical_plan("SELECT 1 AS x")
            .await
            .unwrap();
        let view = ViewTable::new(plan, None);

        let err = ViewTableDefinition::try_from_view(&view).expect_err("missing definition");
        assert!(err.to_string().contains("requires a SQL definition"));
    }

    #[tokio::test]
    /// Verifies building a view definition yields a downcastable [`ViewTable`].
    async fn view_table_definition_build_provider_creates_view_table() {
        let definition = ViewTableDefinition {
            name: "my_view".to_string(),
            definition: "SELECT 42 AS answer".to_string(),
        };

        let context = Arc::new(SessionContext::new());
        let store_url = ObjectStoreUrl::parse("file://").unwrap();

        let provider = definition
            .build_provider(context, &store_url)
            .await
            .expect("view provider should be built");

        let view = provider
            .as_any()
            .downcast_ref::<ViewTable>()
            .expect("provider should be a ViewTable");
        assert_eq!(
            view.definition().cloned(),
            Some("SELECT 42 AS answer".to_string())
        );
    }
}
