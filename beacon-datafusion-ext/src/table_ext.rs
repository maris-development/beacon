//! Serializable table definitions used to rebuild DataFusion table providers.
//!
//! This module contains persisted definitions for listing tables and SQL view tables,
//! together with helper logic that normalizes schema and partition metadata during
//! provider creation.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Weak};

use beacon_common::listing_url::parse_listing_table_url;
use beacon_object_storage::event::ObjectEvent;
use datafusion::catalog::Session;
use datafusion::common::{Constraints, DataFusionError, Statistics, not_impl_err};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::{TableType, ViewTable};
use datafusion::execution::SessionState;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{DdlStatement, LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{Expr, SQLOptions};
use datafusion::{
    arrow::datatypes::{DataType, Schema, SchemaRef},
    catalog::TableProvider,
    common::{arrow_datafusion_err, config_datafusion_err},
    datasource::listing::{ListingOptions, ListingTable, ListingTableConfig},
    execution::object_store::ObjectStoreUrl,
    prelude::SessionContext,
};
use object_store::path::Path as ObjectPath;
use parking_lot::RwLock;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

#[typetag::serde(tag = "definition_type")]
#[async_trait::async_trait]
/// A serializable table definition that can materialize a DataFusion provider.
pub trait TableDefinition: Debug + Send + Sync {
    /// Builds a concrete [`TableProvider`] from this definition.
    ///
    /// Implementations use the provided session context and store URL to resolve
    /// formats, schemas, and physical locations.
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
        data_store_url: &ObjectStoreUrl,
    ) -> anyhow::Result<Arc<dyn TableProvider>>;

    fn depends_on(&self) -> Vec<String> {
        Vec::new()
    }

    fn table_name(&self) -> &str;

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

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

/// Aborts the background refresh task when the last clone of the owning
/// [`ExternalTable`] is dropped.
#[derive(Debug)]
pub struct RefreshListener {
    handle: JoinHandle<()>,
}

impl Drop for RefreshListener {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// The object-store URL whose events drive external-table self-refresh.
const DATASETS_STORE_URL: &str = "datasets://";

/// Subscribe to datasets-store events for a table on `data_store_url`.
///
/// Only the datasets store emits events, so tables on any other store (e.g. the
/// `file://` store used in tests) get `None` and rely on manual `REFRESH`.
/// Returns `None` as well when the datasets store does not support events.
pub(crate) async fn datasets_store_events(
    data_store_url: &ObjectStoreUrl,
) -> Option<Receiver<ObjectEvent>> {
    if data_store_url.as_str() != DATASETS_STORE_URL {
        return None;
    }
    beacon_object_storage::get_datasets_object_store()
        .await
        .subscribe()
}

/// Returns the object path an event refers to.
fn event_path(event: &ObjectEvent) -> &ObjectPath {
    match event {
        ObjectEvent::Created(meta) | ObjectEvent::Modified(meta) => &meta.location,
        ObjectEvent::Deleted(path) => path,
    }
}

/// Returns `true` when `event` points at a file located strictly under `prefix`.
///
/// Matching is segment-aware so that a prefix like `data/example` never matches
/// an unrelated sibling such as `data/example_2/...`. An empty `prefix` (the
/// store root) matches any non-empty `event`.
fn path_under_prefix(prefix: &ObjectPath, event: &ObjectPath) -> bool {
    let prefix_parts: Vec<_> = prefix.parts().collect();
    let event_parts: Vec<_> = event.parts().collect();

    if event_parts.len() <= prefix_parts.len() {
        return false;
    }

    prefix_parts
        .iter()
        .zip(event_parts.iter())
        .all(|(p, e)| p == e)
}

/// Rebuild the inner listing table from `rebuild` and swap it in.
///
/// A no-op (returning `Ok`) when the owning session context has been dropped.
async fn rebuild_into(
    inner: &Arc<RwLock<Arc<ListingTable>>>,
    rebuild: &ExternalTableRebuild,
    ctx: &Weak<SessionContext>,
) -> anyhow::Result<()> {
    let Some(context) = ctx.upgrade() else {
        return Ok(());
    };
    let state = context.state();
    let table = build_listing_table(&state, rebuild).await?;
    *inner.write() = Arc::new(table);
    Ok(())
}

#[derive(Clone, Debug)]
pub struct ExternalTable {
    definition: ExternalTableDefinition,
    inner: Arc<RwLock<Arc<ListingTable>>>,
    rebuild: Arc<ExternalTableRebuild>,
    ctx: Weak<SessionContext>,
    _listener: Option<Arc<RefreshListener>>,
}

impl ExternalTable {
    /// Create a self-refreshing external table.
    ///
    /// When `events` is `Some`, a background task re-infers the schema and
    /// re-lists files whenever a create/modify/delete event lands under one of
    /// the table's storage prefixes. When `None` (the datasets store does not
    /// emit events) the table only updates on manual `REFRESH` or the periodic
    /// table rescan.
    pub fn new_self_refreshing(
        definition: ExternalTableDefinition,
        initial: ListingTable,
        rebuild: ExternalTableRebuild,
        ctx: Weak<SessionContext>,
        events: Option<Receiver<ObjectEvent>>,
    ) -> Self {
        let prefixes: Vec<ObjectPath> = initial
            .table_paths()
            .iter()
            .map(|url| url.prefix().clone())
            .collect();

        let inner = Arc::new(RwLock::new(Arc::new(initial)));
        let rebuild = Arc::new(rebuild);

        let listener = events.map(|mut rx| {
            let inner = inner.clone();
            let rebuild = rebuild.clone();
            let ctx = ctx.clone();
            let table_name = definition.name.clone();
            let handle = tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            let path = event_path(&event);
                            if prefixes.iter().any(|prefix| path_under_prefix(prefix, path)) {
                                if let Err(error) = rebuild_into(&inner, &rebuild, &ctx).await {
                                    tracing::warn!(
                                        table = %table_name,
                                        %error,
                                        "external table self-refresh failed"
                                    );
                                } else {
                                    tracing::info!(table = %table_name, path = %path, "external table refreshed after storage event");
                                }
                            }
                        }
                        Err(RecvError::Lagged(skipped)) => {
                            tracing::warn!(
                                table = %table_name,
                                skipped,
                                "external table event subscriber lagged; some changes may only appear after the next refresh"
                            );
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
            });
            Arc::new(RefreshListener { handle })
        });

        Self {
            definition,
            inner,
            rebuild,
            ctx,
            _listener: listener,
        }
    }

    pub fn definition(&self) -> &ExternalTableDefinition {
        &self.definition
    }

    /// A snapshot of the current inner listing table.
    pub fn inner(&self) -> Arc<ListingTable> {
        self.inner.read().clone()
    }

    /// Re-infer the schema over all current objects and swap in a fresh listing.
    pub async fn refresh(&self) -> anyhow::Result<()> {
        rebuild_into(&self.inner, &self.rebuild, &self.ctx).await
    }
}

#[async_trait::async_trait]
impl TableProvider for ExternalTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.read().schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        (!self.rebuild.constraints.is_empty()).then_some(&self.rebuild.constraints)
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        self.inner.read().table_type()
    }

    /// Get the create statement used to create this table, if available.
    fn get_table_definition(&self) -> Option<&str> {
        self.definition.definition.as_deref()
    }

    /// Get the [`LogicalPlan`] of this table, if available.
    ///
    /// Listing-backed external tables have no logical plan.
    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    /// Get the default value for a column, if available.
    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.rebuild.column_defaults.get(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let inner = self.inner.read().clone();
        inner
            .scan(state, projection, filters, limit)
            .await
            .map_err(|e| DataFusionError::Execution(format!("ExternalTable scan error: {e}")))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        self.inner.read().supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.read().statistics()
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

        // When inferring, apply the default glob now so the rebuild spec lists
        // files identically on every subsequent refresh.
        if provided_schema.is_none() {
            listing_table_url =
                maybe_apply_default_glob(listing_table_url, &options, &self.file_type)?;
        }

        let rebuild = ExternalTableRebuild {
            listing_table_url,
            options,
            provided_schema,
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
            definition_sql: self.definition.clone(),
        };

        let initial = build_listing_table(&session_state, &rebuild).await?;

        let events = datasets_store_events(data_store_url).await;

        Ok(Arc::new(ExternalTable::new_self_refreshing(
            self.clone(),
            initial,
            rebuild,
            Arc::downgrade(&context),
            events,
        )))
    }

    fn table_name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
/// Persisted configuration for a SQL-defined [`ViewTable`].
pub struct ViewTableDefinition {
    /// Logical view name.
    pub name: String,
    /// SQL query used to create the view.
    pub definition: String,
    /// Dependencies on other tables, used to determine rebuild order.
    #[serde(default)]
    pub dependencies: Vec<String>,
}

impl ViewTableDefinition {
    /// Builds a serializable view definition from an existing [`ViewTable`].
    ///
    /// Returns an error when the input view has no persisted SQL definition.
    pub fn try_from_view(view_name: &str, table: &ViewTable) -> anyhow::Result<Self> {
        let plan = table.logical_plan();
        let mut dependencies = Vec::new();
        Self::traverse_logical_plan_for_dependencies(plan, &mut dependencies);

        match table.definition() {
            Some(def) => Ok(Self {
                name: view_name.to_string(),
                definition: def.clone(),
                dependencies,
            }),
            None => Err(anyhow::anyhow!(
                "ViewTableDefinition requires a SQL definition to be created from a ViewTable without a definition"
            )),
        }
    }

    fn traverse_logical_plan_for_dependencies(plan: &LogicalPlan, dependencies: &mut Vec<String>) {
        let _ = plan.apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(table_scan) = node {
                let table_name = table_scan.table_name.to_string();
                if !dependencies.contains(&table_name) {
                    dependencies.push(table_name);
                }
            }

            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        });

        dependencies.sort();
    }

    pub async fn into_view_table(self, context: Arc<SessionContext>) -> anyhow::Result<ViewTable> {
        let options = SQLOptions::new().with_allow_ddl(true);
        let df = context.sql_with_options(&self.definition, options).await?;
        let cloned_plan = df.logical_plan();
        if let LogicalPlan::Ddl(DdlStatement::CreateView(plan)) = cloned_plan {
            Ok(ViewTable::new(
                plan.input.as_ref().clone(),
                Some(self.definition),
            ))
        } else {
            // This should never happen because the definition must have come from a ViewTable, but we defensively handle it just in case.
            anyhow::bail!(
                "Expected logical plan for view '{}' to be a CreateView DDL, but got: {:?}",
                self.name,
                cloned_plan
            );
        }
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

        if let LogicalPlan::Ddl(DdlStatement::CreateView(plan)) = plan {
            Ok(Arc::new(ViewTable::new(
                plan.input.as_ref().clone(),
                Some(self.definition.clone()),
            )))
        } else {
            // This should never happen because the definition must have come from a ViewTable, but we defensively handle it just in case.
            anyhow::bail!(
                "Expected logical plan for view '{}' to be a CreateView DDL, but got: {:?}",
                self.name,
                plan
            );
        }
    }

    fn table_name(&self) -> &str {
        &self.name
    }

    fn depends_on(&self) -> Vec<String> {
        self.dependencies.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }
}

/// A materialized view: a query whose result set is persisted as Parquet files
/// and served directly from disk instead of being recomputed on every read.
///
/// Wraps an inner [`ListingTable`] built over the persisted Parquet, mirroring how
/// [`ExternalTable`] wraps a listing table. The wrapper is the downcast target used
/// by catalog persistence and by refresh/drop detection.
#[derive(Clone, Debug)]
pub struct MaterializedView {
    definition: MaterializedViewDefinition,
    inner: ListingTable,
}

impl MaterializedView {
    pub fn new(definition: MaterializedViewDefinition, inner: ListingTable) -> Self {
        Self { definition, inner }
    }

    pub fn definition(&self) -> &MaterializedViewDefinition {
        &self.definition
    }

    /// Storage prefix (relative to the datasets object store) that holds all
    /// versioned data directories for this materialized view.
    pub fn base_storage_prefix(&self) -> String {
        format!("__beacon__/{}", self.definition.name)
    }
}

#[async_trait::async_trait]
impl TableProvider for MaterializedView {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        Some(self.definition.definition.as_str())
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
            .map_err(|e| DataFusionError::Execution(format!("MaterializedView scan error: {e}")))
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
        not_impl_err!("Insert into MaterializedView is not supported")
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
/// Persisted configuration for a materialized view.
pub struct MaterializedViewDefinition {
    /// Logical view name.
    pub name: String,
    /// Original SQL query that produces the materialized result.
    pub definition: String,
    /// Output schema of the query, recorded for catalog/metadata purposes.
    pub schema: SchemaRef,
    /// Active data directory (relative to the datasets object store) holding the
    /// persisted Parquet files, e.g. `__beacon__/<name>/<uuid>/`.
    pub storage_location: String,
    /// Creation timestamp (Unix epoch milliseconds).
    pub created_at: i64,
    /// Timestamp of the last successful refresh (Unix epoch milliseconds), if any.
    #[serde(default)]
    pub last_refreshed: Option<i64>,
}

#[async_trait::async_trait]
#[typetag::serde(name = "materialized_view")]
impl TableDefinition for MaterializedViewDefinition {
    /// Builds a [`MaterializedView`] provider backed by a [`ListingTable`] over the
    /// persisted Parquet at `storage_location`. The schema is inferred from the
    /// written Parquet to avoid drift between the recorded schema and the files.
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
        data_store_url: &ObjectStoreUrl,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let session_state = context.state();
        let file_format_factory = session_state
            .get_file_format_factory("parquet")
            .ok_or(config_datafusion_err!(
                "Unable to build materialized view '{}': parquet FileFormat not found.",
                self.name
            ))?;
        let file_format =
            file_format_factory.create(&session_state, &std::collections::HashMap::new())?;

        let mut listing_table_url = parse_listing_table_url(data_store_url, &self.storage_location)?;

        let options = ListingOptions::new(file_format)
            .with_file_extension("")
            .with_session_config_options(session_state.config())
            .with_table_partition_cols(vec![]);

        options
            .validate_partitions(&session_state, &listing_table_url)
            .await?;

        listing_table_url = maybe_apply_default_glob(listing_table_url, &options, "parquet")?;
        let resolved_schema = options
            .infer_schema(&session_state, &listing_table_url)
            .await?;

        let config = ListingTableConfig::new(listing_table_url)
            .with_listing_options(options)
            .with_schema(resolved_schema);
        let provider = ListingTable::try_new(config)?.with_cache(
            session_state
                .runtime_env()
                .cache_manager
                .get_file_statistic_cache(),
        );

        Ok(Arc::new(MaterializedView::new(self.clone(), provider)))
    }

    fn table_name(&self) -> &str {
        &self.name
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

#[cfg(test)]
/// Tests for self-refreshing external tables (manual `REFRESH` and event-driven).
mod self_refresh_tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int64Array};
    use datafusion::arrow::datatypes::Field;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::parquet::arrow::ArrowWriter;
    use object_store::local::LocalFileSystem;
    use std::time::Duration;
    use tokio::sync::broadcast;

    fn write_parquet_i64(disk_path: &std::path::Path, values: &[i64]) {
        std::fs::create_dir_all(disk_path.parent().expect("path has parent"))
            .expect("create parent dirs");
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .expect("build batch");
        let file = std::fs::File::create(disk_path).expect("create file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("arrow writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }

    async fn count_rows(ctx: &SessionContext) -> i64 {
        let df = ctx.sql("SELECT count(*) FROM obs").await.expect("plan");
        let batches = df.collect().await.expect("collect");
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 count")
            .value(0)
    }

    fn ctx_with_datasets(dir: &std::path::Path) -> Arc<SessionContext> {
        let ctx = Arc::new(SessionContext::new());
        let store_url = ObjectStoreUrl::parse(DATASETS_STORE_URL).unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir).expect("local store"));
        ctx.register_object_store(store_url.as_ref(), store);
        ctx
    }

    async fn build_external(
        ctx: &Arc<SessionContext>,
        events: Option<Receiver<ObjectEvent>>,
    ) -> ExternalTable {
        let store_url = ObjectStoreUrl::parse(DATASETS_STORE_URL).unwrap();
        let listing_table_url =
            parse_listing_table_url(&store_url, "obs/**/*.parquet").expect("listing url");
        let options =
            ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension("");
        let rebuild = ExternalTableRebuild {
            listing_table_url,
            options,
            provided_schema: None,
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
            definition_sql: None,
        };
        let state = ctx.state();
        let initial = build_listing_table(&state, &rebuild)
            .await
            .expect("initial listing table");
        let definition = ExternalTableDefinition {
            name: "obs".to_string(),
            location: "obs/**/*.parquet".to_string(),
            file_type: "parquet".to_string(),
            schema: Arc::new(Schema::empty()),
            definition: None,
            partition_cols: vec![],
            options: HashMap::new(),
            if_not_exists: false,
        };
        ExternalTable::new_self_refreshing(definition, initial, rebuild, Arc::downgrade(ctx), events)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn manual_refresh_reinfers_and_empties() {
        let dir = tempfile::tempdir().unwrap();
        write_parquet_i64(&dir.path().join("obs/a.parquet"), &[1]);
        let ctx = ctx_with_datasets(dir.path());

        let external = build_external(&ctx, None).await;
        ctx.register_table("obs", Arc::new(external.clone())).unwrap();
        assert_eq!(count_rows(&ctx).await, 1);

        // A new file appears; manual refresh re-lists and picks it up.
        write_parquet_i64(&dir.path().join("obs/b.parquet"), &[2, 3]);
        external.refresh().await.unwrap();
        assert_eq!(count_rows(&ctx).await, 3);

        // All files removed; schema falls back to empty.
        std::fs::remove_file(dir.path().join("obs/a.parquet")).unwrap();
        std::fs::remove_file(dir.path().join("obs/b.parquet")).unwrap();
        external.refresh().await.unwrap();
        assert!(external.schema().fields().is_empty());
        assert_eq!(count_rows(&ctx).await, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn storage_event_triggers_self_refresh() {
        let dir = tempfile::tempdir().unwrap();
        write_parquet_i64(&dir.path().join("obs/a.parquet"), &[1]);
        write_parquet_i64(&dir.path().join("obs/b.parquet"), &[2, 3]);
        let ctx = ctx_with_datasets(dir.path());

        let (tx, rx) = broadcast::channel::<ObjectEvent>(16);
        let external = build_external(&ctx, Some(rx)).await;
        ctx.register_table("obs", Arc::new(external)).unwrap();
        assert_eq!(count_rows(&ctx).await, 3);

        // A file under the table prefix is removed; the table's own listener
        // refreshes it without any table-manager involvement.
        std::fs::remove_file(dir.path().join("obs/b.parquet")).unwrap();
        tx.send(ObjectEvent::Deleted(ObjectPath::from("obs/b.parquet")))
            .unwrap();

        let mut count = count_rows(&ctx).await;
        for _ in 0..50 {
            if count == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            count = count_rows(&ctx).await;
        }
        assert_eq!(count, 1, "deletion under prefix should refresh the table");

        // An event under an unrelated prefix must not trigger a refresh.
        tx.send(ObjectEvent::Deleted(ObjectPath::from("other/x.parquet")))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(count_rows(&ctx).await, 1);
    }
}

#[cfg(test)]
/// Unit tests covering listing-table and view-table definition behavior.
mod tests {
    use super::{
        ExternalTableDefinition, MaterializedViewDefinition, TableDefinition, ViewTableDefinition,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::{MemTable, TableType, ViewTable};
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

        let definition = ViewTableDefinition::try_from_view("view", &view).unwrap();
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

        let err =
            ViewTableDefinition::try_from_view("view", &view).expect_err("missing definition");
        assert!(err.to_string().contains("requires a SQL definition"));
    }

    #[tokio::test]
    /// Verifies building a view definition yields a downcastable [`ViewTable`].
    async fn view_table_definition_build_provider_creates_view_table() {
        let definition = ViewTableDefinition {
            name: "my_view".to_string(),
            definition: "SELECT 42 AS answer".to_string(),
            dependencies: vec![],
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

    #[test]
    /// Verifies legacy view JSON without dependencies remains deserializable.
    fn view_table_definition_deserializes_without_dependencies_for_compatibility() {
        let legacy_json = r#"{
  "type": "view_table",
  "name": "legacy_view",
  "definition": "SELECT 1 AS x"
}"#;

        let definition: Arc<dyn TableDefinition> =
            serde_json::from_str(legacy_json).expect("legacy view JSON should deserialize");

        assert_eq!(definition.table_name(), "legacy_view");
        assert_eq!(definition.table_type(), TableType::View);
        assert!(definition.depends_on().is_empty());
    }

    #[test]
    /// Verifies trait-level table_type defaults to Base unless overridden.
    fn table_definition_table_type_defaults_and_overrides() {
        let external = ExternalTableDefinition {
            name: "ext_table".to_string(),
            location: "tmp/path".to_string(),
            file_type: "parquet".to_string(),
            schema: Arc::new(Schema::empty()),
            definition: None,
            partition_cols: vec![],
            options: HashMap::new(),
            if_not_exists: false,
        };
        let view = ViewTableDefinition {
            name: "view_table".to_string(),
            definition: "SELECT 1".to_string(),
            dependencies: vec![],
        };

        assert_eq!(external.table_type(), TableType::Base);
        assert_eq!(view.table_type(), TableType::View);
    }

    #[tokio::test]
    /// Verifies dependency extraction from direct table scans is stable and deduplicated.
    async fn view_table_dependency_traversal_collects_direct_scans() {
        let context = SessionContext::new();
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let batch = RecordBatch::new_empty(schema.clone());

        for table_name in ["t1", "t2"] {
            let table = MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])
                .expect("mem table should be created");
            context
                .register_table(table_name, Arc::new(table))
                .expect("table should be registered");
        }

        let plan = context
            .state()
            .create_logical_plan(
                "SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.id IN (SELECT id FROM t1)",
            )
            .await
            .expect("logical plan should be created");

        let mut dependencies = Vec::new();
        ViewTableDefinition::traverse_logical_plan_for_dependencies(&plan, &mut dependencies);

        assert_eq!(dependencies, vec!["t1".to_string(), "t2".to_string()]);
    }

    #[tokio::test]
    /// Verifies dependency extraction traverses nested subquery plans embedded in expressions.
    async fn view_table_dependency_traversal_collects_nested_subqueries() {
        let context = SessionContext::new();
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let batch = RecordBatch::new_empty(schema.clone());

        for table_name in ["t1", "t2", "t3"] {
            let table = MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])
                .expect("mem table should be created");
            context
                .register_table(table_name, Arc::new(table))
                .expect("table should be registered");
        }

        let plan = context
            .state()
            .create_logical_plan(
                "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2 WHERE EXISTS (SELECT 1 FROM t3 WHERE t3.id = t2.id))",
            )
            .await
            .expect("logical plan should be created");

        let mut dependencies = Vec::new();
        ViewTableDefinition::traverse_logical_plan_for_dependencies(&plan, &mut dependencies);

        assert_eq!(
            dependencies,
            vec!["t1".to_string(), "t2".to_string(), "t3".to_string()]
        );
    }

    #[test]
    /// Verifies a materialized view definition round-trips through typetag JSON.
    fn materialized_view_definition_serde_round_trip() {
        let definition = MaterializedViewDefinition {
            name: "mv".to_string(),
            definition: "SELECT 1 AS a".to_string(),
            schema: Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)])),
            storage_location: "__beacon__/mv/abc/".to_string(),
            created_at: 42,
            last_refreshed: Some(43),
        };
        let boxed: Arc<dyn TableDefinition> = Arc::new(definition);

        let json = serde_json::to_string(&boxed).expect("definition should serialize");
        assert!(json.contains("\"materialized_view\""));

        let restored: Arc<dyn TableDefinition> =
            serde_json::from_str(&json).expect("definition should deserialize");
        assert_eq!(restored.table_name(), "mv");
        assert_eq!(restored.table_type(), TableType::Base);
    }

    #[tokio::test]
    /// Verifies a materialized view definition scans the persisted Parquet result.
    async fn materialized_view_definition_build_provider_scans_parquet() {
        use super::MaterializedView;
        use datafusion::dataframe::DataFrameWriteOptions;

        let root = create_temp_dir("table_ext_materialized_view");

        // Write the query result to Parquet under the temp directory.
        let writer_ctx = SessionContext::new();
        let df = writer_ctx
            .sql("SELECT 1 AS a, 2 AS b")
            .await
            .expect("query should plan");
        let write_url = format!("file://{}/", root.to_string_lossy());
        df.write_parquet(&write_url, DataFrameWriteOptions::new(), None)
            .await
            .expect("parquet result should be written");

        let definition = MaterializedViewDefinition {
            name: "mv".to_string(),
            definition: "SELECT 1 AS a, 2 AS b".to_string(),
            schema: Arc::new(Schema::empty()),
            storage_location: format!("{}/", to_store_relative_location(&root)),
            created_at: 0,
            last_refreshed: None,
        };

        let context = Arc::new(SessionContext::new());
        let store_url = ObjectStoreUrl::parse("file://").unwrap();

        let provider = definition
            .build_provider(context.clone(), &store_url)
            .await
            .expect("materialized view provider should be built");

        let materialized = provider
            .as_any()
            .downcast_ref::<MaterializedView>()
            .expect("provider should be a MaterializedView");
        assert_eq!(materialized.base_storage_prefix(), "__beacon__/mv");

        let schema = provider.schema();
        assert_eq!(schema.fields().len(), 2);

        let state = context.state();
        let plan = provider
            .scan(&state, None, &[], None)
            .await
            .expect("scan plan should be built");
        let batches = datafusion::physical_plan::collect(plan, context.task_ctx())
            .await
            .expect("scan should execute");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1);

        fs::remove_dir_all(&root).expect("temporary directory should be cleaned up");
    }
}
