//! Serializable table definitions used to rebuild DataFusion table providers.
//!
//! This module holds the [`TableDefinition`] trait and the definitions for SQL
//! views and materialized views. The external table definition is in the
//! `beacon-external-table` crate.

use std::fmt::Debug;
use std::sync::Arc;

use crate::consts::DEFAULT_DB_STORE_URL_OBJECT_URL;
use crate::listing_factory::ListingFactory;
use datafusion::catalog::Session;
use datafusion::common::{Constraints, DataFusionError, Statistics, not_impl_err};
use datafusion::datasource::{TableType, ViewTable};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{DdlStatement, LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{Expr, SQLOptions};
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::TableProvider,
    common::config_datafusion_err,
    datasource::listing::{ListingOptions, ListingTable, ListingTableConfig},
    prelude::SessionContext,
};

use crate::fast_object::FastObjectTable;

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
    ) -> anyhow::Result<Arc<dyn TableProvider>>;

    fn depends_on(&self) -> Vec<String> {
        Vec::new()
    }

    fn table_name(&self) -> &str;

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

/// Reserved prefix in the tables store under which materialized-view Parquet
/// data is persisted, keeping it clear of user tables (which live at
/// `<name>/table.json` in the same store). Mirrors the crawler definitions'
/// `__crawlers__/` prefix convention.
pub const MATERIALIZED_VIEW_PREFIX: &str = "__materialized__";

/// Reserved prefix for beacon's internal auth tables (`__beacon_users`,
/// `__beacon_user_roles`, `__beacon_roles`, `__beacon_role_rules`), held in the
/// tables store. They carry Argon2 password hashes, so they are hidden from
/// user-facing catalog listings (`SHOW TABLES` / `information_schema`) and are
/// readable only by the super-user (an unconditional gate in beacon-core's
/// `statement_plan::authz`, independent of grant enforcement). The write path and
/// super-users still resolve them by name.
pub const INTERNAL_TABLE_PREFIX: &str = "__beacon_";

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
        // The stored definition is the view body (a `SELECT`), which DataFusion
        // plans straight to the view's input plan. Older definitions stored the
        // full `CREATE VIEW … AS …`, which wraps the input in a `CreateView` DDL;
        // unwrap that case for backward compatibility.
        let input = match df.logical_plan() {
            LogicalPlan::Ddl(DdlStatement::CreateView(plan)) => plan.input.as_ref().clone(),
            plan => plan.clone(),
        };
        Ok(ViewTable::new(input, Some(self.definition)))
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "view_table")]
impl TableDefinition for ViewTableDefinition {
    /// Compiles the stored SQL and returns a DataFusion [`ViewTable`] provider.
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        // Compile the SQL definition into a DataFusion logical plan and use it to
        // create a ViewTable provider. The definition is the view body (`SELECT`),
        // which plans straight to the input plan; older `CREATE VIEW … AS …`
        // definitions wrap it in a `CreateView` DDL, which we unwrap.
        let state = context.state();
        let input = match state.create_logical_plan(&self.definition).await? {
            LogicalPlan::Ddl(DdlStatement::CreateView(plan)) => plan.input.as_ref().clone(),
            plan => plan,
        };
        Ok(Arc::new(ViewTable::new(
            input,
            Some(self.definition.clone()),
        )))
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
/// Wraps a [`FastObjectTable`] over the persisted Parquet, mirroring how
/// [`ExternalTable`] does. The wrapper is the downcast target used by catalog
/// persistence and by refresh/drop detection.
#[derive(Clone, Debug)]
pub struct MaterializedView {
    definition: MaterializedViewDefinition,
    inner: FastObjectTable,
    /// The full `CREATE MATERIALIZED VIEW` statement, for `SHOW CREATE TABLE`.
    statement: String,
}

impl MaterializedView {
    pub fn new(definition: MaterializedViewDefinition, inner: FastObjectTable) -> Self {
        let name = datafusion::sql::sqlparser::ast::Ident::with_quote('"', &definition.name);
        let statement = format!(
            "CREATE MATERIALIZED VIEW {name} AS {}",
            definition.definition
        );
        Self {
            definition,
            inner,
            statement,
        }
    }

    pub fn definition(&self) -> &MaterializedViewDefinition {
        &self.definition
    }

    /// Storage prefix (relative to the tables store, see [`TABLES_STORE_URL`])
    /// that holds all versioned data directories for this materialized view, i.e.
    /// `__materialized__/<name>`. Dropping the view reclaims everything under it.
    pub fn base_storage_prefix(&self) -> String {
        format!("{MATERIALIZED_VIEW_PREFIX}/{}", self.definition.name)
    }
}

#[async_trait::async_trait]
impl TableProvider for MaterializedView {
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
        Some(self.statement.as_str())
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
    /// Active data directory (relative to the tables store, see
    /// [`TABLES_STORE_URL`]) holding the persisted Parquet files, e.g.
    /// `__materialized__/<name>/<uuid>/`.
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
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let session_state = context.state();
        let listing_factory = session_state
            .config()
            .get_extension::<ListingFactory>()
            .expect("Listing factory should be registered in session config.");

        let file_format_factory =
            session_state
                .get_file_format_factory("parquet")
                .ok_or(config_datafusion_err!(
                    "Unable to build materialized view '{}': parquet FileFormat not found.",
                    self.name
                ))?;
        let file_format =
            file_format_factory.create(&session_state, &std::collections::HashMap::new())?;

        // Materialized views always read from the tables store, where their data
        // is written, irrespective of the caller's default store (the reload path
        // rebuilds every provider against the datasets URL). `storage_location`
        // already carries the `__materialized__/` prefix.
        let mut listing_table_url = listing_factory.parse_listing_table_url_with_store(
            &session_state,
            &DEFAULT_DB_STORE_URL_OBJECT_URL,
            &self.storage_location,
        )?;

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

        Ok(Arc::new(MaterializedView::new(
            self.clone(),
            FastObjectTable::from_listing_table(provider),
        )))
    }

    fn table_name(&self) -> &str {
        &self.name
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

// #[cfg(test)]
// /// Tests for self-refreshing external tables (manual `REFRESH` and event-driven).
// mod self_refresh_tests {
//     use super::*;
//     use datafusion::arrow::array::{Array, Int64Array};
//     use datafusion::arrow::datatypes::Field;
//     use datafusion::arrow::record_batch::RecordBatch;
// //     use datafusion::parquet::arrow::ArrowWriter;
//     use object_store::local::LocalFileSystem;
//     use std::time::Duration;
//     use tokio::sync::broadcast;

//     /// An arbitrary store URL for these tests; the code under test resolves the
//     /// URL it is handed and has no built-in scheme of its own.
//     const TEST_STORE_URL: &str = "datasets://";

//     fn write_parquet_i64(disk_path: &std::path::Path, values: &[i64]) {
//         std::fs::create_dir_all(disk_path.parent().expect("path has parent"))
//             .expect("create parent dirs");
//         let schema = Arc::new(Schema::new(vec![Field::new(
//             "value",
//             DataType::Int64,
//             false,
//         )]));
//         let batch = RecordBatch::try_new(
//             schema.clone(),
//             vec![Arc::new(Int64Array::from(values.to_vec()))],
//         )
//         .expect("build batch");
//         let file = std::fs::File::create(disk_path).expect("create file");
//         let mut writer = ArrowWriter::try_new(file, schema, None).expect("arrow writer");
//         writer.write(&batch).expect("write batch");
//         writer.close().expect("close writer");
//     }

//     async fn count_rows(ctx: &SessionContext) -> i64 {
//         let df = ctx.sql("SELECT count(*) FROM obs").await.expect("plan");
//         let batches = df.collect().await.expect("collect");
//         batches[0]
//             .column(0)
//             .as_any()
//             .downcast_ref::<Int64Array>()
//             .expect("Int64 count")
//             .value(0)
//     }

//     fn ctx_with_datasets(dir: &std::path::Path) -> Arc<SessionContext> {
//         let ctx = Arc::new(SessionContext::new());
//         let store_url = ObjectStoreUrl::parse(TEST_STORE_URL).unwrap();
//         let store = Arc::new(LocalFileSystem::new_with_prefix(dir).expect("local store"));
//         ctx.register_object_store(store_url.as_ref(), store);
//         ctx
//     }

//     async fn build_external(ctx: &Arc<SessionContext>) -> ExternalTable {
//         let store_url = ObjectStoreUrl::parse(TEST_STORE_URL).unwrap();
//         let listing_table_url =
//             parse_listing_table_url(&store_url, "obs/**/*.parquet").expect("listing url");
//         let options =
//             ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension("");
//         let rebuild = ExternalTableRebuild {
//             listing_table_url,
//             options,
//             provided_schema: None,
//             constraints: Constraints::default(),
//             column_defaults: HashMap::new(),
//             definition_sql: None,
//         };
//         let state = ctx.state();
//         let initial = build_listing_table(&state, &rebuild)
//             .await
//             .expect("initial listing table");
//         let definition = ExternalTableDefinition {
//             name: "obs".to_string(),
//             location: "obs/**/*.parquet".to_string(),
//             file_type: "parquet".to_string(),
//             schema: Arc::new(Schema::empty()),
//             definition: None,
//             partition_cols: vec![],
//             options: HashMap::new(),
//             if_not_exists: false,
//         };
//         ExternalTable::new(definition, initial, rebuild)
//     }

//     #[tokio::test(flavor = "multi_thread")]
//     async fn manual_refresh_reinfers_and_empties() {
//         let dir = tempfile::tempdir().unwrap();
//         write_parquet_i64(&dir.path().join("obs/a.parquet"), &[1]);
//         let ctx = ctx_with_datasets(dir.path());

//         let external = build_external(&ctx).await;
//         ctx.register_table("obs", Arc::new(external.clone()))
//             .unwrap();
//         assert_eq!(count_rows(&ctx).await, 1);

//         // A new file appears; manual refresh re-lists and picks it up.
//         write_parquet_i64(&dir.path().join("obs/b.parquet"), &[2, 3]);
//         external.refresh(&ctx.state()).await.unwrap();
//         assert_eq!(count_rows(&ctx).await, 3);

//         // All files removed; schema falls back to empty.
//         std::fs::remove_file(dir.path().join("obs/a.parquet")).unwrap();
//         std::fs::remove_file(dir.path().join("obs/b.parquet")).unwrap();
//         external.refresh(&ctx.state()).await.unwrap();
//         assert!(external.schema().fields().is_empty());
//         assert_eq!(count_rows(&ctx).await, 0);
//     }

//     /// Which tables a storage event selects. The refresh itself is driven by the
//     /// runtime (it owns the session); this is the matching rule it applies.
//     #[tokio::test(flavor = "multi_thread")]
//     async fn covers_only_paths_under_the_tables_prefix() {
//         let dir = tempfile::tempdir().unwrap();
//         write_parquet_i64(&dir.path().join("obs/a.parquet"), &[1]);
//         let ctx = ctx_with_datasets(dir.path());
//         let external = build_external(&ctx).await;

//         assert!(external.covers(&ObjectPath::from("obs/b.parquet")));
//         assert!(external.covers(&ObjectPath::from("obs/nested/c.parquet")));
//         // A sibling prefix sharing a name prefix must not match.
//         assert!(!external.covers(&ObjectPath::from("obs_2/x.parquet")));
//         assert!(!external.covers(&ObjectPath::from("other/x.parquet")));
//     }
// }

// #[cfg(test)]
// /// Unit tests covering listing-table and view-table definition behavior.
// mod tests {
//     use super::{
//         ExternalTable, ExternalTableDefinition, LogicalTableDefinition, MaterializedViewDefinition,
//         TableDefinition, ViewTableDefinition,
//     };
//     use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
//     use datafusion::arrow::record_batch::RecordBatch;
//     use datafusion::datasource::{MemTable, TableType, ViewTable};
//     use datafusion::execution::object_store::ObjectStoreUrl;
//     use datafusion::prelude::SessionContext;
//     use std::collections::HashMap;
//     use std::fs;
//     use std::path::PathBuf;
//     use std::sync::Arc;
//     use std::time::{SystemTime, UNIX_EPOCH};

//     /// Creates a unique temporary directory for table fixture data.
//     fn create_temp_dir(prefix: &str) -> PathBuf {
//         let mut dir = std::env::temp_dir();
//         let ts = SystemTime::now()
//             .duration_since(UNIX_EPOCH)
//             .expect("system time should be after unix epoch")
//             .as_nanos();
//         dir.push(format!("{prefix}_{}_{}", std::process::id(), ts));
//         fs::create_dir_all(&dir).expect("temporary directory should be created");
//         dir
//     }

//     /// Converts an absolute local path into a location relative to `file://` root.
//     fn to_store_relative_location(path: &std::path::Path) -> String {
//         path.to_string_lossy().trim_start_matches('/').to_string()
//     }

//     #[tokio::test]
//     /// Verifies listing providers can infer schema from folder-backed files.
//     async fn listing_table_definition_build_provider_infers_schema_from_folder() {
//         let root = create_temp_dir("table_ext_listing_infer");
//         let data_file = root.join("part-0.csv");
//         fs::write(&data_file, "1\n2\n").expect("csv fixture should be written");

//         let definition = ExternalTableDefinition {
//             name: "t_csv".to_string(),
//             location: format!("{}/", to_store_relative_location(&root)),
//             file_type: "csv".to_string(),
//             schema: Arc::new(Schema::empty()),
//             definition: Some("SELECT value FROM t_csv".to_string()),
//             partition_cols: vec![],
//             options: HashMap::new(),
//             if_not_exists: false,
//         };

//         let context = Arc::new(SessionContext::new());
//         let store_url = ObjectStoreUrl::parse("file://").unwrap();

//         let provider = definition
//             .build_provider(context, &store_url)
//             .await
//             .expect("listing provider should be built from inferred schema");

//         let schema = provider.schema();
//         assert_eq!(schema.fields().len(), 1);
//         assert!(!schema.field(0).name().is_empty());

//         fs::remove_dir_all(&root).expect("temporary directory should be cleaned up");
//     }

//     #[tokio::test]
//     /// Verifies explicit schemas and partition metadata produce a valid provider schema.
//     async fn listing_table_definition_build_provider_projects_partition_columns() {
//         let root = create_temp_dir("table_ext_listing_partition");
//         let partitioned = root.join("year=2026");
//         fs::create_dir_all(&partitioned).expect("partition folder should be created");
//         fs::write(partitioned.join("part-0.csv"), "1\n")
//             .expect("partition csv fixture should be written");

//         let schema: SchemaRef = Arc::new(Schema::new(vec![
//             Field::new("value", DataType::Int32, true),
//             Field::new("year", DataType::Utf8, true),
//         ]));

//         let definition = ExternalTableDefinition {
//             name: "t_partitioned".to_string(),
//             location: format!("{}/", to_store_relative_location(&root)),
//             file_type: "csv".to_string(),
//             schema,
//             definition: None,
//             partition_cols: vec!["year".to_string()],
//             options: HashMap::new(),
//             if_not_exists: false,
//         };

//         let context = Arc::new(SessionContext::new());
//         let store_url = ObjectStoreUrl::parse("file://").unwrap();

//         let provider = definition
//             .build_provider(context, &store_url)
//             .await
//             .expect("listing provider should be built from explicit schema");

//         let schema = provider.schema();
//         assert_eq!(schema.fields().len(), 2);
//         assert_eq!(schema.field(0).name(), "value");
//         assert_eq!(schema.field(0).data_type(), &DataType::Int32);
//         assert_eq!(schema.field(1).name(), "year");
//         assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

//         fs::remove_dir_all(&root).expect("temporary directory should be cleaned up");
//     }

//     #[tokio::test]
//     /// Verifies unknown file formats fail provider construction with a clear error.
//     async fn listing_table_definition_build_provider_rejects_unknown_file_type() {
//         let definition = ExternalTableDefinition {
//             name: "t_bad".to_string(),
//             location: "tmp".to_string(),
//             file_type: "not_a_real_format".to_string(),
//             schema: Arc::new(Schema::empty()),
//             definition: None,
//             partition_cols: vec![],
//             options: HashMap::new(),
//             if_not_exists: false,
//         };

//         let context = Arc::new(SessionContext::new());
//         let store_url = ObjectStoreUrl::parse("file://").unwrap();

//         let err = definition
//             .build_provider(context, &store_url)
//             .await
//             .expect_err("unknown file type should fail");
//         let msg = err.to_string();

//         assert!(msg.contains("Could not find FileFormat"), "error: {msg}");
//     }

//     #[tokio::test]
//     /// Verifies extracting a definition from a view succeeds when SQL is present.
//     async fn view_table_definition_try_from_view_reads_definition() {
//         let context = SessionContext::new();
//         let plan = context
//             .state()
//             .create_logical_plan("SELECT 1 AS x")
//             .await
//             .unwrap();
//         let view = ViewTable::new(plan, Some("SELECT 1 AS x".to_string()));

//         let definition = ViewTableDefinition::try_from_view("view", &view).unwrap();
//         assert_eq!(definition.name, "view");
//         assert_eq!(definition.definition, "SELECT 1 AS x");
//     }

//     #[tokio::test]
//     /// Verifies extracting a definition from a view fails when SQL is absent.
//     async fn view_table_definition_try_from_view_requires_definition() {
//         let context = SessionContext::new();
//         let plan = context
//             .state()
//             .create_logical_plan("SELECT 1 AS x")
//             .await
//             .unwrap();
//         let view = ViewTable::new(plan, None);

//         let err =
//             ViewTableDefinition::try_from_view("view", &view).expect_err("missing definition");
//         assert!(err.to_string().contains("requires a SQL definition"));
//     }

//     #[tokio::test]
//     /// Verifies building a view definition yields a downcastable [`ViewTable`].
//     async fn view_table_definition_build_provider_creates_view_table() {
//         let definition = ViewTableDefinition {
//             name: "my_view".to_string(),
//             definition: "SELECT 42 AS answer".to_string(),
//             dependencies: vec![],
//         };

//         let context = Arc::new(SessionContext::new());
//         let store_url = ObjectStoreUrl::parse("file://").unwrap();

//         let provider = definition
//             .build_provider(context, &store_url)
//             .await
//             .expect("view provider should be built");

//         let view = provider
//             .as_any()
//             .downcast_ref::<ViewTable>()
//             .expect("provider should be a ViewTable");
//         assert_eq!(
//             view.definition().cloned(),
//             Some("SELECT 42 AS answer".to_string())
//         );
//     }

//     #[test]
//     /// Verifies legacy view JSON without dependencies remains deserializable.
//     fn view_table_definition_deserializes_without_dependencies_for_compatibility() {
//         let legacy_json = r#"{
//   "definition_type": "view_table",
//   "name": "legacy_view",
//   "definition": "SELECT 1 AS x"
// }"#;

//         let definition: Arc<dyn TableDefinition> =
//             serde_json::from_str(legacy_json).expect("legacy view JSON should deserialize");

//         assert_eq!(definition.table_name(), "legacy_view");
//         assert_eq!(definition.table_type(), TableType::View);
//         assert!(definition.depends_on().is_empty());
//     }

//     #[test]
//     /// Verifies trait-level table_type defaults to Base unless overridden.
//     fn table_definition_table_type_defaults_and_overrides() {
//         let external = ExternalTableDefinition {
//             name: "ext_table".to_string(),
//             location: "tmp/path".to_string(),
//             file_type: "parquet".to_string(),
//             schema: Arc::new(Schema::empty()),
//             definition: None,
//             partition_cols: vec![],
//             options: HashMap::new(),
//             if_not_exists: false,
//         };
//         let view = ViewTableDefinition {
//             name: "view_table".to_string(),
//             definition: "SELECT 1".to_string(),
//             dependencies: vec![],
//         };

//         assert_eq!(external.table_type(), TableType::Base);
//         assert_eq!(view.table_type(), TableType::View);
//     }

//     #[tokio::test]
//     /// Verifies dependency extraction from direct table scans is stable and deduplicated.
//     async fn view_table_dependency_traversal_collects_direct_scans() {
//         let context = SessionContext::new();
//         let schema: SchemaRef =
//             Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
//         let batch = RecordBatch::new_empty(schema.clone());

//         for table_name in ["t1", "t2"] {
//             let table = MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])
//                 .expect("mem table should be created");
//             context
//                 .register_table(table_name, Arc::new(table))
//                 .expect("table should be registered");
//         }

//         let plan = context
//             .state()
//             .create_logical_plan(
//                 "SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.id IN (SELECT id FROM t1)",
//             )
//             .await
//             .expect("logical plan should be created");

//         let mut dependencies = Vec::new();
//         ViewTableDefinition::traverse_logical_plan_for_dependencies(&plan, &mut dependencies);

//         assert_eq!(dependencies, vec!["t1".to_string(), "t2".to_string()]);
//     }

//     #[tokio::test]
//     /// Verifies dependency extraction traverses nested subquery plans embedded in expressions.
//     async fn view_table_dependency_traversal_collects_nested_subqueries() {
//         let context = SessionContext::new();
//         let schema: SchemaRef =
//             Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
//         let batch = RecordBatch::new_empty(schema.clone());

//         for table_name in ["t1", "t2", "t3"] {
//             let table = MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])
//                 .expect("mem table should be created");
//             context
//                 .register_table(table_name, Arc::new(table))
//                 .expect("table should be registered");
//         }

//         let plan = context
//             .state()
//             .create_logical_plan(
//                 "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2 WHERE EXISTS (SELECT 1 FROM t3 WHERE t3.id = t2.id))",
//             )
//             .await
//             .expect("logical plan should be created");

//         let mut dependencies = Vec::new();
//         ViewTableDefinition::traverse_logical_plan_for_dependencies(&plan, &mut dependencies);

//         assert_eq!(
//             dependencies,
//             vec!["t1".to_string(), "t2".to_string(), "t3".to_string()]
//         );
//     }

//     #[test]
//     /// Verifies a materialized view definition round-trips through typetag JSON.
//     fn materialized_view_definition_serde_round_trip() {
//         let definition = MaterializedViewDefinition {
//             name: "mv".to_string(),
//             definition: "SELECT 1 AS a".to_string(),
//             schema: Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)])),
//             storage_location: "__materialized__/mv/abc/".to_string(),
//             created_at: 42,
//             last_refreshed: Some(43),
//         };
//         let boxed: Arc<dyn TableDefinition> = Arc::new(definition);

//         let json = serde_json::to_string(&boxed).expect("definition should serialize");
//         assert!(json.contains("\"materialized_view\""));

//         let restored: Arc<dyn TableDefinition> =
//             serde_json::from_str(&json).expect("definition should deserialize");
//         assert_eq!(restored.table_name(), "mv");
//         assert_eq!(restored.table_type(), TableType::Base);
//     }

//     #[tokio::test]
//     /// Verifies a materialized view definition scans the persisted Parquet result.
//     async fn materialized_view_definition_build_provider_scans_parquet() {
//         use super::MaterializedView;
//         use datafusion::dataframe::DataFrameWriteOptions;

//         let root = create_temp_dir("table_ext_materialized_view");

//         // Write the query result to Parquet under the temp directory.
//         let writer_ctx = SessionContext::new();
//         let df = writer_ctx
//             .sql("SELECT 1 AS a, 2 AS b")
//             .await
//             .expect("query should plan");
//         let write_url = format!("file://{}/data/", root.to_string_lossy());
//         df.write_parquet(&write_url, DataFrameWriteOptions::new(), None)
//             .await
//             .expect("parquet result should be written");

//         let definition = MaterializedViewDefinition {
//             name: "mv".to_string(),
//             definition: "SELECT 1 AS a, 2 AS b".to_string(),
//             schema: Arc::new(Schema::empty()),
//             storage_location: "data/".to_string(),
//             created_at: 0,
//             last_refreshed: None,
//         };

//         // Materialized views always read from the tables store; register it
//         // rooted at the temp directory so `storage_location` resolves under it.
//         let context = Arc::new(SessionContext::new());
//         let tables_store_url = super::tables_object_store_url();
//         let tables_store =
//             Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&root).unwrap());
//         context.register_object_store(tables_store_url.as_ref(), tables_store);

//         let provider = definition
//             .build_provider(context.clone(), &tables_store_url)
//             .await
//             .expect("materialized view provider should be built");

//         let materialized = provider
//             .as_any()
//             .downcast_ref::<MaterializedView>()
//             .expect("provider should be a MaterializedView");
//         assert_eq!(materialized.base_storage_prefix(), "__materialized__/mv");

//         let schema = provider.schema();
//         assert_eq!(schema.fields().len(), 2);

//         let state = context.state();
//         let plan = provider
//             .scan(&state, None, &[], None)
//             .await
//             .expect("scan plan should be built");
//         let batches = datafusion::physical_plan::collect(plan, context.task_ctx())
//             .await
//             .expect("scan should execute");
//         let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
//         assert_eq!(rows, 1);

//         fs::remove_dir_all(&root).expect("temporary directory should be cleaned up");
//     }

//     #[test]
//     /// Verifies a logical table definition serializes/deserializes through the
//     /// typetag `TableDefinition` trait, preserving its `logical` tag, paths,
//     /// file type, and format options (e.g. a CSV delimiter).
//     fn logical_table_definition_serde_round_trip() {
//         let mut options = HashMap::new();
//         options.insert("format.delimiter".to_string(), ";".to_string());

//         let definition: Arc<dyn TableDefinition> = Arc::new(LogicalTableDefinition {
//             name: "argo".to_string(),
//             glob_paths: vec!["argo/*.csv".to_string()],
//             file_type: "csv".to_string(),
//             options,
//         });

//         let json = serde_json::to_value(&definition).expect("definition should serialize");
//         assert_eq!(json["definition_type"], "logical");
//         assert_eq!(json["glob_paths"][0], "argo/*.csv");
//         assert_eq!(json["file_type"], "csv");
//         assert_eq!(json["options"]["format.delimiter"], ";");

//         let restored: Arc<dyn TableDefinition> =
//             serde_json::from_value(json).expect("definition should deserialize");
//         assert_eq!(restored.table_name(), "argo");
//         assert_eq!(restored.table_type(), TableType::Base);
//     }

//     #[test]
//     /// Verifies the legacy `paths` key still deserializes into `glob_paths`.
//     fn logical_table_definition_accepts_paths_alias() {
//         let json = serde_json::json!({
//             "definition_type": "logical",
//             "name": "legacy",
//             "paths": ["a/*.parquet", "b/*.parquet"],
//             "file_type": "parquet"
//         });

//         let restored: Arc<dyn TableDefinition> =
//             serde_json::from_value(json).expect("definition with `paths` alias should deserialize");
//         assert_eq!(restored.table_name(), "legacy");
//     }

//     #[tokio::test]
//     /// Verifies a logical table builds a provider that merges schemas across
//     /// multiple glob paths into one super-type schema.
//     async fn logical_table_definition_build_provider_merges_glob_paths() {
//         let root = create_temp_dir("table_ext_logical_build");
//         let dir_a = root.join("a");
//         let dir_b = root.join("b");
//         fs::create_dir_all(&dir_a).expect("dir a should be created");
//         fs::create_dir_all(&dir_b).expect("dir b should be created");
//         fs::write(dir_a.join("part-0.csv"), "value\n1\n2\n").expect("csv a should be written");
//         fs::write(dir_b.join("part-0.csv"), "value\n3\n4\n").expect("csv b should be written");

//         let definition = LogicalTableDefinition {
//             name: "merged".to_string(),
//             glob_paths: vec![
//                 format!("{}/a/*.csv", to_store_relative_location(&root)),
//                 format!("{}/b/*.csv", to_store_relative_location(&root)),
//             ],
//             file_type: "csv".to_string(),
//             options: HashMap::new(),
//         };

//         let context = Arc::new(SessionContext::new());
//         let store_url = ObjectStoreUrl::parse("file://").unwrap();

//         let provider = definition
//             .build_provider(context, &store_url)
//             .await
//             .expect("logical provider should be built from glob paths");

//         let schema = provider.schema();
//         assert_eq!(schema.fields().len(), 1);
//         assert_eq!(schema.field(0).name(), "value");

//         fs::remove_dir_all(&root).expect("temporary directory should be cleaned up");
//     }

//     #[tokio::test]
//     /// Verifies a single-path logical table is read as a first-class
//     /// (self-refreshing) external table rather than a static file collection.
//     async fn logical_table_definition_single_path_builds_external_table() {
//         let root = create_temp_dir("table_ext_logical_single");
//         let dir = root.join("obs");
//         fs::create_dir_all(&dir).expect("dir should be created");
//         fs::write(dir.join("part-0.csv"), "value\n1\n2\n").expect("csv should be written");

//         let definition = LogicalTableDefinition {
//             name: "obs".to_string(),
//             glob_paths: vec![format!("{}/obs/*.csv", to_store_relative_location(&root))],
//             file_type: "csv".to_string(),
//             options: HashMap::new(),
//         };

//         let context = Arc::new(SessionContext::new());
//         let store_url = ObjectStoreUrl::parse("file://").unwrap();

//         let provider = definition
//             .build_provider(context, &store_url)
//             .await
//             .expect("single-path logical provider should be built");

//         assert!(
//             provider.as_any().downcast_ref::<ExternalTable>().is_some(),
//             "single-path logical table should map onto an ExternalTable"
//         );

//         let schema = provider.schema();
//         assert_eq!(schema.fields().len(), 1);
//         assert_eq!(schema.field(0).name(), "value");

//         fs::remove_dir_all(&root).expect("temporary directory should be cleaned up");
//     }
// }
