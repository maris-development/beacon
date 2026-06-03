//! Materialized views: query results persisted as Parquet and served from disk.

use std::sync::Arc;

use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Constraints, DataFusionError, Statistics, config_datafusion_err, not_impl_err};
use datafusion::datasource::TableType;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{Expr, SessionContext};
use datafusion::arrow::datatypes::SchemaRef;

use beacon_common::listing_url::parse_listing_table_url;

use super::definition::TableDefinition;
use super::listing::maybe_apply_default_glob;

/// A materialized view: a query whose result set is persisted as Parquet files
/// and served directly from disk instead of being recomputed on every read.
///
/// Wraps an inner [`ListingTable`] built over the persisted Parquet, mirroring how
/// [`ExternalTable`](super::ExternalTable) wraps a listing table. The wrapper is the
/// downcast target used by catalog persistence and by refresh/drop detection.
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
mod tests {
    use super::{MaterializedView, MaterializedViewDefinition};
    use crate::table_ext::TableDefinition;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::TableType;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::prelude::SessionContext;
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
