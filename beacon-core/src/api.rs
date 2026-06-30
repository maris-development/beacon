//! Beacon-core-owned contracts for outer layers such as beacon-api.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use beacon_data_lake::crawler::{CrawlReport, CrawlerDefinition, TableNaming};
use beacon_datafusion_ext::format_ext::DatasetMetadata;
use beacon_datafusion_ext::table_ext::TableDefinition;
use beacon_functions::function_doc::FunctionDoc;
use crate::metrics::ConsolidatedMetrics;
use serde_json::{Map, Value};
use utoipa::ToSchema;

/// Re-exported typed table-extension contracts (see [`crate::extensions`]).
pub use crate::extensions::{
    McpExtension, Preset, PresetExtension, PresetFilter, PresetOp, TableExtensions,
};

/// A single parameter of a registered function.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct FunctionParameterInfo {
    /// Parameter name as used in the function signature.
    #[schema(example = "input")]
    pub name: String,
    /// Human-readable description of the parameter's purpose.
    pub description: String,
    /// SQL/Arrow data type accepted by the parameter (e.g. `Float64`, `Utf8`).
    #[schema(example = "Float64")]
    pub data_type: String,
}

/// Documentation for a single function registered with the runtime
/// (scalar, aggregate, or table-valued).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct FunctionInfo {
    /// The name the function is invoked by in queries.
    #[schema(example = "abs")]
    pub function_name: String,
    /// Human-readable description of what the function does.
    pub description: String,
    /// The data type the function returns (e.g. `Float64`).
    #[schema(example = "Float64")]
    pub return_type: String,
    /// Ordered list of the function's parameters.
    pub params: Vec<FunctionParameterInfo>,
}

impl TryFrom<FunctionDoc> for FunctionInfo {
    type Error = anyhow::Error;

    fn try_from(value: FunctionDoc) -> Result<Self, Self::Error> {
        Ok(serde_json::from_value(serde_json::to_value(value)?)?)
    }
}

/// Metadata about a single dataset file discovered in the datasets store.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct DatasetInfo {
    /// Datasets-store-relative path of the file.
    #[schema(example = "argo/floats.parquet")]
    pub file_path: String,
    /// Detected file format identifier (e.g. `parquet`, `nc`, `csv`).
    #[schema(example = "parquet")]
    pub format: String,
    /// Whether the runtime can read this file's schema for inspection.
    pub can_inspect: bool,
    /// Whether the file supports partial (predicate/column-pushdown) exploration.
    pub can_partial_explore: bool,
    /// Size in bytes of the underlying object(s), when known.
    pub size: Option<u64>,
    /// Last-modified timestamp (RFC 3339), when known.
    pub last_modified: Option<String>,
}

impl From<DatasetMetadata> for DatasetInfo {
    fn from(value: DatasetMetadata) -> Self {
        Self {
            file_path: value.file_path,
            format: value.format,
            can_inspect: value.can_inspect,
            can_partial_explore: value.can_partial_explore,
            size: value.size,
            last_modified: value.last_modified.map(|dt| dt.to_rfc3339()),
        }
    }
}

/// A user account and the roles assigned to it, for the admin Users page.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct AuthUserView {
    pub username: String,
    pub roles: Vec<String>,
    /// True for the single config-defined super-user (not editable via SQL).
    pub is_super_user: bool,
    /// True for the Beacon-managed anonymous user, which can't be deleted while
    /// anonymous access is enabled.
    pub is_anonymous: bool,
}

impl From<beacon_auth::UserRecord> for AuthUserView {
    fn from(value: beacon_auth::UserRecord) -> Self {
        Self {
            username: value.username,
            roles: value.roles,
            is_super_user: false,
            is_anonymous: false,
        }
    }
}

/// A single grant/deny rule, flattened for the UI.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct AuthRuleView {
    /// Privilege name, e.g. `SELECT`.
    pub privilege: String,
    /// Target kind: `table`, `path`, or `all` (every target).
    pub target_type: String,
    /// Table name or path glob; `None` when the target is `all`.
    pub target_value: Option<String>,
}

impl From<&beacon_auth::PrivilegeRule> for AuthRuleView {
    fn from(rule: &beacon_auth::PrivilegeRule) -> Self {
        let (target_type, target_value) = match &rule.target {
            None | Some(beacon_auth::PrivilegeTarget::All) => ("all".to_string(), None),
            Some(beacon_auth::PrivilegeTarget::Table(t)) => ("table".to_string(), Some(t.clone())),
            Some(beacon_auth::PrivilegeTarget::Path(p)) => ("path".to_string(), Some(p.clone())),
        };
        Self {
            privilege: rule.privilege.to_string(),
            target_type,
            target_value,
        }
    }
}

/// A role with its grant and deny rules, for the admin Roles page.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct AuthRoleView {
    pub name: String,
    pub grants: Vec<AuthRuleView>,
    pub denies: Vec<AuthRuleView>,
}

impl From<beacon_auth::Role> for AuthRoleView {
    fn from(role: beacon_auth::Role) -> Self {
        // Rules live in a HashSet; sort for a stable, readable order.
        let to_sorted = |rules: &std::collections::HashSet<beacon_auth::PrivilegeRule>| {
            let mut views: Vec<AuthRuleView> = rules.iter().map(AuthRuleView::from).collect();
            views.sort_by(|a, b| {
                (&a.privilege, &a.target_type, &a.target_value).cmp(&(
                    &b.privilege,
                    &b.target_type,
                    &b.target_value,
                ))
            });
            views
        };
        Self {
            grants: to_sorted(&role.grants),
            denies: to_sorted(&role.denies),
            name: role.name,
        }
    }
}

/// A single field (column) of an Arrow schema, projected for the API.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct SchemaFieldView {
    /// Column name.
    #[schema(example = "temperature")]
    pub name: String,
    /// Arrow data type rendered as a string (e.g. `Float64`, `Utf8`, `Timestamp(...)`).
    #[schema(example = "Float64")]
    pub data_type: String,
    /// Whether the column may contain null values.
    pub nullable: bool,
    /// Arbitrary field-level metadata carried on the Arrow field.
    pub metadata: BTreeMap<String, String>,
}

impl From<&Field> for SchemaFieldView {
    fn from(value: &Field) -> Self {
        Self {
            name: value.name().to_string(),
            data_type: value.data_type().to_string(),
            nullable: value.is_nullable(),
            metadata: value.metadata().clone().into_iter().collect(),
        }
    }
}

/// An Arrow schema (the ordered fields plus schema-level metadata), projected for the API.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct SchemaView {
    /// The schema's fields (columns), in order.
    pub fields: Vec<SchemaFieldView>,
    /// Arbitrary schema-level metadata key/value pairs.
    pub metadata: BTreeMap<String, String>,
}

impl From<&Schema> for SchemaView {
    fn from(value: &Schema) -> Self {
        Self {
            fields: value
                .fields()
                .iter()
                .map(|field| SchemaFieldView::from(field.as_ref()))
                .collect(),
            metadata: value.metadata().clone().into_iter().collect(),
        }
    }
}

/// A Beacon query request body. The payload is a free-form JSON object describing
/// either a structured JSON query or a SQL query (`{"sql": "SELECT ..."}`), along
/// with the desired output format. The object is flattened, so its keys appear at
/// the top level of the request body rather than under a `query` field.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[schema(example = json!({ "sql": "SELECT 1", "output": { "format": "csv" } }))]
pub struct QueryRequest {
    /// The flattened query object (JSON or SQL query plus output options).
    #[schema(value_type = Object)]
    #[serde(flatten)]
    pub query: BTreeMap<String, Value>,
}

impl QueryRequest {
    pub fn into_query(self) -> anyhow::Result<crate::query::Query> {
        Ok(serde_json::from_value(Value::Object(
            self.query.into_iter().collect::<Map<String, Value>>(),
        ))?)
    }
}

/// Planner and execution metrics recorded for a previously executed query,
/// retrievable by query id via `GET /api/query/metrics/{query_id}`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct QueryMetricsView {
    /// Total rows scanned from the input sources.
    pub input_rows: u64,
    /// Total bytes scanned from the input sources.
    pub input_bytes: u64,
    /// Number of rows in the query result.
    pub result_num_rows: u64,
    /// Size of the query result in bytes.
    pub result_size_in_bytes: u64,
    /// Source file paths touched while executing the query.
    pub file_paths: Vec<String>,
    /// Wall-clock execution time in milliseconds.
    pub execution_time_ms: u64,
    /// The original query payload that produced these metrics.
    #[schema(value_type = Object)]
    pub query: Value,
    /// The query's unique identifier (UUID).
    pub query_id: String,
    /// The logical plan as parsed, before optimization (JSON).
    #[schema(value_type = Object)]
    pub parsed_logical_plan: Value,
    /// The logical plan after the optimizer ran (JSON).
    #[schema(value_type = Object)]
    pub optimized_logical_plan: Value,
    /// Per-node execution metrics from the physical plan (JSON).
    #[schema(value_type = Object)]
    pub node_metrics: Value,
}

impl TryFrom<ConsolidatedMetrics> for QueryMetricsView {
    type Error = anyhow::Error;

    fn try_from(value: ConsolidatedMetrics) -> Result<Self, Self::Error> {
        Ok(Self {
            input_rows: value.input_rows,
            input_bytes: value.input_bytes,
            result_num_rows: value.result_num_rows,
            result_size_in_bytes: value.result_size_in_bytes,
            file_paths: value.file_paths,
            execution_time_ms: value.execution_time_ms,
            query: value.query,
            query_id: value.query_id.to_string(),
            parsed_logical_plan: value.parsed_logical_plan,
            optimized_logical_plan: value.optimized_logical_plan,
            node_metrics: serde_json::to_value(value.node_metrics)?,
        })
    }
}

/// The storage format and options of a registered table, as a flattened
/// configuration object. Internal (double-underscore) option keys are stripped.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct TableConfigView {
    /// The flattened table configuration (type, location, options, ...).
    #[schema(value_type = Object)]
    #[serde(flatten)]
    pub config: BTreeMap<String, Value>,
}

impl TryFrom<Arc<dyn TableDefinition>> for TableConfigView {
    type Error = anyhow::Error;

    fn try_from(value: Arc<dyn TableDefinition>) -> Result<Self, Self::Error> {
        match serde_json::to_value(value)? {
            Value::Object(mut config) => {
                // Hide internal (double-underscore) option keys — e.g. the crawler
                // ownership marker — from the user-facing config. They are an
                // implementation detail of the definition, not user-set options.
                if let Some(Value::Object(options)) = config.get_mut("options") {
                    options.retain(|key, _| !key.starts_with("__"));
                }
                // Never expose a persisted credential — even encrypted — through
                // the admin table-config endpoint (external SQL-database tables
                // carry one in `secret`).
                if config.contains_key("secret") {
                    config.insert("secret".to_string(), Value::String("***".to_string()));
                }
                Ok(Self {
                    config: config.into_iter().collect(),
                })
            }
            other => Err(anyhow::anyhow!(
                "expected table config object, got {other:?}"
            )),
        }
    }
}

/// How a crawler turns a discovered group of files into a table name. Mirrors the
/// data-lake [`TableNaming`] so the API surface need not depend on its internals.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TableNamingView {
    /// Use the leaf component of the group's base prefix (`argo/floats` -> `floats`).
    #[default]
    LeafPrefix,
    /// Prefix the leaf with the crawler name (`<crawler>_<leaf>`).
    CrawlerPrefixed,
}

impl From<TableNaming> for TableNamingView {
    fn from(value: TableNaming) -> Self {
        match value {
            TableNaming::LeafPrefix => Self::LeafPrefix,
            TableNaming::CrawlerPrefixed => Self::CrawlerPrefixed,
        }
    }
}

impl From<TableNamingView> for TableNaming {
    fn from(value: TableNamingView) -> Self {
        match value {
            TableNamingView::LeafPrefix => Self::LeafPrefix,
            TableNamingView::CrawlerPrefixed => Self::CrawlerPrefixed,
        }
    }
}

/// Request body to define (or replace) a crawler. Mirrors the SQL `CREATE CRAWLER`
/// surface as structured JSON; maps into a data-lake [`CrawlerDefinition`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[schema(example = json!({
    "name": "argo",
    "target_prefix": "argo/",
    "format_filter": ["parquet", "nc"],
    "table_naming": "crawler_prefixed",
    "detect_partitions": true,
    "schedule_secs": 900,
    "event_driven": false,
    "options": { "read_dimensions": "lat,lon" }
}))]
pub struct CreateCrawlerRequest {
    /// Unique crawler name.
    #[schema(example = "argo")]
    pub name: String,
    /// Datasets-store prefix to scan, e.g. `argo/`.
    #[schema(example = "argo/")]
    pub target_prefix: String,
    /// Restrict discovery to these format identifiers (e.g. `["parquet", "nc"]`).
    /// Omit (or `null`) to crawl every registered format.
    #[serde(default)]
    #[schema(example = json!(["parquet", "nc"]))]
    pub format_filter: Option<Vec<String>>,
    /// How discovered groups are named.
    #[serde(default)]
    pub table_naming: TableNamingView,
    /// Detect Hive-style `key=value/` partitions (default `true`).
    #[serde(default = "default_true")]
    #[schema(default = true, example = true)]
    pub detect_partitions: bool,
    /// Periodic crawl interval, in seconds. Omit for no timer.
    #[serde(default)]
    #[schema(example = 900)]
    pub schedule_secs: Option<u64>,
    /// Subscribe to datasets-store events under `target_prefix` for incremental crawls.
    #[serde(default)]
    #[schema(default = false, example = false)]
    pub event_driven: bool,
    /// Extra format options forwarded into every discovered table's `OPTIONS`.
    #[serde(default)]
    #[schema(example = json!({ "read_dimensions": "lat,lon" }))]
    pub options: HashMap<String, String>,
}

fn default_true() -> bool {
    true
}

impl From<CreateCrawlerRequest> for CrawlerDefinition {
    fn from(value: CreateCrawlerRequest) -> Self {
        CrawlerDefinition {
            name: value.name,
            target_prefix: value.target_prefix,
            format_filter: value.format_filter,
            table_naming: value.table_naming.into(),
            detect_partitions: value.detect_partitions,
            schedule_secs: value.schedule_secs,
            event_driven: value.event_driven,
            options: value.options,
        }
    }
}

/// A crawler definition as returned to API clients. Mirrors [`CrawlerDefinition`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct CrawlerView {
    /// Unique crawler name.
    pub name: String,
    /// Datasets-store prefix the crawler scans, e.g. `argo/`.
    pub target_prefix: String,
    /// Format identifiers discovery is restricted to, or `null` to crawl every
    /// registered format.
    pub format_filter: Option<Vec<String>>,
    /// How discovered groups are turned into table names.
    pub table_naming: TableNamingView,
    /// Whether Hive-style `key=value/` partitions are detected.
    pub detect_partitions: bool,
    /// Periodic crawl interval in seconds, or `null` for no timer.
    pub schedule_secs: Option<u64>,
    /// Whether the crawler subscribes to datasets-store events for incremental crawls.
    pub event_driven: bool,
    /// Extra format options forwarded into every discovered table's `OPTIONS`.
    pub options: HashMap<String, String>,
}

impl From<CrawlerDefinition> for CrawlerView {
    fn from(value: CrawlerDefinition) -> Self {
        Self {
            name: value.name,
            target_prefix: value.target_prefix,
            format_filter: value.format_filter,
            table_naming: value.table_naming.into(),
            detect_partitions: value.detect_partitions,
            schedule_secs: value.schedule_secs,
            event_driven: value.event_driven,
            options: value.options,
        }
    }
}

/// The outcome of a single crawler run. Mirrors the data-lake [`CrawlReport`].
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct CrawlReportView {
    /// Crawler name.
    pub crawler: String,
    /// Candidate tables discovered.
    pub discovered: usize,
    /// Newly registered tables.
    pub created: Vec<String>,
    /// Existing crawler-owned tables that were refreshed.
    pub updated: Vec<String>,
    /// Tables left untouched because they are not owned by this crawler.
    pub skipped: Vec<String>,
    /// Per-table failures as `[name, error message]` pairs.
    pub failed: Vec<(String, String)>,
    /// Files that did not match any crawlable format.
    pub skipped_files: usize,
}

impl From<CrawlReport> for CrawlReportView {
    fn from(value: CrawlReport) -> Self {
        Self {
            crawler: value.crawler,
            discovered: value.discovered,
            created: value.created,
            updated: value.updated,
            skipped: value.skipped,
            failed: value.failed,
            skipped_files: value.skipped_files,
        }
    }
}

/// Request body to create an external table from structured fields. The runtime
/// assembles the equivalent `CREATE EXTERNAL TABLE` statement and runs it through
/// the same DDL path as SQL.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[schema(example = json!({
    "name": "observations",
    "location": "obs/",
    "file_type": "PARQUET",
    "partition_cols": ["year", "month"],
    "options": {},
    "if_not_exists": false
}))]
pub struct CreateExternalTableRequest {
    /// Logical table name.
    #[schema(example = "observations")]
    pub name: String,
    /// Datasets-store-relative location or glob (e.g. `obs/` or `data/**/*.parquet`),
    /// or a scheme-qualified location for `REMOTE`/`DELTA` types.
    #[schema(example = "obs/")]
    pub location: String,
    /// Storage type, e.g. `PARQUET`, `CSV`, `DELTA`, `REMOTE`.
    #[schema(example = "PARQUET")]
    pub file_type: String,
    /// Hive-style partition columns, in path order.
    #[serde(default)]
    #[schema(example = json!(["year", "month"]))]
    pub partition_cols: Vec<String>,
    /// Format-specific options forwarded to the table's `OPTIONS`.
    #[serde(default)]
    pub options: HashMap<String, String>,
    /// Skip creation (instead of erroring) when the table already exists.
    #[serde(default)]
    #[schema(default = false, example = false)]
    pub if_not_exists: bool,
}

#[cfg(test)]
mod table_config_redaction_tests {
    use super::*;
    use beacon_sql_databases::{EncryptedSecret, SqlDatabaseTableDefinition, SqlEngine};

    /// The admin table-config view must never expose a persisted credential,
    /// even in its encrypted form — the `secret` field is replaced with `***`.
    #[test]
    fn sql_database_secret_is_redacted_in_config_view() {
        let mut options = BTreeMap::new();
        options.insert("host".to_string(), "db.internal".to_string());
        let definition: Arc<dyn TableDefinition> = Arc::new(SqlDatabaseTableDefinition {
            name: "orders".to_string(),
            engine: SqlEngine::Postgres,
            remote_table: "public.orders".to_string(),
            schema: beacon_sql_databases::unresolved_schema(),
            options,
            secret: Some(EncryptedSecret::encrypt("super-secret-password", &[9u8; 32]).unwrap()),
        });

        let view = TableConfigView::try_from(definition).unwrap();
        let json = serde_json::to_string(&view).unwrap();

        assert!(!json.contains("super-secret-password"));
        // The encrypted material (ciphertext/nonce) must not leak either.
        assert!(!json.contains("ciphertext"));
        assert_eq!(view.config.get("secret"), Some(&Value::String("***".to_string())));
        // Non-secret connection options remain visible.
        assert!(json.contains("db.internal"));
    }
}
