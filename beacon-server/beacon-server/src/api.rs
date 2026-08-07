//! The wire contract: the JSON shapes this server accepts and returns.
//!
//! These live here, with the handlers that serialize them and the `ToSchema`
//! derives that document them, rather than in the runtime. The runtime deals in
//! Arrow and its own domain types — `SchemaRef`, `RecordBatch`,
//! `ConsolidatedMetrics`, `CrawlerDefinition` — and this module is where those
//! become the JSON a client sees.

use std::collections::{BTreeMap, HashMap};

use beacon_core::beacon_auth::{PrivilegeRule, PrivilegeTarget, Role, UserRecord};
use beacon_core::crawler::{CrawlReport, CrawlerDefinition, TableNaming};
use beacon_datafusion_ext::format_ext::DatasetMetadata;
use serde_json::{Map, Value};
use utoipa::ToSchema;

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

/// Documentation for a single function available in queries (scalar, aggregate,
/// or window), shaped from DataFusion's function catalog.
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

impl From<UserRecord> for AuthUserView {
    fn from(value: UserRecord) -> Self {
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

impl From<&PrivilegeRule> for AuthRuleView {
    fn from(rule: &PrivilegeRule) -> Self {
        let (target_type, target_value) = match &rule.target {
            None | Some(PrivilegeTarget::All) => ("all".to_string(), None),
            Some(PrivilegeTarget::Table(t)) => ("table".to_string(), Some(t.clone())),
            Some(PrivilegeTarget::Path(p)) => ("path".to_string(), Some(p.clone())),
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

impl From<Role> for AuthRoleView {
    fn from(role: Role) -> Self {
        // Rules live in a HashSet; sort for a stable, readable order.
        let to_sorted = |rules: &std::collections::HashSet<PrivilegeRule>| {
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

/// How an Arrow schema reaches a client.
///
/// Schemas are serialized as Arrow serializes them — `{ "fields": [...],
/// "metadata": {...} }`, each field carrying `name`, `data_type`, `nullable` and
/// `metadata` — rather than through a projection of beacon's own. The runtime
/// hands out a `SchemaRef`, and that *is* the contract: a simple type renders as
/// a string (`"Float64"`), a parameterized one as a single-key object
/// (`{"Timestamp": ["Microsecond", null]}`), which is exactly enough for a client
/// to reconstruct the type rather than parse a display string.
///
/// Documented as an opaque object because `arrow::datatypes::Schema` is not a
/// `ToSchema`; the example above is the shape.
pub const SCHEMA_RESPONSE: &str = "An Arrow schema: { fields: [{ name, data_type, nullable, metadata }], metadata }";

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
    pub fn into_query(self) -> anyhow::Result<beacon_core::query::Query> {
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
    /// The principal that ran the query (`anonymous` when none authenticated).
    #[schema(example = "beacon-admin")]
    pub username: String,
    /// When the query finished, RFC 3339 in UTC.
    #[schema(example = "2026-07-30T18:21:03.114Z")]
    pub finished_at: String,
    /// The logical plan as parsed, before optimization: the PostgreSQL-style
    /// `[{ "Plan": … }]` document plan viewers consume — an array, not an object.
    #[schema(value_type = Vec<Object>)]
    pub parsed_logical_plan: Value,
    /// The logical plan after the optimizer ran, in the same shape.
    #[schema(value_type = Vec<Object>)]
    pub optimized_logical_plan: Value,
    /// Per-node execution metrics from the physical plan (JSON).
    #[schema(value_type = Object)]
    pub node_metrics: Value,
}

/// Marks a timestamp as UTC when it carries no offset.
///
/// `finished_at` is stored as a zone-less timestamp holding a UTC instant, so it
/// renders without an offset — and a bare timestamp is read as *local* time by
/// most clients (JavaScript's `Date` among them). The wire contract is RFC 3339,
/// so the `Z` is supplied here.
fn utc_rfc3339(value: &str) -> String {
    if value.is_empty() {
        return String::new();
    }
    let time = value.rsplit('T').next().unwrap_or_default();
    let has_offset = value.ends_with('Z') || time.contains('+') || time.contains('-');
    if has_offset {
        value.to_string()
    } else {
        format!("{value}Z")
    }
}

impl QueryMetricsView {
    /// Maps one `beacon.system.query_metrics` row onto the wire shape.
    ///
    /// The table stores the open-ended parts — the query, both plans, the metric
    /// tree, the file list — as JSON strings, because their shape follows
    /// DataFusion's and would otherwise pin the table schema to it. The contract
    /// here has always been nested JSON, so they are parsed back; anything
    /// unparseable degrades to `null` rather than failing the response.
    pub fn from_row(row: &Value) -> Self {
        let text = |key: &str| row.get(key).and_then(Value::as_str).unwrap_or_default();
        let count = |key: &str| row.get(key).and_then(Value::as_u64).unwrap_or_default();
        let json = |key: &str| serde_json::from_str(text(key)).unwrap_or(Value::Null);

        Self {
            input_rows: count("input_rows"),
            input_bytes: count("input_bytes"),
            result_num_rows: count("result_num_rows"),
            result_size_in_bytes: count("result_size_in_bytes"),
            file_paths: serde_json::from_str(text("file_paths")).unwrap_or_default(),
            execution_time_ms: count("execution_time_ms"),
            query: json("query"),
            query_id: text("query_id").to_string(),
            username: text("username").to_string(),
            finished_at: utc_rfc3339(text("finished_at")),
            parsed_logical_plan: json("parsed_logical_plan"),
            optimized_logical_plan: json("optimized_logical_plan"),
            node_metrics: json("node_metrics"),
        }
    }
}

/// The body of an endpoint that is kept routed but no longer does anything, so a
/// client that still calls it is told why rather than left with a 404.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct DeprecationNotice {
    /// What replaced the endpoint, in a sentence a human can act on.
    pub message: String,
}

impl DeprecationNotice {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// How a crawler turns a discovered group of files into a table name. Mirrors the
/// server-side [`TableNaming`] so the API surface need not depend on its internals.
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
/// surface as structured JSON; maps into a server-side [`CrawlerDefinition`].
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

/// The outcome of a single crawler run. Mirrors the server-side [`CrawlReport`].
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
    /// or a scheme-qualified location for `REMOTE`/`DELTA`/`ICECHUNK` types.
    #[schema(example = "obs/")]
    pub location: String,
    /// Storage type, e.g. `PARQUET`, `CSV`, `DELTA`, `ICECHUNK`, `REMOTE`.
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
mod query_request_tests {
    use super::*;

    fn request(json: &str) -> QueryRequest {
        serde_json::from_str(json).expect("request body should deserialize")
    }

    /// The request body is a free-form flattened map that is re-serialized into a
    /// [`Query`](beacon_core::query::Query); the SQL form must survive that round trip with its
    /// output options intact, since this is the only path a REST client's query
    /// takes into the runtime.
    #[test]
    fn sql_request_round_trips_into_a_sql_query() {
        let query = request(r#"{"sql": "SELECT 1", "output": {"format": "csv"}}"#)
            .into_query()
            .expect("a SQL body should convert");

        assert!(matches!(query.inner, beacon_core::query::InnerQuery::Sql(sql) if sql == "SELECT 1"));
        assert!(query.output.is_some());
    }

    /// The structured form's fields live at the top level of the body (they are
    /// flattened), so they must reach the JSON query compiler rather than being
    /// mistaken for unknown keys.
    #[test]
    fn structured_request_round_trips_into_a_json_query() {
        let query = request(r#"{"select": [{"column": "depth"}], "limit": 5}"#)
            .into_query()
            .expect("a structured body should convert");

        assert!(matches!(query.inner, beacon_core::query::InnerQuery::Json(_)));
        assert!(query.output.is_none());
    }

    /// Conversion is where a malformed body is caught — the map itself accepts any
    /// JSON object, so a typo'd key must fail here instead of being silently
    /// dropped and producing a subtly different query.
    #[test]
    fn unknown_keys_are_rejected_at_conversion() {
        // Note: `InnerQuery` is untagged, so serde reports only "data did not
        // match any variant" — the offending key is not named. The contract tested
        // here is that the body is *rejected*, not that the message is precise.
        assert!(request(r#"{"select": ["depth"], "limmit": 5}"#)
            .into_query()
            .is_err());

        // A body that is neither SQL nor a structured query is rejected too.
        assert!(request(r#"{"nonsense": true}"#).into_query().is_err());
    }
}
