//! Beacon-core-owned contracts for outer layers such as beacon-api.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use beacon_data_lake::crawler::{CrawlReport, CrawlerDefinition, TableNaming};
use beacon_datafusion_ext::format_ext::DatasetMetadata;
use beacon_datafusion_ext::table_ext::TableDefinition;
use beacon_functions::function_doc::FunctionDoc;
use crate::metrics::ConsolidatedMetrics;
use serde_json::{json, Map, Value};
use utoipa::ToSchema;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct FunctionParameterInfo {
    pub name: String,
    pub description: String,
    pub data_type: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct FunctionInfo {
    pub function_name: String,
    pub description: String,
    pub return_type: String,
    pub params: Vec<FunctionParameterInfo>,
}

impl TryFrom<FunctionDoc> for FunctionInfo {
    type Error = anyhow::Error;

    fn try_from(value: FunctionDoc) -> Result<Self, Self::Error> {
        Ok(serde_json::from_value(serde_json::to_value(value)?)?)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct DatasetInfo {
    pub file_path: String,
    pub format: String,
    pub can_inspect: bool,
    pub can_partial_explore: bool,
}

impl From<DatasetMetadata> for DatasetInfo {
    fn from(value: DatasetMetadata) -> Self {
        Self {
            file_path: value.file_path,
            format: value.format,
            can_inspect: value.can_inspect,
            can_partial_explore: value.can_partial_explore,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct SchemaFieldView {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct SchemaView {
    pub fields: Vec<SchemaFieldView>,
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct QueryRequest {
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct QueryMetricsView {
    pub input_rows: u64,
    pub input_bytes: u64,
    pub result_num_rows: u64,
    pub result_size_in_bytes: u64,
    pub file_paths: Vec<String>,
    pub execution_time_ms: u64,
    pub query: Value,
    pub query_id: String,
    pub parsed_logical_plan: Value,
    pub optimized_logical_plan: Value,
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct TableConfigView {
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
    pub name: String,
    pub target_prefix: String,
    pub format_filter: Option<Vec<String>>,
    pub table_naming: TableNamingView,
    pub detect_partitions: bool,
    pub schedule_secs: Option<u64>,
    pub event_driven: bool,
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
