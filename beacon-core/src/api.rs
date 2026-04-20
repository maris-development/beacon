//! Beacon-core-owned contracts for outer layers such as beacon-api.

use std::collections::BTreeMap;

use arrow::datatypes::{Field, Schema};
use beacon_data_lake::table::TableFormat;
use beacon_datafusion_ext::format_ext::DatasetMetadata;
use beacon_functions::function_doc::FunctionDoc;
use beacon_planner::metrics::ConsolidatedMetrics;
use serde_json::{Map, Value};
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
    pub fn into_query(self) -> anyhow::Result<beacon_query::Query> {
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

impl TryFrom<TableFormat> for TableConfigView {
    type Error = anyhow::Error;

    fn try_from(value: TableFormat) -> Result<Self, Self::Error> {
        match serde_json::to_value(value)? {
            Value::Object(config) => Ok(Self {
                config: config.into_iter().collect(),
            }),
            other => Err(anyhow::anyhow!(
                "expected table config object, got {other:?}"
            )),
        }
    }
}
