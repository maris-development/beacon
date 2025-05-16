use std::sync::Arc;

use crate::{query_result::QueryResult, virtual_machine};
use arrow::datatypes::SchemaRef;
use beacon_functions::function_doc::FunctionDoc;
use beacon_output::OutputResponse;
use beacon_planner::{metrics::ConsolidatedMetrics, prelude::MetricsTracker};
use beacon_query::{output::QueryOutputFile, parser::Parser, Query};
use beacon_sources::formats_factory::Formats;
use beacon_tables::table::Table;
use datafusion::{common::HashMap, prelude::DataFrame};
use parking_lot::Mutex;

pub struct Runtime {
    virtual_machine: virtual_machine::VirtualMachine,
    query_metrics: Mutex<HashMap<uuid::Uuid, ConsolidatedMetrics>>,
}

impl Runtime {
    pub async fn new() -> anyhow::Result<Self> {
        let virtual_machine = virtual_machine::VirtualMachine::new().await?;
        Ok(Self {
            virtual_machine,
            query_metrics: Mutex::new(HashMap::new()),
        })
    }

    pub async fn run_client_query(&self, query: Query) -> anyhow::Result<QueryResult> {
        let plan =
            beacon_planner::prelude::plan_query(self.virtual_machine.session_ctx(), query).await?;

        self.virtual_machine.run_plan(&plan).await?;

        self.query_metrics.lock().insert(
            plan.query_id,
            plan.metrics_tracker.get_consolidated_metrics(),
        );

        Ok(QueryResult {
            output_buffer: plan.output_buffer,
            query_id: plan.query_id,
        })
    }

    pub fn get_query_metrics(&self, query_id: uuid::Uuid) -> Option<ConsolidatedMetrics> {
        self.query_metrics.lock().get(&query_id).cloned()
    }

    pub async fn explain_client_query(&self, query: Query) -> anyhow::Result<String> {
        let plan = Parser::parse(self.virtual_machine.session_ctx().as_ref(), query).await?;
        let json = plan.datafusion_plan.display_pg_json().to_string();
        Ok(json)
    }

    pub fn list_functions(&self) -> Vec<FunctionDoc> {
        self.virtual_machine.list_functions()
    }

    pub async fn add_table(&self, table: Table) -> anyhow::Result<()> {
        self.virtual_machine.add_table(table).await
    }

    pub async fn delete_table(&self, table_name: &str) -> anyhow::Result<()> {
        self.virtual_machine.delete_table(table_name).await
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.virtual_machine.list_tables()
    }

    pub fn default_table(&self) -> String {
        self.virtual_machine.default_table()
    }

    pub async fn list_table_config(&self, table_name: String) -> anyhow::Result<Table> {
        self.virtual_machine.list_table_config(table_name).await
    }

    pub async fn list_table_schema(&self, table_name: String) -> Option<SchemaRef> {
        self.virtual_machine.list_table_schema(table_name).await
    }

    pub async fn list_table_extensions(
        &self,
        table_name: String,
    ) -> anyhow::Result<Vec<Arc<dyn beacon_tables::table_extension::TableExtension>>> {
        self.virtual_machine.list_table_extensions(table_name).await
    }

    pub async fn list_default_table_schema(&self) -> SchemaRef {
        self.virtual_machine.list_default_table_schema().await
    }

    pub async fn list_datasets(
        &self,
        pattern: Option<String>,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<String>> {
        self.virtual_machine
            .list_datasets(pattern, offset, limit)
            .await
    }

    pub async fn total_datasets(&self) -> anyhow::Result<usize> {
        self.virtual_machine.total_datasets().await
    }

    pub async fn list_dataset_schema(&self, file: String) -> anyhow::Result<SchemaRef> {
        self.virtual_machine.list_dataset_schema(file).await
    }
}
