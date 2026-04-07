use std::{collections::HashMap, sync::Arc};

use crate::{
    query_result::QueryResult,
    sys::{self, SystemInfo},
    virtual_machine,
};
use arrow::datatypes::SchemaRef;
use beacon_data_lake::table::{Table, TableFormat};
use beacon_datafusion_ext::format_ext::DatasetMetadata;
use beacon_functions::function_doc::FunctionDoc;
use beacon_planner::metrics::ConsolidatedMetrics;
use beacon_query::{parser::Parser, Query};
use datafusion::execution::SendableRecordBatchStream;
use futures::stream::BoxStream;
use parking_lot::Mutex;

pub struct Runtime {
    virtual_machine: virtual_machine::VirtualMachine,
    query_metrics: Arc<Mutex<HashMap<uuid::Uuid, ConsolidatedMetrics>>>,
}

impl Runtime {
    pub async fn new() -> anyhow::Result<Self> {
        let virtual_machine = virtual_machine::VirtualMachine::new().await?;
        Ok(Self {
            virtual_machine,
            query_metrics: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn run_client_query(&self, query: Query) -> anyhow::Result<QueryResult> {
        let table_manager = self.virtual_machine.table_manager();
        let file_manager = self.virtual_machine.file_manager();
        let plan = beacon_planner::prelude::plan_query(
            self.virtual_machine.session_ctx(),
            table_manager.as_ref(),
            file_manager.as_ref(),
            query,
        )
        .await?;

        self.virtual_machine
            .run_plan(plan, self.query_metrics.clone())
            .await
    }

    pub fn system_info(&self) -> SystemInfo {
        sys::SystemInfo::new()
    }

    pub fn get_query_metrics(&self, query_id: uuid::Uuid) -> Option<ConsolidatedMetrics> {
        self.query_metrics.lock().get(&query_id).cloned()
    }

    pub async fn explain_client_query(&self, query: Query) -> anyhow::Result<String> {
        let table_manager = self.virtual_machine.table_manager();
        let file_manager = self.virtual_machine.file_manager();
        let plan = Parser::parse(
            self.virtual_machine.session_ctx().as_ref(),
            table_manager.as_ref(),
            file_manager.as_ref(),
            query,
        )
        .await?;
        let json = plan.datafusion_plan.display_pg_json().to_string();
        Ok(json)
    }

    pub fn list_functions(&self) -> Vec<FunctionDoc> {
        self.virtual_machine.list_functions()
    }

    pub fn list_table_functions(&self) -> Vec<FunctionDoc> {
        self.virtual_machine.list_table_functions()
    }

    pub async fn add_table(&self, table: Table) -> anyhow::Result<()> {
        self.virtual_machine.add_table(table).await
    }

    pub async fn apply_table_operation(
        &self,
        table_name: &str,
        op: serde_json::Value,
    ) -> anyhow::Result<()> {
        self.virtual_machine
            .apply_table_operation(table_name, op)
            .await
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

    pub async fn list_table_config(&self, table_name: String) -> Option<TableFormat> {
        self.virtual_machine.list_table_config(table_name).await
    }

    pub async fn list_table_schema(&self, table_name: String) -> Option<SchemaRef> {
        self.virtual_machine.list_table_schema(table_name).await
    }

    pub async fn list_default_table_schema(&self) -> SchemaRef {
        self.virtual_machine.list_default_table_schema().await
    }

    pub async fn list_datasets(
        &self,
        pattern: Option<String>,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<DatasetMetadata>> {
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

    pub async fn upload_file<S>(
        &self,
        file_path: &str,
        stream: S,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        S: futures::Stream<Item = Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>>
            + Unpin,
    {
        self.virtual_machine.upload_file(file_path, stream).await
    }

    pub async fn download_file(
        &self,
        file_path: &str,
    ) -> Result<
        BoxStream<'static, Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        self.virtual_machine.download_file(file_path).await
    }

    pub async fn delete_file(
        &self,
        file_path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.virtual_machine.delete_file(file_path).await
    }

    pub async fn run_sql(
        &self,
        sql: String,
        is_super_user: bool,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        self.virtual_machine.run_sql(sql, is_super_user).await
    }
}
