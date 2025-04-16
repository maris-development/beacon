use std::sync::Arc;

use crate::virtual_machine;
use arrow::datatypes::SchemaRef;
use beacon_functions::function_doc::FunctionDoc;
use beacon_output::OutputResponse;
use beacon_query::{parser::Parser, Query};
use beacon_sources::formats_factory::Formats;
use beacon_tables::table::Table;

pub struct Runtime {
    virtual_machine: virtual_machine::VirtualMachine,
}

impl Runtime {
    pub async fn new() -> anyhow::Result<Self> {
        let virtual_machine = virtual_machine::VirtualMachine::new().await?;
        Ok(Self { virtual_machine })
    }

    pub async fn run_client_query(&self, query: Query) -> anyhow::Result<OutputResponse> {
        let plan = Parser::parse(self.virtual_machine.session_ctx().as_ref(), query).await?;

        let output = self
            .virtual_machine
            .run_plan(plan.inner_datafusion_plan, &plan.output)
            .await?;
        Ok(output)
    }

    pub async fn explain_client_query(&self, query: Query) -> anyhow::Result<String> {
        let plan = Parser::parse(self.virtual_machine.session_ctx().as_ref(), query).await?;
        let json = plan.inner_datafusion_plan.display_pg_json().to_string();
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
