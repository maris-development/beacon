use crate::virtual_machine;
use arrow::datatypes::SchemaRef;
use beacon_output::Output;
use beacon_query::{parser::Parser, Query};

pub struct Runtime {
    virtual_machine: virtual_machine::VirtualMachine,
}

impl Runtime {
    pub fn new() -> anyhow::Result<Self> {
        let virtual_machine = virtual_machine::VirtualMachine::new()?;
        Ok(Self { virtual_machine })
    }

    pub async fn run_client_query(&self, query: Query) -> anyhow::Result<Output> {
        let plan = Parser::parse(self.virtual_machine.session_ctx().as_ref(), query).await?;
        let output = self
            .virtual_machine
            .run_plan(plan.inner_datafusion_plan, &plan.output)
            .await?;
        Ok(output)
    }

    pub async fn list_tables(&self) -> Vec<String> {
        self.virtual_machine.list_tables().await
    }

    pub async fn list_table_schema(&self, table_name: String) -> Option<SchemaRef> {
        self.virtual_machine.list_table_schema(table_name).await
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

    pub async fn list_dataset_schema(&self, file: String) -> anyhow::Result<SchemaRef> {
        self.virtual_machine.list_dataset_schema(file).await
    }
}
