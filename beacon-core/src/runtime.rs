use crate::{
    output::Output,
    query::{parser::Parser, Query},
    virtual_machine,
};

pub struct Runtime {
    virtual_machine: virtual_machine::VirtualMachine,
}

impl Runtime {
    pub fn new() -> anyhow::Result<Self> {
        let virtual_machine = virtual_machine::VirtualMachine::new()?;
        Ok(Self { virtual_machine })
    }

    pub async fn run_client_query(&self, query: Query) -> anyhow::Result<Output> {
        let plan = Parser::parse(self.virtual_machine.session_ctx().as_ref(), query.inner).await?;
        let output = self.virtual_machine.run_plan(plan, &query.output).await?;
        Ok(output)
    }
}
