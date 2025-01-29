use crate::{output::Output, query::Query, virtual_machine};

pub struct Runtime {
    virtual_machine: virtual_machine::VirtualMachine,
}

impl Runtime {
    pub fn new() -> anyhow::Result<Self> {
        let virtual_machine = virtual_machine::VirtualMachine::new()?;
        Ok(Self { virtual_machine })
    }

    pub async fn run_client_query(&self, query: Query) -> anyhow::Result<Output> {
        match query.inner {
            crate::query::InnerQuery::Sql(sql) => {
                self.virtual_machine
                    .run_client_sql(&sql, &query.output)
                    .await
            }
            crate::query::InnerQuery::Json(query_body) => {
                return anyhow::Result::Err(anyhow::anyhow!("Not implemented yet"));
            }
        }
    }
}
