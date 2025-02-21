use std::sync::Arc;

use datafusion::{
    execution::{
        disk_manager::DiskManagerConfig, memory_pool::FairSpillPool, runtime_env::RuntimeEnvBuilder,
    },
    prelude::{SQLOptions, SessionConfig, SessionContext},
};

use crate::{
    output::{Output, OutputFormat},
    providers::{datasets::DatasetsProviderFunction, BeaconProvider},
};

pub struct VirtualMachine {
    memory_pool: Arc<FairSpillPool>,
    session_ctx: Arc<SessionContext>,
}

impl VirtualMachine {
    pub fn new() -> anyhow::Result<Self> {
        let memory_pool = Arc::new(FairSpillPool::new(
            beacon_config::CONFIG.vm_memory_size * 1024 * 1024,
        ));

        let session_ctx = Self::init_ctx(memory_pool.clone())?;

        Ok(Self {
            memory_pool,
            session_ctx,
        })
    }

    fn init_ctx(mem_pool: Arc<FairSpillPool>) -> anyhow::Result<Arc<SessionContext>> {
        let mut config = SessionConfig::new()
            .with_batch_size(1024 * 1024 * 4)
            .with_coalesce_batches(true)
            .with_information_schema(true);

        config.options_mut().sql_parser.enable_ident_normalization = false;

        let disk_manager_conf = DiskManagerConfig::NewOs;

        let runtime_env = RuntimeEnvBuilder::new()
            .with_disk_manager(disk_manager_conf)
            .with_memory_pool(mem_pool)
            .build_arc()?;

        let session_context = SessionContext::new_with_config_rt(config, runtime_env);

        for provider in inventory::iter::<&'static dyn BeaconProvider> {
            let function = provider.function();
            let schema_function = provider.schema_function();

            session_context.register_udtf(function.name(), function.function().clone());
            session_context
                .register_udtf(schema_function.name(), schema_function.function().clone());
        }

        session_context.register_udtf("datasets", Arc::new(DatasetsProviderFunction));

        Ok(Arc::new(session_context))
    }

    pub async fn run_client_sql(&self, sql: &str, output: &OutputFormat) -> anyhow::Result<Output> {
        let sql_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let df = self.session_ctx.sql_with_options(sql, sql_options).await?;
        output.output(self.session_ctx.clone(), df).await
    }

    pub async fn run_sql(&self, sql: &str, output: &OutputFormat) -> anyhow::Result<Output> {
        let df = self.session_ctx.sql(sql).await?;

        output.output(self.session_ctx.clone(), df).await
    }
}
