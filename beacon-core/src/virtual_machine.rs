use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_output::{Output, OutputFormat};
use datafusion::{
    catalog::TableProvider,
    datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl},
    execution::{
        disk_manager::DiskManagerConfig, memory_pool::FairSpillPool, object_store::ObjectStoreUrl,
        runtime_env::RuntimeEnvBuilder,
    },
    logical_expr::LogicalPlan,
    prelude::{DataFrame, SQLOptions, SessionConfig, SessionContext},
};
use futures::StreamExt;
use object_store::{local::LocalFileSystem, ObjectStore};
use tracing::{event, Level};

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

        session_context.register_object_store(
            ObjectStoreUrl::parse("file://").unwrap().as_ref(),
            beacon_config::OBJECT_STORE_LOCAL_FS.clone(),
        );

        Ok(Arc::new(session_context))
    }

    pub fn session_ctx(&self) -> Arc<SessionContext> {
        self.session_ctx.clone()
    }

    pub async fn run_client_sql(&self, sql: &str, output: &OutputFormat) -> anyhow::Result<Output> {
        let sql_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let df = self.session_ctx.sql_with_options(sql, sql_options).await?;
        output.output(self.session_ctx.clone(), df).await
    }

    pub async fn run_plan(
        &self,
        plan: LogicalPlan,
        output: &OutputFormat,
    ) -> anyhow::Result<Output> {
        let df = DataFrame::new(self.session_ctx.state(), plan);
        output.output(self.session_ctx.clone(), df).await
    }

    pub async fn run_sql(&self, sql: &str, output: &OutputFormat) -> anyhow::Result<Output> {
        let df = self.session_ctx.sql(sql).await?;

        output.output(self.session_ctx.clone(), df).await
    }

    pub async fn list_table_schema(&self, table_name: String) -> Option<SchemaRef> {
        self.session_ctx
            .table(table_name)
            .await
            .map(|t| Arc::new(t.schema().as_arrow().to_owned()))
            .ok()
    }

    pub async fn list_tables(&self) -> Vec<String> {
        self.session_ctx
            .catalog("datafusion")
            .unwrap()
            .schema("public")
            .unwrap()
            .table_names()
    }

    pub async fn list_dataset_schema(&self, file: String) -> anyhow::Result<SchemaRef> {
        let dataset_schema_path = format!(
            "file:///{}/{}",
            beacon_config::DATASETS_DIR_PREFIX.to_string(),
            file
        );
        let state = self.session_ctx.state();

        let table_url = ListingTableUrl::parse(&dataset_schema_path).map_err(|e| {
            anyhow::anyhow!(
                "Error parsing dataset path: {:?} - {:?}",
                dataset_schema_path,
                e
            )
        })?;
        let table_config = ListingTableConfig::new(table_url)
            .infer_options(&state)
            .await?
            .infer_schema(&state)
            .await?;

        let schema = table_config.file_schema.unwrap();

        Ok(schema)
    }

    pub async fn list_datasets(
        &self,
        pattern: Option<String>,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<String>> {
        let discovery_path = format!(
            "/{}/{}",
            beacon_config::DATASETS_DIR_PREFIX.to_string(),
            pattern.unwrap_or("*".to_string())
        );

        let state = self.session_ctx.state();

        let object_store = beacon_config::OBJECT_STORE_LOCAL_FS.clone();
        let table_url = ListingTableUrl::parse(&discovery_path).map_err(|e| {
            anyhow::anyhow!(
                "Error parsing discovery path: {:?} - {:?}",
                discovery_path,
                e
            )
        })?;

        let mut datasets = Vec::new();
        let mut stream = table_url
            .list_all_files(&state, object_store.as_ref(), "")
            .await
            .map_err(|e| anyhow::anyhow!("Error listing datasets: {:?}", e))?;

        while let Some(item) = stream.next().await {
            match item {
                Ok(item) => {
                    datasets.push(item.location.to_string());
                }
                Err(e) => {
                    event!(Level::ERROR, "Error listing datasets: {:?}", e);
                }
            }
        }
        datasets.sort();

        Ok(datasets
            .iter()
            .skip(offset.unwrap_or(0))
            .take(limit.unwrap_or(datasets.len()))
            .cloned()
            .map(|s| {
                //Remove the prefix from the file path
                s.replace(
                    &format!("{}/", beacon_config::DATASETS_DIR_PREFIX.to_string()),
                    "",
                )
            })
            .collect())
    }
}
