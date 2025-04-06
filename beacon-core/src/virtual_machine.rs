use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_functions::function_doc::FunctionDoc;
use beacon_output::{OutputFormat, OutputResponse};
use beacon_sources::{
    formats_factory::Formats, netcdf_format::NetCDFFileFormatFactory,
    parquet_format::SuperParquetFormatFactory,
};
use beacon_tables::{schema_provider::BeaconSchemaProvider, table::Table};
use datafusion::{
    catalog::SchemaProvider,
    datasource::{
        file_format::{
            arrow::ArrowFormatFactory, csv::CsvFormatFactory, parquet::ParquetFormatFactory,
            FileFormatFactory,
        },
        listing::{ListingTableConfig, ListingTableUrl},
    },
    execution::{
        disk_manager::DiskManagerConfig, memory_pool::FairSpillPool, object_store::ObjectStoreUrl,
        runtime_env::RuntimeEnvBuilder, SessionStateBuilder,
    },
    logical_expr::LogicalPlan,
    prelude::{DataFrame, SQLOptions, SessionConfig, SessionContext},
};
use futures::StreamExt;
use tracing::{event, Level};

pub struct VirtualMachine {
    session_ctx: Arc<SessionContext>,
    schema_provider: Arc<BeaconSchemaProvider>,
}

impl VirtualMachine {
    pub async fn new() -> anyhow::Result<Self> {
        let memory_pool = Arc::new(FairSpillPool::new(
            beacon_config::CONFIG.vm_memory_size * 1024 * 1024,
        ));

        let session_ctx = Self::init_ctx(memory_pool.clone())?;

        let beacon_schema_provider =
            Arc::new(BeaconSchemaProvider::new(session_ctx.clone()).await?);

        session_ctx
            .catalog("datafusion")
            .unwrap()
            .register_schema("public", beacon_schema_provider.clone())?;

        //INIT FUNCTIONS FROM beacon-functions module
        let geo_udfs = beacon_functions::geo::geo_udfs();
        for udf in geo_udfs {
            session_ctx.register_udf(udf);
        }

        let utils_ufds = beacon_functions::util::util_udfs();
        for udf in utils_ufds {
            session_ctx.register_udf(udf);
        }

        let blue_cloud_udfs = beacon_functions::blue_cloud::blue_cloud_udfs();
        for udf in blue_cloud_udfs {
            session_ctx.register_udf(udf);
        }
        //FINISH INIT FUNCTIONS FROM beacon-functions module

        Ok(Self {
            session_ctx,
            schema_provider: beacon_schema_provider,
        })
    }

    fn init_ctx(mem_pool: Arc<FairSpillPool>) -> anyhow::Result<Arc<SessionContext>> {
        let mut config = SessionConfig::new()
            .with_batch_size(1024 * 1024 * 4)
            .with_coalesce_batches(true)
            .with_information_schema(true);

        config.options_mut().sql_parser.enable_ident_normalization = false;
        config
            .options_mut()
            .execution
            .listing_table_ignore_subdirectory = false;

        let disk_manager_conf = DiskManagerConfig::NewOs;

        let runtime_env = RuntimeEnvBuilder::new()
            .with_disk_manager(disk_manager_conf)
            .with_memory_pool(mem_pool)
            .build_arc()?;

        let mut session_state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime_env)
            .with_default_features()
            .build();

        session_state.register_file_format(Arc::new(NetCDFFileFormatFactory), true)?;
        session_state.register_file_format(Arc::new(SuperParquetFormatFactory), true)?;
        session_state.register_file_format(Arc::new(ArrowFormatFactory::new()), true)?;
        session_state.register_file_format(Arc::new(CsvFormatFactory::new()), true)?;

        let session_context = SessionContext::new_with_state(session_state);

        session_context.register_object_store(
            ObjectStoreUrl::parse("file://").unwrap().as_ref(),
            beacon_config::OBJECT_STORE_LOCAL_FS.clone(),
        );

        Ok(Arc::new(session_context))
    }

    pub fn session_ctx(&self) -> Arc<SessionContext> {
        self.session_ctx.clone()
    }

    pub fn list_functions(&self) -> Vec<FunctionDoc> {
        let mut functions: Vec<FunctionDoc> = self
            .session_ctx
            .state()
            .scalar_functions()
            .values()
            .map(|f| FunctionDoc::from_scalar(f))
            .collect();

        functions.sort_by(|a, b| a.function_name.cmp(&b.function_name));
        functions.dedup_by(|a, b| a.function_name == b.function_name);

        functions
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.schema_provider.table_names()
    }

    pub async fn list_table_schema(&self, table_name: String) -> Option<SchemaRef> {
        self.session_ctx
            .table(table_name)
            .await
            .map(|t| Arc::new(t.schema().as_arrow().to_owned()))
            .ok()
    }

    pub async fn list_default_table_schema(&self) -> SchemaRef {
        let table = self
            .session_ctx
            .table(beacon_config::CONFIG.default_table.as_str())
            .await
            .expect("Default table not found");
        Arc::new(table.schema().as_arrow().to_owned())
    }

    pub fn default_table(&self) -> String {
        beacon_config::CONFIG.default_table.clone()
    }

    pub async fn add_table(&self, table: Table) -> anyhow::Result<()> {
        Ok(self.schema_provider.add_table(table).await?)
    }

    pub async fn delete_table(&self, table_name: &str) -> anyhow::Result<()> {
        Ok(self.schema_provider.delete_table(table_name).await?)
    }

    pub async fn run_client_sql(
        &self,
        sql: &str,
        output: &OutputFormat,
    ) -> anyhow::Result<OutputResponse> {
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
    ) -> anyhow::Result<OutputResponse> {
        let df = DataFrame::new(self.session_ctx.state(), plan);
        output.output(self.session_ctx.clone(), df).await
    }

    pub async fn run_sql(
        &self,
        sql: &str,
        output: &OutputFormat,
    ) -> anyhow::Result<OutputResponse> {
        let df = self.session_ctx.sql(sql).await?;

        output.output(self.session_ctx.clone(), df).await
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

        tracing::debug!("Listing datasets from: {:?}", table_url);

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

    pub async fn total_datasets(&self) -> anyhow::Result<usize> {
        let discovery_path = format!("/{}/*", beacon_config::DATASETS_DIR_PREFIX.to_string());

        let state = self.session_ctx.state();

        let object_store = beacon_config::OBJECT_STORE_LOCAL_FS.clone();
        let table_url = ListingTableUrl::parse(&discovery_path).map_err(|e| {
            anyhow::anyhow!(
                "Error parsing discovery path: {:?} - {:?}",
                discovery_path,
                e
            )
        })?;

        let mut count = 0;
        let mut stream = table_url
            .list_all_files(&state, object_store.as_ref(), "")
            .await
            .map_err(|e| anyhow::anyhow!("Error listing datasets: {:?}", e))?;

        while let Some(item) = stream.next().await {
            match item {
                Ok(_) => {
                    count += 1;
                }
                Err(e) => {
                    event!(Level::ERROR, "Error listing datasets: {:?}", e);
                }
            }
        }

        Ok(count)
    }
}
