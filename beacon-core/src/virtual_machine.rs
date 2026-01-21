use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::AsArray,
    datatypes::{SchemaRef, UInt64Type},
};
use beacon_data_lake::{table::Table, DataLake};
use beacon_formats::{Dataset, FileFormatFactoryExt};
use beacon_functions::{file_formats::BeaconTableFunctionImpl, function_doc::FunctionDoc};
use beacon_planner::{metrics::ConsolidatedMetrics, plan::BeaconQueryPlan};
use datafusion::{
    catalog::{SchemaProvider, TableFunctionImpl},
    datasource::listing::ListingTableUrl,
    execution::{
        disk_manager::DiskManagerConfig, memory_pool::FairSpillPool,
        runtime_env::RuntimeEnvBuilder, SessionStateBuilder,
    },
    prelude::{SessionConfig, SessionContext},
};
use futures::StreamExt;
use parking_lot::Mutex;

use crate::query_result::{ArrowOutputStream, QueryOutput, QueryResult};

pub struct VirtualMachine {
    table_functions: Vec<Arc<dyn BeaconTableFunctionImpl>>,
    session_ctx: Arc<SessionContext>,
    data_lake: Arc<DataLake>,
}

impl VirtualMachine {
    pub async fn new() -> anyhow::Result<Self> {
        let memory_pool = Arc::new(FairSpillPool::new(
            beacon_config::CONFIG.vm_memory_size * 1024 * 1024,
        ));

        let session_ctx = Self::init_ctx(memory_pool.clone())?;
        let data_lake = Arc::new(DataLake::new(session_ctx.clone()).await);

        session_ctx
            .catalog("datafusion")
            .unwrap()
            .register_schema("public", data_lake.clone())?;

        // INIT Functions from geodatafusion
        geodatafusion::register(&session_ctx);

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

        // Register table functions
        let table_functions = beacon_functions::file_formats::register_table_functions(
            tokio::runtime::Handle::current(),
            session_ctx.clone(),
            data_lake.data_object_store_url(),
            beacon_object_storage::get_datasets_object_store().await,
        );

        for tf in table_functions.iter() {
            session_ctx.register_udtf(
                tf.name().as_str(),
                Arc::clone(tf) as Arc<dyn TableFunctionImpl>,
            );
        }

        //FINISH INIT FUNCTIONS FROM beacon-functions module
        Ok(Self {
            table_functions,
            session_ctx,
            data_lake,
        })
    }

    fn init_ctx(mem_pool: Arc<FairSpillPool>) -> anyhow::Result<Arc<SessionContext>> {
        let mut config = SessionConfig::new()
            .with_coalesce_batches(true)
            .with_information_schema(true)
            .with_collect_statistics(true);

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

        let session_state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime_env)
            .with_default_features()
            .build();

        let session_context = Arc::new(SessionContext::new_with_state(session_state));

        Ok(session_context)
    }

    pub fn session_ctx(&self) -> Arc<SessionContext> {
        self.session_ctx.clone()
    }

    pub fn data_lake(&self) -> Arc<DataLake> {
        self.data_lake.clone()
    }

    pub fn list_functions(&self) -> Vec<FunctionDoc> {
        let mut functions: Vec<FunctionDoc> = self
            .session_ctx
            .state()
            .scalar_functions()
            .values()
            .flat_map(|f| FunctionDoc::from_scalar(f))
            .collect();

        functions.sort_by(|a, b| a.function_name.cmp(&b.function_name));
        functions.dedup_by(|a, b| a.function_name == b.function_name);

        functions
    }

    pub fn list_table_functions(&self) -> Vec<FunctionDoc> {
        let mut table_functions: Vec<FunctionDoc> = self
            .table_functions
            .iter()
            .map(|tf| FunctionDoc::from_beacon_table_function(tf.as_ref()))
            .collect();

        table_functions.sort_by(|a, b| a.function_name.cmp(&b.function_name));
        table_functions.dedup_by(|a, b| a.function_name == b.function_name);

        table_functions
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.data_lake.table_names()
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
        Ok(self.data_lake.create_table(table).await?)
    }

    pub async fn delete_table(&self, table_name: &str) -> anyhow::Result<()> {
        Ok(self.data_lake.remove_table(table_name).await?)
    }

    #[tracing::instrument(skip(self, beacon_plan, query_metrics))]
    pub async fn run_plan(
        &self,
        beacon_plan: BeaconQueryPlan,
        query_metrics: Arc<Mutex<HashMap<uuid::Uuid, ConsolidatedMetrics>>>,
    ) -> anyhow::Result<QueryResult> {
        let task_ctx = self.session_ctx().task_ctx();

        match beacon_plan.output_file {
            Some(output_file) => {
                // This means the plan contains an output file to write to. We should run the entire stream to completion. Afterwards we can return the output file info.
                let mut stream = datafusion::physical_plan::execute_stream(
                    beacon_plan.physical_plan.clone(),
                    task_ctx,
                )?;

                let mut total_rows: u64 = 0;
                while let Some(maybe_batch) = stream.next().await {
                    let batch = maybe_batch?;
                    let num_rows_array = batch.column(0).as_primitive::<UInt64Type>();
                    if num_rows_array.len() > 0 {
                        let num_rows = num_rows_array.value(0);
                        beacon_plan.metrics_tracker.add_output_rows(num_rows);
                        total_rows += num_rows;
                    }
                }

                tracing::info!("Query Returned {} rows", total_rows);
                tracing::info!("Query result size in bytes: {:?}", output_file.size());

                beacon_plan
                    .metrics_tracker
                    .add_output_bytes(output_file.size()?);

                let consolidated_metrics = beacon_plan.metrics_tracker.get_consolidated_metrics();
                query_metrics
                    .lock()
                    .insert(beacon_plan.query_id, consolidated_metrics);

                Ok(QueryResult {
                    query_output: QueryOutput::File(output_file),
                    query_id: beacon_plan.query_id,
                })
            }
            None => {
                // This means the plan does not contain an output file. We can stream results back to the user directly.
                let stream = datafusion::physical_plan::execute_stream(
                    beacon_plan.physical_plan.clone(),
                    task_ctx,
                )?;

                let output_stream = ArrowOutputStream {
                    stream,
                    metrics: beacon_plan.metrics_tracker.clone(),
                    all_consolidated_metrics: query_metrics.clone(),
                };

                Ok(QueryResult {
                    query_output: QueryOutput::Stream(output_stream),
                    query_id: beacon_plan.query_id,
                })
            }
        }
    }

    pub async fn list_dataset_schema(&self, dataset: String) -> anyhow::Result<SchemaRef> {
        Ok(self.data_lake.list_dataset_schema(&dataset).await?)
    }

    pub async fn list_datasets(
        &self,
        pattern: Option<String>,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<Dataset>> {
        Ok(self.data_lake.list_datasets(offset, limit, pattern).await?)
    }

    pub async fn total_datasets(&self) -> anyhow::Result<usize> {
        self.list_datasets(None, None, None).await.map(|v| v.len())
    }

    pub(crate) async fn list_table_config(&self, table_name: String) -> Option<Table> {
        self.data_lake.list_table(&table_name)
    }

    pub(crate) async fn apply_table_operation(
        &self,
        table_name: &str,
        op: serde_json::Value,
    ) -> Result<(), anyhow::Error> {
        Ok(self.data_lake.apply_operation(table_name, op).await?)
    }
}
