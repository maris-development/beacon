use std::sync::Arc;

use async_trait::async_trait;
use beacon_data_lake::DATASETS_OBJECT_STORE_URL;
use beacon_table::BeaconTable;
use datafusion::{
    catalog::TableProvider,
    datasource::{physical_plan, ViewTable},
    execution::{object_store::ObjectStoreUrl, SendableRecordBatchStream},
    logical_expr::{dml::InsertOp, CreateMemoryTable, DdlStatement, LogicalPlan},
    physical_plan::EmptyRecordBatchStream,
    prelude::{DataFrame, SQLOptions, SessionContext},
};

use crate::statement_handlers::{
    context::HandlerContext,
    payload::{StatementKind, StatementPayload},
    traits::StatementHandler,
};

pub(crate) struct DFStatementHandler;

impl DFStatementHandler {
    fn empty_ddl_stream(plan: &LogicalPlan) -> SendableRecordBatchStream {
        Box::pin(EmptyRecordBatchStream::new(
            plan.schema().as_arrow().clone().into(),
        ))
    }

    fn ensure_drop_table_exists(
        session_ctx: &SessionContext,
        drop_table_statement: &datafusion::logical_expr::DropTable,
    ) -> anyhow::Result<()> {
        if drop_table_statement.if_exists {
            return Ok(());
        }
        let exists = session_ctx.table_exist(drop_table_statement.name.clone())?;
        if exists {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Table '{}' does not exist",
                drop_table_statement.name
            ))
        }
    }

    async fn execute_create_external_table(
        session_ctx: &SessionContext,
        state: &datafusion::execution::context::SessionState,
        create_external: &datafusion::logical_expr::CreateExternalTable,
    ) -> anyhow::Result<()> {
        let table_factory = session_ctx
            .table_factory(&create_external.file_type)
            .ok_or(anyhow::anyhow!(
                "Unsupported file type '{}' for CREATE EXTERNAL TABLE",
                create_external.file_type
            ))?;

        let created_table = table_factory.create(state, create_external).await?;
        session_ctx.register_table(create_external.name.clone(), created_table)?;
        Ok(())
    }

    fn execute_create_view(
        session_ctx: &SessionContext,
        create_view: &datafusion::logical_expr::CreateView,
    ) -> anyhow::Result<()> {
        let table = ViewTable::new(
            create_view.input.as_ref().clone(),
            create_view.definition.clone(),
        );
        session_ctx.register_table(create_view.name.clone(), Arc::new(table))?;
        Ok(())
    }

    async fn execute_create_table(
        session_ctx: &SessionContext,
        table_cmd: &CreateMemoryTable,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let table_name = table_cmd.name.to_string();
        let schema = table_cmd.input.schema();
        let store_url = DATASETS_OBJECT_STORE_URL.clone();
        let store = session_ctx.runtime_env().object_store(&store_url).expect(
            "Datasets Store URL invalid. Check that DATASETS_OBJECT_STORE_URL is set correctly.",
        );

        let table = BeaconTable::new(
            store,
            store_url,
            table_name.clone(),
            object_store::path::Path::from("__beacon_tables__").child(table_name.clone()),
            Some(schema.as_arrow().clone().into()),
        )
        .await?;

        // Register the table in the session context so it can be queried
        session_ctx.register_table(&table_name, Arc::new(table))?;

        // Execute the input plan to populate the table with data
        Self::execute_insert_into_table(
            session_ctx,
            &table_name,
            &InsertOp::Append,
            table_cmd.input.clone(),
        )
        .await
    }

    async fn execute_insert_into_table(
        session_ctx: &SessionContext,
        table_name: &str,
        insert_op: &InsertOp,
        input: Arc<LogicalPlan>,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let provider = session_ctx.table_provider(table_name).await?;
        let beacon_table = provider
            .as_any()
            .downcast_ref::<BeaconTable>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Table '{}' is not a BeaconTable and cannot be inserted into",
                    table_name
                )
            })?;
        // Compile the logical plan to a physical plan and execute it to get the record batches to insert

        let state = session_ctx.state();
        let phys_plan = state.create_physical_plan(input.as_ref()).await?;
        let insert_plan = beacon_table
            .insert_into(&state, phys_plan, *insert_op)
            .await?;
        let task_ctx = session_ctx.task_ctx();
        let stream = datafusion::physical_plan::execute_stream(insert_plan.clone(), task_ctx)?;

        Ok(stream)
    }

    async fn execute_create_index(
        session_ctx: &SessionContext,
        index_cmd: &datafusion::logical_expr::CreateIndex,
    ) -> anyhow::Result<()> {
        let table_name = index_cmd.table.to_string();
        let provider = session_ctx.table_provider(index_cmd.table.clone()).await?;
        let beacon_table = provider
            .as_any()
            .downcast_ref::<BeaconTable>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Table '{}' is not a BeaconTable and does not support CREATE INDEX",
                    table_name
                )
            })?;

        // Extract column names from SortExpr, requiring plain column references
        let column_names: Vec<(String, bool)> = index_cmd
            .columns
            .iter()
            .map(|sort_expr| match &sort_expr.expr {
                datafusion::prelude::Expr::Column(col) => {
                    Ok((col.name().to_string(), sort_expr.asc))
                }
                other => Err(anyhow::anyhow!(
                    "CREATE INDEX only supports plain column references, found: {other}"
                )),
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        // Determine index type from USING clause (default: clustered)
        let using = index_cmd.using.as_deref().unwrap_or("clustered");
        let table_index = match using.to_lowercase().as_str() {
            "clustered" => {
                beacon_table::BeaconTableIndex::Clustered(beacon_table::ClusteredIndex {
                    columns: column_names
                        .into_iter()
                        .map(|(name, asc)| beacon_table::ClusteredIndexColumn {
                            name,
                            ascending: asc,
                        })
                        .collect(),
                })
            }
            "zorder" | "z_order" | "z-order" => {
                beacon_table::BeaconTableIndex::ZOrder(beacon_table::ZOrderIndex {
                    columns: column_names.into_iter().map(|(name, _)| name).collect(),
                })
            }
            other => {
                return Err(anyhow::anyhow!(
                    "Unsupported index type '{other}'. Supported types: 'clustered', 'zorder'"
                ));
            }
        };

        let state = session_ctx.state();
        let plan = beacon_table.create_index(&state, table_index).await?;
        let task_ctx = session_ctx.task_ctx();
        datafusion::physical_plan::collect(plan, task_ctx).await?;

        tracing::info!("Created {using} index on table '{table_name}'");
        Ok(())
    }
}

#[async_trait]
impl StatementHandler for DFStatementHandler {
    fn kind(&self) -> StatementKind {
        StatementKind::DFStatement
    }

    async fn execute(
        &self,
        payload: StatementPayload,
        context: &HandlerContext,
        sql_options: &SQLOptions,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let statement = payload.into_df_statement()?;
        let session_ctx = context.session_ctx();
        let state = session_ctx.state();
        let plan = state.statement_to_plan(statement).await?;

        sql_options.verify_plan(&plan)?;

        match &plan {
            LogicalPlan::Ddl(DdlStatement::DropTable(drop_table_statement)) => {
                Self::ensure_drop_table_exists(&session_ctx, drop_table_statement)?;
                session_ctx.deregister_table(drop_table_statement.name.clone())?;
                Ok(Self::empty_ddl_stream(&plan))
            }
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(create_external)) => {
                Self::execute_create_external_table(&session_ctx, &state, create_external).await?;
                Ok(Self::empty_ddl_stream(&plan))
            }
            LogicalPlan::Ddl(DdlStatement::CreateView(create_view)) => {
                Self::execute_create_view(&session_ctx, create_view)?;
                Ok(Self::empty_ddl_stream(&plan))
            }
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(table)) => {
                let stream = Self::execute_create_table(&session_ctx, table).await?;
                Ok(stream)
            }
            LogicalPlan::Ddl(DdlStatement::CreateIndex(index)) => {
                Self::execute_create_index(&session_ctx, index).await?;
                Ok(Self::empty_ddl_stream(&plan))
            }
            LogicalPlan::Dml(dml_statement) => match dml_statement.op {
                datafusion::logical_expr::WriteOp::Insert(insert_op) => {
                    tracing::debug!(
                        "Executing INSERT INTO for table '{}'",
                        dml_statement.table_name.to_string()
                    );
                    let stream = Self::execute_insert_into_table(
                        &session_ctx,
                        &dml_statement.table_name.to_string(),
                        &insert_op,
                        dml_statement.input.clone(),
                    )
                    .await?;
                    Ok(stream)
                }
                datafusion::logical_expr::WriteOp::Ctas => {
                    tracing::debug!(
                        "Executing CTAS for table '{}'",
                        dml_statement.table_name.to_string()
                    );
                    let df = DataFrame::new(state, plan);
                    Ok(df.execute_stream().await?)
                }
                _ => {
                    let df = DataFrame::new(state, plan);
                    Ok(df.execute_stream().await?)
                }
            },
            _ => {
                let df = DataFrame::new(state, plan);
                Ok(df.execute_stream().await?)
            }
        }
    }
}
