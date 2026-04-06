use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    datasource::ViewTable,
    execution::SendableRecordBatchStream,
    logical_expr::{DdlStatement, LogicalPlan},
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
            _ => {
                let df = DataFrame::new(state, plan);
                Ok(df.execute_stream().await?)
            }
        }
    }
}
