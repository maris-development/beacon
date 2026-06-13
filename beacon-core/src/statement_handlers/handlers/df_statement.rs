use std::sync::Arc;

use async_trait::async_trait;
use beacon_data_lake::DATASETS_OBJECT_STORE_URL;
use beacon_datafusion_ext::table_ext::{MaterializedView, TableDefinition};
use datafusion::{
    catalog::{TableProvider, TableProviderFactory},
    datasource::{physical_plan, ViewTable},
    execution::SendableRecordBatchStream,
    logical_expr::{dml::InsertOp, CreateMemoryTable, DdlStatement, LogicalPlan},
    physical_plan::EmptyRecordBatchStream,
    prelude::{DataFrame, SQLOptions, SessionContext},
};

use datafusion::sql::sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ColumnDef as SqlColumnDef, Ident as SqlIdent,
    ObjectName, Statement as SqlAstStatement,
};
use datafusion::sql::TableReference;

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
        handler_context: &HandlerContext,
        session_ctx: &SessionContext,
        state: &datafusion::execution::context::SessionState,
        create_external: &datafusion::logical_expr::CreateExternalTable,
    ) -> anyhow::Result<()> {
        let table_factory = handler_context.listing_table_factory();
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

    /// Handle `CREATE TABLE` (incl. `CREATE TABLE AS SELECT`) by creating an
    /// Iceberg-backed managed table and populating it from the input plan.
    ///
    /// DataFusion lowers both `CREATE TABLE t (cols...)` and
    /// `CREATE TABLE t AS SELECT ...` to [`CreateMemoryTable`]; the former carries
    /// an empty input (zero rows), the latter the query plan. Creating the table
    /// from the input's Arrow schema and then inserting the input handles both.
    async fn execute_create_table(
        session_ctx: &SessionContext,
        table_cmd: &CreateMemoryTable,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        // The bare table name (drop any catalog/schema qualification) is the
        // Iceberg table name; registration uses the original reference.
        let table_ref = table_cmd.name.clone();
        let table_name = table_ref.table().to_string();

        if session_ctx.table_exist(table_ref.clone())? {
            if table_cmd.if_not_exists {
                return Ok(Self::empty_ddl_stream(&LogicalPlan::Ddl(
                    DdlStatement::CreateMemoryTable(table_cmd.clone()),
                )));
            }
            return Err(anyhow::anyhow!("Table '{}' already exists", table_name));
        }

        let arrow_schema = table_cmd.input.schema().as_arrow().clone();
        let catalog = beacon_iceberg::get_catalog()?;
        let namespace = beacon_iceberg::beacon_namespace();

        let table =
            beacon_iceberg::create_iceberg_table(&catalog, &namespace, &table_name, &arrow_schema)
                .await?;

        // Register so the table is queryable and its `table.json` pointer is
        // persisted by the TableManager.
        session_ctx.register_table(table_ref, Arc::new(table))?;

        // A bare `CREATE TABLE t (cols...)` lowers to an empty input relation:
        // there is nothing to insert, so return an empty result. Only CTAS
        // carries a real query plan to populate the table from.
        if matches!(table_cmd.input.as_ref(), LogicalPlan::EmptyRelation(_)) {
            return Ok(Box::pin(EmptyRecordBatchStream::new(Arc::new(
                arrow::datatypes::Schema::empty(),
            ))));
        }

        Self::execute_insert_into_table(
            session_ctx,
            &table_name,
            &InsertOp::Append,
            table_cmd.input.clone(),
        )
        .await
    }

    /// Insert the rows produced by `input` into an existing managed table.
    ///
    /// Works for any [`TableProvider`] that supports `insert_into` (Iceberg
    /// tables do); no provider downcast is required.
    async fn execute_insert_into_table(
        session_ctx: &SessionContext,
        table_name: &str,
        insert_op: &InsertOp,
        input: Arc<LogicalPlan>,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let provider = session_ctx.table_provider(table_name).await?;

        let state = session_ctx.state();
        let phys_plan = state.create_physical_plan(input.as_ref()).await?;
        let insert_plan = provider.insert_into(&state, phys_plan, *insert_op).await?;
        let task_ctx = session_ctx.task_ctx();
        let stream = datafusion::physical_plan::execute_stream(insert_plan.clone(), task_ctx)?;

        Ok(stream)
    }

    /// Replace **all** rows of an Iceberg table with the result of `new_contents`
    /// (copy-on-write). Shared by `DELETE` (surviving rows) and `UPDATE` (updated
    /// rows). Requires the target be an Iceberg table; rejects anything else.
    async fn replace_table_with_plan(
        session_ctx: &SessionContext,
        table_ref: datafusion::sql::TableReference,
        new_contents: LogicalPlan,
    ) -> anyhow::Result<()> {
        let provider = session_ctx.table_provider(table_ref.clone()).await?;
        let definition = provider
            .as_any()
            .downcast_ref::<beacon_iceberg::IcebergTable>()
            .map(|table| table.definition().clone())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Row mutations are only supported on Iceberg tables, but '{}' is not one",
                    table_ref.table()
                )
            })?;

        let state = session_ctx.state();
        let phys_plan = state.create_physical_plan(&new_contents).await?;
        let task_ctx = session_ctx.task_ctx();
        let stream = datafusion::physical_plan::execute_stream(phys_plan, task_ctx.clone())?;

        let catalog = beacon_iceberg::get_catalog()?;
        beacon_iceberg::replace_table_contents(
            &catalog,
            &definition.namespace,
            &definition.name,
            stream,
            &task_ctx,
        )
        .await?;

        // The registered provider caches its snapshot and will not observe the
        // catalog-level replace; rebuild it so subsequent scans see the new data.
        let fresh = definition
            .build_provider(Arc::new(session_ctx.clone()), &DATASETS_OBJECT_STORE_URL)
            .await?;
        session_ctx.register_table(table_ref, fresh)?;

        Ok(())
    }

    /// Execute `DELETE FROM t [WHERE p]` against an Iceberg table by copy-on-write:
    /// recompute the surviving rows (`NOT p`, or none when there is no `WHERE`)
    /// and atomically replace the table's data files.
    async fn execute_delete(
        session_ctx: &SessionContext,
        dml: &datafusion::logical_expr::DmlStatement,
    ) -> anyhow::Result<()> {
        // Build the "keep" plan by inverting the delete predicate, reusing the
        // Dml input's own TableScan child so column qualifiers line up.
        let keep_plan = match dml.input.as_ref() {
            LogicalPlan::Filter(filter) => {
                let keep = datafusion::logical_expr::Filter::try_new(
                    datafusion::logical_expr::not(filter.predicate.clone()),
                    filter.input.clone(),
                )?;
                LogicalPlan::Filter(keep)
            }
            scan @ LogicalPlan::TableScan(_) => {
                // No WHERE clause: delete every row -> keep nothing.
                let keep = datafusion::logical_expr::Filter::try_new(
                    datafusion::prelude::lit(false),
                    Arc::new(scan.clone()),
                )?;
                LogicalPlan::Filter(keep)
            }
            other => {
                return Err(anyhow::anyhow!(
                    "Unsupported DELETE plan shape: {}",
                    other.display()
                ));
            }
        };

        Self::replace_table_with_plan(session_ctx, dml.table_name.clone(), keep_plan).await
    }

    /// Execute `UPDATE t SET col = expr [, …] [WHERE p]` against an Iceberg table
    /// by copy-on-write. DataFusion lowers this to a projection (assignment or
    /// passthrough per column, aliased to the column name) over the *matching*
    /// rows; the full post-update table is rebuilt as a single CASE projection
    /// over the unfiltered scan: `CASE WHEN p THEN <new> ELSE <old> END`.
    async fn execute_update(
        session_ctx: &SessionContext,
        dml: &datafusion::logical_expr::DmlStatement,
    ) -> anyhow::Result<()> {
        let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
            return Err(anyhow::anyhow!(
                "Unsupported UPDATE plan shape: {}",
                dml.input.display()
            ));
        };

        let new_contents = match projection.input.as_ref() {
            LogicalPlan::Filter(filter) => {
                let scan = filter.input.as_ref();
                // Positional column mapping below assumes the scan's columns are
                // exactly the table's columns (no FROM/join).
                if projection.expr.len() != scan.schema().fields().len() {
                    return Err(anyhow::anyhow!(
                        "UPDATE ... FROM / joins are not supported"
                    ));
                }
                let scan_cols = scan.schema().columns();
                let predicate = &filter.predicate;

                let case_exprs = projection
                    .expr
                    .iter()
                    .enumerate()
                    .map(|(i, expr)| {
                        let name = projection.schema.field(i).name().clone();
                        let new_value = expr.clone().unalias();
                        let old_value =
                            datafusion::prelude::Expr::Column(scan_cols[i].clone());
                        Ok(datafusion::logical_expr::when(predicate.clone(), new_value)
                            .otherwise(old_value)?
                            .alias(name))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;

                datafusion::logical_expr::LogicalPlanBuilder::from(scan.clone())
                    .project(case_exprs)?
                    .build()?
            }
            // No WHERE clause: the projection already covers every row.
            _ => dml.input.as_ref().clone(),
        };

        Self::replace_table_with_plan(session_ctx, dml.table_name.clone(), new_contents).await
    }

    /// Execute `ALTER TABLE t …` against an Iceberg table by mapping the parsed
    /// operations to schema changes and rebuilding the table under the new schema.
    async fn execute_alter_table(
        session_ctx: &SessionContext,
        name: &ObjectName,
        operations: &[AlterTableOperation],
    ) -> anyhow::Result<()> {
        let table_ref = TableReference::parse_str(&name.to_string());

        // ALTER is only supported on Iceberg-backed tables.
        let provider = session_ctx.table_provider(table_ref.clone()).await?;
        let definition = provider
            .as_any()
            .downcast_ref::<beacon_iceberg::IcebergTable>()
            .map(|table| table.definition().clone())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "ALTER TABLE is only supported on Iceberg tables, but '{}' is not one",
                    table_ref.table()
                )
            })?;

        let mut changes = Vec::new();
        for operation in operations {
            match operation {
                AlterTableOperation::AddColumn { column_def, .. } => {
                    changes.push(beacon_iceberg::SchemaChange::AddColumn {
                        name: column_def.name.value.clone(),
                        data_type: Self::sql_column_type_to_arrow(column_def)?,
                    });
                }
                AlterTableOperation::DropColumn { column_names, .. } => {
                    for column_name in column_names {
                        changes.push(beacon_iceberg::SchemaChange::DropColumn {
                            name: column_name.value.clone(),
                        });
                    }
                }
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    changes.push(beacon_iceberg::SchemaChange::RenameColumn {
                        from: old_column_name.value.clone(),
                        to: new_column_name.value.clone(),
                    });
                }
                AlterTableOperation::AlterColumn { column_name, op } => match op {
                    AlterColumnOperation::SetDataType { data_type, .. } => {
                        let column_def = SqlColumnDef {
                            name: SqlIdent::new("c"),
                            data_type: data_type.clone(),
                            options: Vec::new(),
                        };
                        changes.push(beacon_iceberg::SchemaChange::AlterColumnType {
                            name: column_name.value.clone(),
                            data_type: Self::sql_column_type_to_arrow(&column_def)?,
                        });
                    }
                    other => {
                        return Err(anyhow::anyhow!(
                            "Unsupported ALTER COLUMN operation: {other}"
                        ));
                    }
                },
                other => {
                    return Err(anyhow::anyhow!("Unsupported ALTER TABLE operation: {other}"));
                }
            }
        }

        let catalog = beacon_iceberg::get_catalog()?;
        let store = beacon_iceberg::get_warehouse_store()?;
        beacon_iceberg::alter_table_schema(
            &catalog,
            &store,
            &definition.namespace,
            &definition.name,
            &changes,
        )
        .await?;

        // Rebuild the registered provider so subsequent queries see the new schema.
        let fresh = definition
            .build_provider(Arc::new(session_ctx.clone()), &DATASETS_OBJECT_STORE_URL)
            .await?;
        session_ctx.register_table(table_ref, fresh)?;

        Ok(())
    }

    /// Convert a SQL column definition's type to an Arrow type, reusing
    /// DataFusion's full type support (no catalog access is needed).
    fn sql_column_type_to_arrow(
        column_def: &SqlColumnDef,
    ) -> anyhow::Result<arrow::datatypes::DataType> {
        let provider = AlterTypeContextProvider::default();
        let planner = datafusion::sql::planner::SqlToRel::new(&provider);
        let schema = planner
            .build_schema(vec![column_def.clone()])
            .map_err(|error| anyhow::anyhow!("Unsupported column type: {error}"))?;
        Ok(schema.field(0).data_type().clone())
    }
}

/// Minimal [`ContextProvider`] used only to convert SQL column types to Arrow
/// types (via `SqlToRel::build_schema`); type conversion never touches the
/// catalog, functions, or variables.
#[derive(Default)]
struct AlterTypeContextProvider {
    options: datafusion::config::ConfigOptions,
}

impl datafusion::sql::planner::ContextProvider for AlterTypeContextProvider {
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> datafusion::error::Result<Arc<dyn datafusion::logical_expr::TableSource>> {
        datafusion::common::plan_err!("ALTER type conversion does not resolve table '{name}'")
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<datafusion::logical_expr::ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(
        &self,
        _name: &str,
    ) -> Option<Arc<datafusion::logical_expr::AggregateUDF>> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<datafusion::logical_expr::WindowUDF>> {
        None
    }

    fn get_variable_type(&self, _variable: &[String]) -> Option<arrow::datatypes::DataType> {
        None
    }

    fn options(&self) -> &datafusion::config::ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
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

        // DataFusion has no `ALTER TABLE` planning, so intercept it here and drive
        // Iceberg schema evolution directly.
        if let datafusion::sql::parser::Statement::Statement(sql_stmt) = &statement {
            if let SqlAstStatement::AlterTable(alter) = sql_stmt.as_ref() {
                Self::execute_alter_table(&session_ctx, &alter.name, &alter.operations).await?;
                return Ok(Box::pin(EmptyRecordBatchStream::new(Arc::new(
                    arrow::datatypes::Schema::empty(),
                ))));
            }
        }

        let plan = state.statement_to_plan(statement).await?;

        sql_options.verify_plan(&plan)?;

        match &plan {
            LogicalPlan::Ddl(DdlStatement::DropTable(drop_table_statement)) => {
                Self::ensure_drop_table_exists(&session_ctx, drop_table_statement)?;

                // Inspect the provider before deregistering so we can reclaim its
                // backing storage afterwards: materialized views persist Parquet
                // under a data prefix, Iceberg tables own metadata+data in the
                // Iceberg warehouse.
                let provider = session_ctx
                    .table_provider(drop_table_statement.name.clone())
                    .await
                    .ok();
                let materialized_prefix = provider.as_ref().and_then(|provider| {
                    provider
                        .as_any()
                        .downcast_ref::<MaterializedView>()
                        .map(|mv| mv.base_storage_prefix())
                });
                let iceberg_definition = provider.as_ref().and_then(|provider| {
                    provider
                        .as_any()
                        .downcast_ref::<beacon_iceberg::IcebergTable>()
                        .map(|table| table.definition().clone())
                });

                session_ctx.deregister_table(drop_table_statement.name.clone())?;

                if let Some(prefix) = materialized_prefix {
                    crate::statement_plan::materialized_view::delete_datasets_prefix(
                        &session_ctx,
                        &prefix,
                    )
                    .await;
                }

                if let Some(definition) = iceberg_definition {
                    let store = beacon_iceberg::get_warehouse_store()?;
                    beacon_iceberg::drop_iceberg_table(
                        &store,
                        &definition.namespace,
                        &definition.name,
                    )
                    .await?;
                }

                Ok(Self::empty_ddl_stream(&plan))
            }
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(create_external)) => {
                Self::execute_create_external_table(context, &session_ctx, &state, create_external)
                    .await?;
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
            LogicalPlan::Dml(dml_statement)
                if matches!(
                    dml_statement.op,
                    datafusion::logical_expr::WriteOp::Insert(_)
                ) =>
            {
                let datafusion::logical_expr::WriteOp::Insert(insert_op) = dml_statement.op else {
                    unreachable!("guarded by matches! above")
                };
                tracing::debug!(
                    "Executing INSERT INTO for table '{}'",
                    dml_statement.table_name.to_string()
                );
                let stream = Self::execute_insert_into_table(
                    &session_ctx,
                    &dml_statement.table_name.table().to_string(),
                    &insert_op,
                    dml_statement.input.clone(),
                )
                .await?;
                Ok(stream)
            }
            LogicalPlan::Dml(dml_statement)
                if matches!(dml_statement.op, datafusion::logical_expr::WriteOp::Delete) =>
            {
                tracing::debug!(
                    "Executing DELETE for table '{}'",
                    dml_statement.table_name.to_string()
                );
                Self::execute_delete(&session_ctx, dml_statement).await?;
                Ok(Self::empty_ddl_stream(&plan))
            }
            LogicalPlan::Dml(dml_statement)
                if matches!(dml_statement.op, datafusion::logical_expr::WriteOp::Update) =>
            {
                tracing::debug!(
                    "Executing UPDATE for table '{}'",
                    dml_statement.table_name.to_string()
                );
                Self::execute_update(&session_ctx, dml_statement).await?;
                Ok(Self::empty_ddl_stream(&plan))
            }
            LogicalPlan::Copy(copy) => {
                let mut copy_cleaned = copy.clone();
                copy_cleaned.output_url =
                    format!("{}{}", *DATASETS_OBJECT_STORE_URL, copy.output_url);

                let df = DataFrame::new(state, LogicalPlan::Copy(copy_cleaned));
                Ok(df.execute_stream().await?)
            }
            _ => {
                let df = DataFrame::new(state, plan);
                Ok(df.execute_stream().await?)
            }
        }
    }
}
