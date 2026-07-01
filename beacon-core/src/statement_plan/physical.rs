//! Physical execution-plan nodes for beacon's custom statements.
//!
//! Each node performs its side effect (running a query, writing Parquet,
//! mutating the catalog) when executed and emits no rows. Because
//! [`ExecutionPlan::execute`] is synchronous but the work is async, the side
//! effect is run inside a one-shot stream (the same pattern DataFusion's
//! `DataSinkExec` uses), with the [`SessionContext`] recovered from the planner's
//! [`SessionCell`] at execution time.

use std::{any::Any, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::{
    error::{DataFusionError, Result},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{dml::InsertOp, CreateExternalTable, LogicalPlan},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    },
    prelude::SessionContext,
    sql::TableReference,
};
use futures::{StreamExt, TryStreamExt};

use super::{
    actions, crawler,
    logical::{
        count_arrow_schema, show_crawlers_arrow_schema, show_indexes_arrow_schema, AlterTableSpec,
        Mutation,
    },
    materialized_view, SessionCell,
};
use crate::extensions::{
    drop_table_extension, set_table_extension, show_extensions_arrow_schema,
    show_table_extensions_batch,
};

/// `PlanProperties` for a single-partition node producing `schema`.
fn plan_properties(schema: SchemaRef) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}

/// `PlanProperties` for a single-partition node with an empty output schema.
fn side_effect_properties() -> PlanProperties {
    plan_properties(Arc::new(Schema::empty()))
}

/// Map a beacon (`anyhow`) error into a DataFusion execution error.
fn to_df_err(error: anyhow::Error) -> DataFusionError {
    DataFusionError::External(error.into())
}

/// An empty (zero-row) stream with the given schema.
fn empty_with_schema(schema: SchemaRef) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::empty(),
    ))
}

/// Run an async action that yields a row stream (e.g. an INSERT's count stream)
/// and forward its rows, surfacing the action's error through the stream.
fn forward_stream<F>(schema: SchemaRef, future: F) -> SendableRecordBatchStream
where
    F: std::future::Future<Output = Result<SendableRecordBatchStream>> + Send + 'static,
{
    let stream = futures::stream::once(future).try_flatten();
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

/// Like [`forward_stream`], but the action may produce no stream (e.g. a bare
/// `CREATE TABLE`), in which case an empty stream with `schema` is returned.
fn optional_forward_stream<F>(schema: SchemaRef, future: F) -> SendableRecordBatchStream
where
    F: std::future::Future<Output = Result<Option<SendableRecordBatchStream>>> + Send + 'static,
{
    let inner_schema = schema.clone();
    let stream = futures::stream::once(async move {
        future
            .await
            .map(|maybe| maybe.unwrap_or_else(|| empty_with_schema(inner_schema)))
    })
    .try_flatten();
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

/// Recover the [`SessionContext`] the side effect needs (`register_table`,
/// catalog access) from the planner's late-filled weak handle.
fn upgrade_session(cell: &SessionCell) -> Result<Arc<SessionContext>> {
    cell.get().and_then(|weak| weak.upgrade()).ok_or_else(|| {
        DataFusionError::Execution(
            "Beacon session context is unavailable; cannot execute statement".to_string(),
        )
    })
}

/// Run an async side effect that yields no rows, surfacing any error through the
/// stream. The resulting stream has an empty schema and a single (error-or-empty)
/// item.
fn side_effect_stream<F>(future: F) -> SendableRecordBatchStream
where
    F: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let schema: SchemaRef = Arc::new(Schema::empty());
    let stream = futures::stream::once(future).filter_map(|res| async move {
        match res {
            Ok(()) => None,
            Err(error) => Some(Err::<arrow::record_batch::RecordBatch, _>(error)),
        }
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

/// Physical node for `CREATE MATERIALIZED VIEW`.
#[derive(Debug)]
pub(crate) struct CreateMaterializedViewExec {
    view_name: String,
    query_sql: String,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl CreateMaterializedViewExec {
    pub(crate) fn new(view_name: String, query_sql: String, session: SessionCell) -> Self {
        Self {
            view_name,
            query_sql,
            session,
            cache: Arc::new(side_effect_properties()),
        }
    }
}

impl DisplayAs for CreateMaterializedViewExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CreateMaterializedViewExec: name={}", self.view_name)
            }
            DisplayFormatType::TreeRender => write!(f, "CreateMaterializedViewExec"),
        }
    }
}

impl ExecutionPlan for CreateMaterializedViewExec {
    fn name(&self) -> &str {
        "CreateMaterializedViewExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let session = upgrade_session(&self.session)?;
        let view_name = self.view_name.clone();
        let query_sql = self.query_sql.clone();
        Ok(side_effect_stream(async move {
            materialized_view::create_materialized_view(&session, &view_name, &query_sql)
                .await
                .map_err(|error| DataFusionError::External(error.into()))
        }))
    }
}

/// Physical node for `REFRESH [TABLE] <name>`.
#[derive(Debug)]
pub(crate) struct RefreshExec {
    name: String,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl RefreshExec {
    pub(crate) fn new(name: String, session: SessionCell) -> Self {
        Self {
            name,
            session,
            cache: Arc::new(side_effect_properties()),
        }
    }
}

impl DisplayAs for RefreshExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "RefreshExec: name={}", self.name)
            }
            DisplayFormatType::TreeRender => write!(f, "RefreshExec"),
        }
    }
}

impl ExecutionPlan for RefreshExec {
    fn name(&self) -> &str {
        "RefreshExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let session = upgrade_session(&self.session)?;
        let name = self.name.clone();
        Ok(side_effect_stream(async move {
            materialized_view::refresh_table(&session, &name)
                .await
                .map_err(|error| DataFusionError::External(error.into()))
        }))
    }
}

/// Macro for the boilerplate of a childless, empty-output side-effect exec.
macro_rules! side_effect_exec {
    ($exec:ident, $display:literal, $body:expr) => {
        impl DisplayAs for $exec {
            fn fmt_as(
                &self,
                t: DisplayFormatType,
                f: &mut std::fmt::Formatter<'_>,
            ) -> std::fmt::Result {
                match t {
                    DisplayFormatType::Default | DisplayFormatType::Verbose => self.fmt_label(f),
                    DisplayFormatType::TreeRender => write!(f, $display),
                }
            }
        }

        impl ExecutionPlan for $exec {
            fn name(&self) -> &str {
                $display
            }
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn properties(&self) -> &Arc<PlanProperties> {
                &self.cache
            }
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
                vec![]
            }
            fn with_new_children(
                self: Arc<Self>,
                _children: Vec<Arc<dyn ExecutionPlan>>,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                Ok(self)
            }
            fn execute(
                &self,
                _partition: usize,
                _context: Arc<TaskContext>,
            ) -> Result<SendableRecordBatchStream> {
                #[allow(clippy::redundant_closure_call)]
                $body(self)
            }
        }
    };
}

/// Physical node for the auth-management statements (CREATE/DROP USER/ROLE, GRANT/DENY/REVOKE).
#[derive(Debug)]
pub(crate) struct AuthExec {
    statement: crate::parser::statement::AuthStatement,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl AuthExec {
    pub(crate) fn new(
        statement: crate::parser::statement::AuthStatement,
        session: SessionCell,
    ) -> Self {
        Self {
            statement,
            session,
            cache: Arc::new(side_effect_properties()),
        }
    }
    fn fmt_label(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AuthExec: {}", self.statement)
    }
}

side_effect_exec!(AuthExec, "AuthExec", |exec: &AuthExec| {
    let session = upgrade_session(&exec.session)?;
    let statement = exec.statement.clone();
    Ok(side_effect_stream(async move {
        super::auth::apply_auth_statement(&session, &statement).map_err(to_df_err)
    }))
});

/// Physical node for `DROP TABLE`.
#[derive(Debug)]
pub(crate) struct DropTableExec {
    name: TableReference,
    if_exists: bool,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl DropTableExec {
    pub(crate) fn new(name: TableReference, if_exists: bool, session: SessionCell) -> Self {
        Self {
            name,
            if_exists,
            session,
            cache: Arc::new(side_effect_properties()),
        }
    }
    fn fmt_label(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DropTableExec: name={}", self.name)
    }
}

side_effect_exec!(DropTableExec, "DropTableExec", |exec: &DropTableExec| {
    let session = upgrade_session(&exec.session)?;
    let name = exec.name.clone();
    let if_exists = exec.if_exists;
    Ok(side_effect_stream(async move {
        actions::drop_table(&session, &name, if_exists)
            .await
            .map_err(to_df_err)
    }))
});

/// Physical node for `CREATE EXTERNAL TABLE`.
#[derive(Debug)]
pub(crate) struct CreateExternalTableExec {
    cmd: CreateExternalTable,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl CreateExternalTableExec {
    pub(crate) fn new(cmd: CreateExternalTable, session: SessionCell) -> Self {
        Self {
            cmd,
            session,
            cache: Arc::new(side_effect_properties()),
        }
    }
    fn fmt_label(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CreateExternalTableExec: name={}", self.cmd.name)
    }
}

side_effect_exec!(
    CreateExternalTableExec,
    "CreateExternalTableExec",
    |exec: &CreateExternalTableExec| {
        let session = upgrade_session(&exec.session)?;
        let cmd = exec.cmd.clone();
        Ok(side_effect_stream(async move {
            actions::create_external_table(&session, &cmd)
                .await
                .map_err(to_df_err)
        }))
    }
);

/// Physical node for `CREATE VIEW`.
#[derive(Debug)]
pub(crate) struct CreateViewExec {
    name: TableReference,
    input: LogicalPlan,
    definition: Option<String>,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl CreateViewExec {
    pub(crate) fn new(
        name: TableReference,
        input: LogicalPlan,
        definition: Option<String>,
        session: SessionCell,
    ) -> Self {
        Self {
            name,
            input,
            definition,
            session,
            cache: Arc::new(side_effect_properties()),
        }
    }
    fn fmt_label(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CreateViewExec: name={}", self.name)
    }
}

side_effect_exec!(CreateViewExec, "CreateViewExec", |exec: &CreateViewExec| {
    let session = upgrade_session(&exec.session)?;
    let name = exec.name.clone();
    let input = exec.input.clone();
    let definition = exec.definition.clone();
    Ok(side_effect_stream(async move {
        actions::create_view(&session, &name, &input, &definition).map_err(to_df_err)
    }))
});

/// Physical node for `ALTER TABLE`.
#[derive(Debug)]
pub(crate) struct AlterTableExec {
    spec: AlterTableSpec,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl AlterTableExec {
    pub(crate) fn new(spec: AlterTableSpec, session: SessionCell) -> Self {
        Self {
            spec,
            session,
            cache: Arc::new(side_effect_properties()),
        }
    }
    fn fmt_label(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AlterTableExec: name={}", self.spec.name)
    }
}

side_effect_exec!(AlterTableExec, "AlterTableExec", |exec: &AlterTableExec| {
    let session = upgrade_session(&exec.session)?;
    let spec = exec.spec.clone();
    Ok(side_effect_stream(async move {
        actions::alter_table(&session, &spec)
            .await
            .map_err(to_df_err)
    }))
});

/// Physical node for the copy-on-write replacement behind `DELETE`/`UPDATE`.
#[derive(Debug)]
pub(crate) struct ReplaceTableContentsExec {
    table: TableReference,
    child: Arc<dyn ExecutionPlan>,
    mutation: Option<Mutation>,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl ReplaceTableContentsExec {
    pub(crate) fn new(
        table: TableReference,
        child: Arc<dyn ExecutionPlan>,
        mutation: Option<Mutation>,
        session: SessionCell,
    ) -> Self {
        Self {
            table,
            child,
            mutation,
            session,
            cache: Arc::new(side_effect_properties()),
        }
    }
}

impl DisplayAs for ReplaceTableContentsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ReplaceTableContentsExec: table={}", self.table)
            }
            DisplayFormatType::TreeRender => write!(f, "ReplaceTableContentsExec"),
        }
    }
}

impl ExecutionPlan for ReplaceTableContentsExec {
    fn name(&self) -> &str {
        "ReplaceTableContentsExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            table: self.table.clone(),
            child: children.swap_remove(0),
            mutation: self.mutation.clone(),
            session: self.session.clone(),
            cache: self.cache.clone(),
        }))
    }
    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let session = upgrade_session(&self.session)?;
        let table = self.table.clone();
        let child = self.child.clone();
        let mutation = self.mutation.clone();
        Ok(side_effect_stream(async move {
            actions::replace_table_contents(&session, &table, child, mutation, &context)
                .await
                .map_err(to_df_err)
        }))
    }
}

/// Physical node for `INSERT INTO`. Produces the inserted-row count.
#[derive(Debug)]
pub(crate) struct InsertExec {
    table: TableReference,
    op: InsertOp,
    child: Arc<dyn ExecutionPlan>,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl InsertExec {
    pub(crate) fn new(
        table: TableReference,
        op: InsertOp,
        child: Arc<dyn ExecutionPlan>,
        session: SessionCell,
    ) -> Self {
        Self {
            table,
            op,
            child,
            session,
            cache: Arc::new(plan_properties(count_arrow_schema())),
        }
    }
}

impl DisplayAs for InsertExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "InsertExec: table={}", self.table)
            }
            DisplayFormatType::TreeRender => write!(f, "InsertExec"),
        }
    }
}

impl ExecutionPlan for InsertExec {
    fn name(&self) -> &str {
        "InsertExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            table: self.table.clone(),
            op: self.op,
            child: children.swap_remove(0),
            session: self.session.clone(),
            cache: self.cache.clone(),
        }))
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let session = upgrade_session(&self.session)?;
        let table = self.table.table().to_string();
        let op = self.op;
        let child = self.child.clone();
        Ok(forward_stream(count_arrow_schema(), async move {
            actions::insert_into(&session, &table, op, child)
                .await
                .map_err(to_df_err)
        }))
    }
}

/// Physical node for `CREATE TABLE` / `CREATE TABLE AS SELECT`.
#[derive(Debug)]
pub(crate) struct CreateTableExec {
    name: TableReference,
    is_ctas: bool,
    if_not_exists: bool,
    child: Arc<dyn ExecutionPlan>,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl CreateTableExec {
    pub(crate) fn new(
        name: TableReference,
        is_ctas: bool,
        if_not_exists: bool,
        child: Arc<dyn ExecutionPlan>,
        session: SessionCell,
    ) -> Self {
        let schema: SchemaRef = if is_ctas {
            count_arrow_schema()
        } else {
            Arc::new(Schema::empty())
        };
        Self {
            name,
            is_ctas,
            if_not_exists,
            child,
            session,
            cache: Arc::new(plan_properties(schema)),
        }
    }
}

impl DisplayAs for CreateTableExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CreateTableExec: name={}", self.name)
            }
            DisplayFormatType::TreeRender => write!(f, "CreateTableExec"),
        }
    }
}

impl ExecutionPlan for CreateTableExec {
    fn name(&self) -> &str {
        "CreateTableExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            name: self.name.clone(),
            is_ctas: self.is_ctas,
            if_not_exists: self.if_not_exists,
            child: children.swap_remove(0),
            session: self.session.clone(),
            cache: self.cache.clone(),
        }))
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let session = upgrade_session(&self.session)?;
        let name = self.name.clone();
        let child = self.child.clone();
        let is_ctas = self.is_ctas;
        let if_not_exists = self.if_not_exists;
        let schema = self.schema();
        Ok(optional_forward_stream(schema, async move {
            actions::create_table(&session, &name, child, is_ctas, if_not_exists)
                .await
                .map_err(to_df_err)
        }))
    }
}

/// Physical node for `CREATE CRAWLER`.
#[derive(Debug)]
pub(crate) struct CreateCrawlerExec {
    name: String,
    target_prefix: Option<String>,
    options: Vec<(String, String)>,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl CreateCrawlerExec {
    pub(crate) fn new(
        name: String,
        target_prefix: Option<String>,
        options: Vec<(String, String)>,
        session: SessionCell,
    ) -> Self {
        Self {
            name,
            target_prefix,
            options,
            session,
            cache: Arc::new(side_effect_properties()),
        }
    }
    fn fmt_label(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CreateCrawlerExec: name={}", self.name)
    }
}

side_effect_exec!(
    CreateCrawlerExec,
    "CreateCrawlerExec",
    |exec: &CreateCrawlerExec| {
        let session = upgrade_session(&exec.session)?;
        let name = exec.name.clone();
        let target_prefix = exec.target_prefix.clone();
        let options = exec.options.clone();
        Ok(side_effect_stream(async move {
            crawler::create_crawler(&session, &name, target_prefix, &options)
                .await
                .map_err(to_df_err)
        }))
    }
);

/// Physical node for `RUN CRAWLER <name>`.
#[derive(Debug)]
pub(crate) struct RunCrawlerExec {
    name: String,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl RunCrawlerExec {
    pub(crate) fn new(name: String, session: SessionCell) -> Self {
        Self {
            name,
            session,
            cache: Arc::new(side_effect_properties()),
        }
    }
    fn fmt_label(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RunCrawlerExec: name={}", self.name)
    }
}

side_effect_exec!(RunCrawlerExec, "RunCrawlerExec", |exec: &RunCrawlerExec| {
    let session = upgrade_session(&exec.session)?;
    let name = exec.name.clone();
    Ok(side_effect_stream(async move {
        crawler::run_crawler(&session, &name).await.map_err(to_df_err)
    }))
});

/// Physical node for `DROP CRAWLER <name>`.
#[derive(Debug)]
pub(crate) struct DropCrawlerExec {
    name: String,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl DropCrawlerExec {
    pub(crate) fn new(name: String, session: SessionCell) -> Self {
        Self {
            name,
            session,
            cache: Arc::new(side_effect_properties()),
        }
    }
    fn fmt_label(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DropCrawlerExec: name={}", self.name)
    }
}

side_effect_exec!(
    DropCrawlerExec,
    "DropCrawlerExec",
    |exec: &DropCrawlerExec| {
        let session = upgrade_session(&exec.session)?;
        let name = exec.name.clone();
        Ok(side_effect_stream(async move {
            crawler::drop_crawler(&session, &name)
                .await
                .map_err(to_df_err)
        }))
    }
);

/// Physical node for `SHOW CRAWLERS`. Unlike the other crawler nodes it produces
/// rows (one per crawler).
#[derive(Debug)]
pub(crate) struct ShowCrawlersExec {
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl ShowCrawlersExec {
    pub(crate) fn new(session: SessionCell) -> Self {
        Self {
            session,
            cache: Arc::new(plan_properties(show_crawlers_arrow_schema())),
        }
    }
}

impl DisplayAs for ShowCrawlersExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(f, "ShowCrawlersExec"),
            DisplayFormatType::TreeRender => write!(f, "ShowCrawlersExec"),
        }
    }
}

impl ExecutionPlan for ShowCrawlersExec {
    fn name(&self) -> &str {
        "ShowCrawlersExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let session = upgrade_session(&self.session)?;
        let schema = show_crawlers_arrow_schema();
        let stream = futures::stream::once(async move {
            crawler::show_crawlers(&session).await.map_err(to_df_err)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Physical node for `CREATE INDEX ... ON <table> (<column>)`.
#[derive(Debug)]
pub(crate) struct CreateIndexExec {
    table: String,
    column: String,
    name: Option<String>,
    using: Option<String>,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl CreateIndexExec {
    pub(crate) fn new(
        table: String,
        column: String,
        name: Option<String>,
        using: Option<String>,
        session: SessionCell,
    ) -> Self {
        Self {
            table,
            column,
            name,
            using,
            session,
            cache: Arc::new(side_effect_properties()),
        }
    }
    fn fmt_label(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CreateIndexExec: table={} column={}", self.table, self.column)
    }
}

side_effect_exec!(CreateIndexExec, "CreateIndexExec", |exec: &CreateIndexExec| {
    let session = upgrade_session(&exec.session)?;
    let table = exec.table.clone();
    let column = exec.column.clone();
    let name = exec.name.clone();
    let using = exec.using.clone();
    Ok(side_effect_stream(async move {
        actions::create_index(&session, &table, &column, name, using)
            .await
            .map_err(to_df_err)
    }))
});

/// Physical node for `DROP INDEX <name> ON <table>`.
#[derive(Debug)]
pub(crate) struct DropIndexExec {
    table: String,
    name: String,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl DropIndexExec {
    pub(crate) fn new(table: String, name: String, session: SessionCell) -> Self {
        Self {
            table,
            name,
            session,
            cache: Arc::new(side_effect_properties()),
        }
    }
    fn fmt_label(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DropIndexExec: table={} name={}", self.table, self.name)
    }
}

side_effect_exec!(DropIndexExec, "DropIndexExec", |exec: &DropIndexExec| {
    let session = upgrade_session(&exec.session)?;
    let table = exec.table.clone();
    let name = exec.name.clone();
    Ok(side_effect_stream(async move {
        actions::drop_index(&session, &table, &name)
            .await
            .map_err(to_df_err)
    }))
});

/// Physical node for `SHOW INDEXES ON <table>`. Produces one row per index.
#[derive(Debug)]
pub(crate) struct ShowIndexesExec {
    table: String,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl ShowIndexesExec {
    pub(crate) fn new(table: String, session: SessionCell) -> Self {
        Self {
            table,
            session,
            cache: Arc::new(plan_properties(show_indexes_arrow_schema())),
        }
    }
}

impl DisplayAs for ShowIndexesExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ShowIndexesExec: table={}", self.table)
            }
            DisplayFormatType::TreeRender => write!(f, "ShowIndexesExec"),
        }
    }
}

impl ExecutionPlan for ShowIndexesExec {
    fn name(&self) -> &str {
        "ShowIndexesExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let session = upgrade_session(&self.session)?;
        let table = self.table.clone();
        let schema = show_indexes_arrow_schema();
        let stream = futures::stream::once(async move {
            actions::list_indexes(&session, &table).await.map_err(to_df_err)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Physical node for `SET EXTENSION '<kind>' FOR <table> TO '<json>'`.
#[derive(Debug)]
pub(crate) struct SetExtensionExec {
    kind: String,
    table: String,
    json: String,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl SetExtensionExec {
    pub(crate) fn new(kind: String, table: String, json: String, session: SessionCell) -> Self {
        Self {
            kind,
            table,
            json,
            session,
            cache: Arc::new(side_effect_properties()),
        }
    }
    fn fmt_label(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SetExtensionExec: table={} kind={}", self.table, self.kind)
    }
}

side_effect_exec!(
    SetExtensionExec,
    "SetExtensionExec",
    |exec: &SetExtensionExec| {
        let session = upgrade_session(&exec.session)?;
        let kind = exec.kind.clone();
        let table = exec.table.clone();
        let json = exec.json.clone();
        Ok(side_effect_stream(async move {
            set_table_extension(&session, &table, &kind, &json)
                .await
                .map_err(to_df_err)
        }))
    }
);

/// Physical node for `DROP EXTENSION '<kind>' FOR <table>`.
#[derive(Debug)]
pub(crate) struct DropExtensionExec {
    kind: String,
    table: String,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl DropExtensionExec {
    pub(crate) fn new(kind: String, table: String, session: SessionCell) -> Self {
        Self {
            kind,
            table,
            session,
            cache: Arc::new(side_effect_properties()),
        }
    }
    fn fmt_label(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DropExtensionExec: table={} kind={}", self.table, self.kind)
    }
}

side_effect_exec!(
    DropExtensionExec,
    "DropExtensionExec",
    |exec: &DropExtensionExec| {
        let session = upgrade_session(&exec.session)?;
        let kind = exec.kind.clone();
        let table = exec.table.clone();
        Ok(side_effect_stream(async move {
            drop_table_extension(&session, &table, &kind)
                .await
                .map_err(to_df_err)
        }))
    }
);

/// Physical node for `SHOW EXTENSIONS FOR <table>`. Produces one JSON row.
#[derive(Debug)]
pub(crate) struct ShowExtensionsExec {
    table: String,
    session: SessionCell,
    cache: Arc<PlanProperties>,
}

impl ShowExtensionsExec {
    pub(crate) fn new(table: String, session: SessionCell) -> Self {
        Self {
            table,
            session,
            cache: Arc::new(plan_properties(show_extensions_arrow_schema())),
        }
    }
}

impl DisplayAs for ShowExtensionsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ShowExtensionsExec: table={}", self.table)
            }
            DisplayFormatType::TreeRender => write!(f, "ShowExtensionsExec"),
        }
    }
}

impl ExecutionPlan for ShowExtensionsExec {
    fn name(&self) -> &str {
        "ShowExtensionsExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let session = upgrade_session(&self.session)?;
        let table = self.table.clone();
        let schema = show_extensions_arrow_schema();
        let stream = futures::stream::once(async move {
            show_table_extensions_batch(&session, &table)
                .await
                .map_err(to_df_err)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
