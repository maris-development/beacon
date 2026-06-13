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
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    },
    prelude::SessionContext,
};
use futures::StreamExt;

use super::{materialized_view, SessionCell};

/// `PlanProperties` for a single-partition node with an empty output schema.
fn side_effect_properties() -> PlanProperties {
    let schema: SchemaRef = Arc::new(Schema::empty());
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
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
