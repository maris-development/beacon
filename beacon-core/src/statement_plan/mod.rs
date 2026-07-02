//! Beacon's custom physical-planner path: a single execution pipeline for
//! everything beacon runs.
//!
//! Statements and queries alike are lowered to a DataFusion `LogicalPlan` —
//! standard DDL/DML nodes for what DataFusion can represent, and
//! [`LogicalPlan::Extension`] nodes (see [`logical`] / [`physical`]) for what it
//! cannot (materialized views, `REFRESH`, `ALTER TABLE`, copy-on-write
//! `DELETE`/`UPDATE`) — then validated ([`validate_query_plan`]) and executed
//! through `create_physical_plan` -> `execute_stream` ([`execute_statement_plan`]),
//! with the [`BeaconQueryPlanner`] turning beacon's nodes into execution plans.
//!
//! [`LogicalPlan::Extension`]: datafusion::logical_expr::LogicalPlan::Extension

mod actions;
mod auth;
mod authz;
pub(crate) mod crawler;
mod logical;
mod lower;
pub(crate) mod materialized_view;
mod physical;
mod query_planner;
mod stream_coalescer;
pub(crate) mod table_engine;

use std::sync::{Arc, OnceLock, Weak};

use datafusion::{
    common::tree_node::{TreeNode, TreeNodeRecursion},
    execution::SendableRecordBatchStream,
    logical_expr::{Extension, LogicalPlan},
    prelude::{SQLOptions, SessionContext},
};

use crate::parser::statement::{
    AuthStatement, CreateCrawlerStatement, CreateIndexStatement, CreateMaterializedViewStatement,
    DropCrawlerStatement, DropExtensionStatement, DropIndexStatement, RefreshStatement,
    RunCrawlerStatement, SetExtensionStatement, ShowExtensionsStatement, ShowIndexesStatement,
};

pub(crate) use authz::authorize_logical_plan;
pub(crate) use lower::lower_df_statement;
pub(crate) use query_planner::BeaconQueryPlanner;

/// Validate a lowered query plan against the caller's privileges, just before
/// execution — the single place permissions are enforced (rather than in the SQL
/// parser or the JSON compiler).
///
/// Standard `DDL`/`DML`/`COPY` nodes are gated by DataFusion's
/// [`SQLOptions::verify_plan`] (everything allowed for super-users, nothing for
/// others). Any beacon [`LogicalPlan::Extension`] node — materialized views,
/// `REFRESH`, `ALTER TABLE`, and the copy-on-write replacement behind
/// `DELETE`/`UPDATE` — additionally requires super-user, since `verify_plan`
/// cannot see through extension nodes.
pub(crate) fn validate_query_plan(plan: &LogicalPlan, is_super_user: bool) -> anyhow::Result<()> {
    let sql_options = SQLOptions::new()
        .with_allow_ddl(is_super_user)
        .with_allow_dml(is_super_user)
        .with_allow_statements(is_super_user);
    // A super-user is allowed every statement kind, so any failure here is a
    // non-super-user attempting a privileged operation. DataFusion's own message
    // ("DDL not supported: ...") reads like a missing feature, so reframe it as a
    // permissions error while keeping the underlying detail for debugging.
    sql_options.verify_plan(plan).map_err(|source| {
        anyhow::anyhow!(
            "operation not permitted: this statement requires super-user privileges ({source})"
        )
    })?;

    if !is_super_user && plan_contains_extension(plan)? {
        anyhow::bail!("operation not permitted: this statement requires super-user privileges");
    }

    Ok(())
}

/// Whether `plan` produces a result set that can be exported in an output format.
///
/// A requested output format wraps the plan in a `COPY TO` (see
/// [`Output::parse`](crate::query::output::Output::parse)), which only accepts a
/// row-producing input. Side-effecting / administrative statements return no rows,
/// so they cannot be exported: standard DDL, DML (`INSERT`/`UPDATE`/`DELETE`),
/// `COPY`, and `SET`, plus beacon's side-effecting extension nodes (materialized
/// views, `REFRESH`, `ALTER TABLE`, copy-on-write `DELETE`/`UPDATE`, crawler/index
/// DDL), which all expose an empty schema. Row-producing statements (`SELECT`,
/// `SHOW CRAWLERS`, `SHOW INDEXES`, ...) do produce an exportable result set.
pub(crate) fn plan_produces_result_set(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Ddl(_)
        | LogicalPlan::Dml(_)
        | LogicalPlan::Copy(_)
        | LogicalPlan::Statement(_) => false,
        // Beacon's extension nodes carry their own output schema: the
        // side-effecting ones (materialized views, `REFRESH`, `ALTER TABLE`,
        // copy-on-write `DELETE`/`UPDATE`, crawler/index DDL) report an empty
        // schema, while the row-producing ones (`SHOW CRAWLERS`, `SHOW INDEXES`)
        // report real columns — so the schema decides whether they can be exported.
        LogicalPlan::Extension(ext) => !ext.node.schema().fields().is_empty(),
        // Everything else is a row-producing query (`SELECT`, `VALUES`, ...).
        other => !other.schema().fields().is_empty(),
    }
}

/// Whether `plan` contains any [`LogicalPlan::Extension`] node (all of beacon's
/// extension nodes are super-user-only operations).
fn plan_contains_extension(plan: &LogicalPlan) -> anyhow::Result<bool> {
    let mut found = false;
    plan.apply(|node| {
        if matches!(node, LogicalPlan::Extension(_)) {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    Ok(found)
}

/// Late-initialized, weak handle to the [`SessionContext`] shared with the
/// custom planner.
///
/// The context is built *from* the session state that owns the planner, so the
/// planner is constructed with an empty cell that is filled with a [`Weak`]
/// reference immediately after the context exists (see `Runtime::init_ctx`).
/// Beacon's custom execution-plan nodes only receive a `TaskContext` at
/// execution time, but their side effects (e.g. `register_table`, catalog
/// access) need the full `SessionContext`; this cell is how they recover it. A
/// `Weak` avoids the context -> state -> planner -> context reference cycle.
pub(crate) type SessionCell = Arc<OnceLock<Weak<SessionContext>>>;

/// Create an empty [`SessionCell`] to be filled once the context exists.
pub(crate) fn new_session_cell() -> SessionCell {
    Arc::new(OnceLock::new())
}

/// Build the logical plan for an auth-management statement (CREATE/DROP USER/ROLE, GRANT/DENY/
/// REVOKE). Lowered to an [`Extension`] node so it inherits the super-user gate in
/// [`validate_query_plan`] (all beacon extension nodes are super-user-only).
pub(crate) fn auth_plan(statement: AuthStatement) -> LogicalPlan {
    let key = statement.to_string();
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::AuthNode {
            statement: logical::Keyed::new(key, statement),
        }),
    })
}

/// Build the logical plan for `CREATE MATERIALIZED VIEW <name> AS <query>`.
pub(crate) fn create_materialized_view_plan(
    statement: CreateMaterializedViewStatement,
) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::CreateMaterializedViewNode::new(
            statement.view_name.to_string(),
            statement.query_sql,
        )),
    })
}

/// Build the logical plan for `REFRESH [TABLE] <name>`.
pub(crate) fn refresh_plan(statement: RefreshStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::RefreshNode::new(statement.name.to_string())),
    })
}

/// Build the logical plan for `CREATE CRAWLER ...`.
pub(crate) fn create_crawler_plan(statement: CreateCrawlerStatement) -> LogicalPlan {
    let options: Vec<(String, String)> = statement.options.into_iter().collect();
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::CreateCrawlerNode::new(
            statement.name.to_string(),
            statement.target_prefix,
            options,
        )),
    })
}

/// Build the logical plan for `RUN CRAWLER <name>`.
pub(crate) fn run_crawler_plan(statement: RunCrawlerStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::RunCrawlerNode::new(statement.name.to_string())),
    })
}

/// Build the logical plan for `DROP CRAWLER <name>`.
pub(crate) fn drop_crawler_plan(statement: DropCrawlerStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::DropCrawlerNode::new(statement.name.to_string())),
    })
}

/// Build the logical plan for `SHOW CRAWLERS`.
pub(crate) fn show_crawlers_plan() -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::ShowCrawlersNode),
    })
}

/// Build the logical plan for `SET EXTENSION '<kind>' FOR <table> TO '<json>'`.
pub(crate) fn set_extension_plan(statement: SetExtensionStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::SetExtensionNode::new(
            statement.kind,
            statement.table.to_string(),
            statement.json,
        )),
    })
}

/// Build the logical plan for `DROP EXTENSION '<kind>' FOR <table>`.
pub(crate) fn drop_extension_plan(statement: DropExtensionStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::DropExtensionNode::new(
            statement.kind,
            statement.table.to_string(),
        )),
    })
}

/// Build the logical plan for `SHOW EXTENSIONS FOR <table>`.
pub(crate) fn show_extensions_plan(statement: ShowExtensionsStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::ShowExtensionsNode::new(statement.table.to_string())),
    })
}

/// Build the logical plan for `CREATE INDEX [<name>] ON <table> (<column>) [USING <type>]`.
pub(crate) fn create_index_plan(statement: CreateIndexStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::CreateIndexNode {
            table: statement.table.to_string(),
            column: statement.column,
            name: statement.name.map(|n| n.to_string()),
            using: statement.using,
        }),
    })
}

/// Build the logical plan for `DROP INDEX <name> ON <table>`.
pub(crate) fn drop_index_plan(statement: DropIndexStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::DropIndexNode {
            table: statement.table.to_string(),
            name: statement.name.to_string(),
        }),
    })
}

/// Build the logical plan for `SHOW INDEXES ON <table>`.
pub(crate) fn show_indexes_plan(statement: ShowIndexesStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::ShowIndexesNode {
            table: statement.table.to_string(),
        }),
    })
}

/// Plan and execute a beacon statement logical plan through the single
/// `create_physical_plan` -> `execute_stream` pipeline, coalescing the result the
/// same way the legacy statement executor does.
///
/// Side-effecting statements (DDL, `DELETE`/`UPDATE`, materialized-view ops)
/// produce no rows, i.e. an empty output schema. Those are driven to completion
/// here so the side effect is performed and any error surfaces eagerly from
/// `run_sql` — as the legacy handlers did — rather than only when the caller
/// drains the stream. Row-producing statements (`SELECT`, `INSERT`, `COPY`) keep
/// streaming lazily.
pub(crate) async fn execute_statement_plan(
    session_ctx: &Arc<SessionContext>,
    plan: LogicalPlan,
) -> anyhow::Result<SendableRecordBatchStream> {
    use futures::TryStreamExt;

    // Statements (e.g. `SET beacon.table_engine = '…'`) cannot be physical-planned;
    // DataFusion applies them to the session via `execute_logical_plan`. Route them
    // there so the session config actually changes, then drain the (empty) result.
    if matches!(plan, LogicalPlan::Statement(_)) {
        let schema: arrow::datatypes::SchemaRef = Arc::new(arrow::datatypes::Schema::empty());
        session_ctx
            .execute_logical_plan(plan)
            .await?
            .collect()
            .await?;
        return Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                schema,
                futures::stream::empty(),
            ),
        ));
    }

    let physical_plan = session_ctx.state().create_physical_plan(&plan).await?;
    let stream = datafusion::physical_plan::execute_stream(physical_plan, session_ctx.task_ctx())?;
    let stream = stream_coalescer::coalesce_sql_stream(session_ctx, stream);

    if stream.schema().fields().is_empty() {
        let schema = stream.schema();
        stream.try_collect::<Vec<_>>().await?;
        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                schema,
                futures::stream::empty(),
            ),
        ))
    } else {
        Ok(stream)
    }
}
