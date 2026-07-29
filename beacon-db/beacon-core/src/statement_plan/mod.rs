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

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};

use datafusion::{
    common::tree_node::{TreeNode, TreeNodeRecursion},
    execution::SendableRecordBatchStream,
    logical_expr::{Extension, LogicalPlan},
    prelude::{SQLOptions, SessionContext},
};

use crate::parser::statement::{
    AttachStatement, AuthStatement, CreateCrawlerStatement, CreateIndexStatement,
    CreateMaterializedViewStatement, CreateSecretStatement, DetachStatement, DropCrawlerStatement,
    DropExtensionStatement, DropIndexStatement, DropSecretStatement, RefreshStatement,
    RunCrawlerStatement, SetExtensionStatement, ShowExtensionsStatement, ShowIndexesStatement,
    SummarizeStatement,
};

pub(crate) use authz::authorize_logical_plan;
pub(crate) use stream_coalescer::CoalesceSqlStream;
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

/// Upgrade a [`SessionCell`] to the live session, naming the caller in the error.
///
/// Fails only once the runtime has been torn down (or, in principle, before the
/// cell is filled — which cannot happen for anything built during startup).
pub(crate) fn upgrade_session(cell: &SessionCell, who: &str) -> anyhow::Result<Arc<SessionContext>> {
    cell.get()
        .and_then(|weak| weak.upgrade())
        .ok_or_else(|| anyhow::anyhow!("{who}: beacon session context is unavailable"))
}

/// The bare value of a parsed object name, for use as a storage/lookup key.
/// `ObjectName::Display` re-adds SQL quoting, so lowering via `to_string()`
/// would store `CREATE CRAWLER "c1"` under the key `"c1"` instead of `c1`.
fn object_name_value(name: &datafusion::sql::sqlparser::ast::ObjectName) -> String {
    name.0
        .iter()
        .map(|part| match part.as_ident() {
            Some(ident) => ident.value.clone(),
            None => part.to_string(),
        })
        .collect::<Vec<_>>()
        .join(".")
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
            object_name_value(&statement.view_name),
            statement.query_sql,
        )),
    })
}

/// Build the logical plan for `REFRESH [TABLE] <name>`.
pub(crate) fn refresh_plan(statement: RefreshStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::RefreshNode::new(object_name_value(&statement.name))),
    })
}

/// Build the logical plan for `CREATE CRAWLER ...`.
pub(crate) fn create_crawler_plan(statement: CreateCrawlerStatement) -> LogicalPlan {
    let options: Vec<(String, String)> = statement.options.into_iter().collect();
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::CreateCrawlerNode::new(
            object_name_value(&statement.name),
            statement.target_prefix,
            options,
        )),
    })
}

/// Build the logical plan for `RUN CRAWLER <name>`.
pub(crate) fn run_crawler_plan(statement: RunCrawlerStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::RunCrawlerNode::new(object_name_value(&statement.name))),
    })
}

/// Build the logical plan for `DROP CRAWLER <name>`.
pub(crate) fn drop_crawler_plan(statement: DropCrawlerStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::DropCrawlerNode::new(object_name_value(&statement.name))),
    })
}

/// Build the logical plan for `ATTACH '<url>' AS <name> [WITH (...)]`.
///
/// Recognized options: `token` (bearer), `username`+`password` (Basic), and `tls`. Fallible
/// because the credential combination is validated here (e.g. a token *and* a password is refused).
pub(crate) fn attach_plan(statement: AttachStatement) -> anyhow::Result<LogicalPlan> {
    let secret = statement.options.get("secret").cloned();
    let token = statement.options.get("token").cloned();
    let username = statement.options.get("username").cloned();
    let password = statement.options.get("password").cloned();
    anyhow::ensure!(
        !(secret.is_some() && (token.is_some() || username.is_some() || password.is_some())),
        "ATTACH takes either a `secret` or inline credentials, not both"
    );
    // Inline credential (used when no `secret` is named); a named secret is resolved at execution.
    let credential =
        beacon_datafusion_ext::remote::RemoteCredential::from_parts(token, username, password)?;
    let tls = statement
        .options
        .get("tls")
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(logical::AttachNode::new(
            statement.name,
            statement.url,
            credential,
            secret,
            tls,
        )),
    }))
}

/// Build the logical plan for `DETACH <name>`.
pub(crate) fn detach_plan(statement: DetachStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::DetachNode::new(statement.name)),
    })
}

/// Build the logical plan for `CREATE SECRET <name> (TYPE …, …, SCOPE …)`.
///
/// Fallible: `TYPE` is required and validated here, and `SCOPE` defaults to the backend's
/// scheme-wide prefix. The remaining parameters are credential options, with the conventional
/// names (`KEY_ID`, `SECRET`, `REGION`, …) mapped to `object_store` config keys.
pub(crate) fn create_secret_plan(statement: CreateSecretStatement) -> anyhow::Result<LogicalPlan> {
    use beacon_datafusion_ext::secrets::SecretType;

    let mut params = statement.params;
    let type_value = take_ci(&mut params, "type").ok_or_else(|| {
        anyhow::anyhow!("CREATE SECRET requires a TYPE (S3, GCS, AZURE, HTTP, or BEACON)")
    })?;
    let secret_type = SecretType::parse(&type_value).ok_or_else(|| {
        anyhow::anyhow!("unknown secret TYPE '{type_value}'; use S3, GCS, AZURE, HTTP, or BEACON")
    })?;
    let scope =
        take_ci(&mut params, "scope").unwrap_or_else(|| secret_type.default_scope().to_string());

    let mut options: Vec<(String, String)> = params
        .into_iter()
        .map(|(key, value)| {
            // Beacon secrets carry Flight SQL creds (`token`/`username`/`password`) verbatim; only
            // object-store secrets get the conventional→object_store option-name aliasing.
            let key = if secret_type.is_beacon() {
                key.to_ascii_lowercase()
            } else {
                normalize_secret_option_key(&key)
            };
            (key, value)
        })
        .collect();
    // Sorted so the node hashes/compares deterministically.
    options.sort();

    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(logical::CreateSecretNode::new(
            statement.name,
            secret_type,
            scope,
            options,
            statement.persistent,
        )),
    }))
}

/// Build the logical plan for `DROP SECRET [IF EXISTS] <name>`.
pub(crate) fn drop_secret_plan(statement: DropSecretStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::DropSecretNode::new(
            statement.name,
            statement.if_exists,
        )),
    })
}

/// Build the logical plan for `SHOW SECRETS`.
pub(crate) fn show_secrets_plan() -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::ShowSecretsNode),
    })
}

/// Build the logical plan for `SUMMARIZE <source>`.
///
/// Lowers to a generated, single-pass aggregate `SELECT` (no custom node): one CTE computes every
/// column's stats in one scan, and a `UNION ALL` re-projects that one row into one output row per
/// column. Because the result is an ordinary query, `SUMMARIZE` works on a read-only database and
/// needs no special privileges.
pub(crate) async fn summarize_plan(
    session_ctx: &SessionContext,
    statement: SummarizeStatement,
) -> anyhow::Result<LogicalPlan> {
    // Plan the source once (no execution) to learn its columns and types.
    let source_plan = session_ctx
        .state()
        .create_logical_plan(&statement.source)
        .await
        .map_err(|e| anyhow::anyhow!("SUMMARIZE source could not be planned: {e}"))?;
    let fields = source_plan.schema().fields();
    anyhow::ensure!(
        !fields.is_empty(),
        "SUMMARIZE requires a source with at least one column"
    );

    let sql = build_summarize_sql(&statement.source, fields);
    session_ctx
        .state()
        .create_logical_plan(&sql)
        .await
        .map_err(|e| anyhow::anyhow!("failed to plan SUMMARIZE: {e}"))
}

/// Generate the single-pass profiling SQL for `SUMMARIZE` over `source`'s `fields`.
fn build_summarize_sql(source: &str, fields: &arrow::datatypes::Fields) -> String {
    use arrow::datatypes::DataType;

    let is_numeric = |dt: &DataType| {
        matches!(
            dt,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(..)
                | DataType::Decimal256(..)
        )
    };
    let is_orderable = |dt: &DataType| {
        is_numeric(dt)
            || matches!(
                dt,
                DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Utf8View
                    | DataType::Boolean
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Time32(_)
                    | DataType::Time64(_)
                    | DataType::Timestamp(..)
                    | DataType::Duration(_)
            )
    };

    let mut aggs: Vec<String> = vec!["CAST(count(*) AS BIGINT) AS __n".to_string()];
    let mut branches: Vec<String> = Vec::new();

    for (i, field) in fields.iter().enumerate() {
        let col = summarize_quote_ident(field.name());
        let dt = field.data_type();
        let (orderable, numeric) = (is_orderable(dt), is_numeric(dt));

        // Every stat is cast to a uniform output type so all UNION branches align; unsupported
        // stats (min/max of an unorderable type, avg/std of a non-numeric) become NULL.
        let null_v = "CAST(NULL AS VARCHAR)";
        let null_i = "CAST(NULL AS BIGINT)";
        let null_d = "CAST(NULL AS DOUBLE)";
        let min = if orderable { format!("CAST(min({col}) AS VARCHAR)") } else { null_v.into() };
        let max = if orderable { format!("CAST(max({col}) AS VARCHAR)") } else { null_v.into() };
        // Exact distinct (approx_distinct doesn't cover floats); fine for a profiling summary.
        let uniq = if orderable { format!("CAST(count(DISTINCT {col}) AS BIGINT)") } else { null_i.into() };
        let avg = if numeric { format!("CAST(avg(TRY_CAST({col} AS DOUBLE)) AS DOUBLE)") } else { null_d.into() };
        let std = if numeric { format!("CAST(stddev(TRY_CAST({col} AS DOUBLE)) AS DOUBLE)") } else { null_d.into() };

        aggs.push(format!("{min} AS c{i}_min"));
        aggs.push(format!("{max} AS c{i}_max"));
        aggs.push(format!("{uniq} AS c{i}_uniq"));
        aggs.push(format!("{avg} AS c{i}_avg"));
        aggs.push(format!("{std} AS c{i}_std"));
        aggs.push(format!("CAST(count({col}) AS BIGINT) AS c{i}_cnt"));

        let name_lit = summarize_string_literal(field.name());
        let type_lit = summarize_string_literal(&dt.to_string());
        // `__ord` keeps the output in the source's column order (UNION ALL is unordered).
        branches.push(format!(
            "SELECT {i} AS __ord, {name_lit} AS column_name, {type_lit} AS column_type, \
             c{i}_min AS \"min\", c{i}_max AS \"max\", c{i}_uniq AS \"distinct\", \
             c{i}_avg AS \"avg\", c{i}_std AS \"std\", c{i}_cnt AS \"count\", \
             CAST(CASE WHEN __n = 0 THEN 0 ELSE (__n - c{i}_cnt) * 100.0 / __n END AS DOUBLE) \
             AS null_percentage FROM __summarize_agg"
        ));
    }

    format!(
        "WITH __summarize_agg AS (SELECT {} FROM ({source}) AS __summarize_src) \
         SELECT column_name, column_type, \"min\", \"max\", \"distinct\", \"avg\", \"std\", \
         \"count\", null_percentage FROM ({}) AS __summarize_out ORDER BY __ord",
        aggs.join(", "),
        branches.join(" UNION ALL ")
    )
}

fn summarize_quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn summarize_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Remove a key from `params` case-insensitively, returning its value.
fn take_ci(params: &mut HashMap<String, String>, key: &str) -> Option<String> {
    let found = params.keys().find(|k| k.eq_ignore_ascii_case(key)).cloned()?;
    params.remove(&found)
}

/// Map conventional `CREATE SECRET` parameter names to `object_store` config keys; unknown keys pass
/// through lowercased, so native `object_store` keys (`access_key_id`, `region`, …) work directly.
fn normalize_secret_option_key(key: &str) -> String {
    match key.to_ascii_uppercase().as_str() {
        "KEY_ID" => "access_key_id".to_string(),
        "SECRET" => "secret_access_key".to_string(),
        "REGION" => "region".to_string(),
        "SESSION_TOKEN" => "session_token".to_string(),
        "ENDPOINT" => "endpoint".to_string(),
        "ACCOUNT_NAME" => "account_name".to_string(),
        "ACCOUNT_KEY" => "account_key".to_string(),
        _ => key.to_ascii_lowercase(),
    }
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
            object_name_value(&statement.table),
            statement.json,
        )),
    })
}

/// Build the logical plan for `DROP EXTENSION '<kind>' FOR <table>`.
pub(crate) fn drop_extension_plan(statement: DropExtensionStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::DropExtensionNode::new(
            statement.kind,
            object_name_value(&statement.table),
        )),
    })
}

/// Build the logical plan for `SHOW EXTENSIONS FOR <table>`.
pub(crate) fn show_extensions_plan(statement: ShowExtensionsStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::ShowExtensionsNode::new(object_name_value(&statement.table))),
    })
}

/// Build the logical plan for `CREATE INDEX [<name>] ON <table> (<column>) [USING <type>]`.
pub(crate) fn create_index_plan(statement: CreateIndexStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::CreateIndexNode {
            table: object_name_value(&statement.table),
            column: statement.column,
            name: statement.name.as_ref().map(object_name_value),
            using: statement.using,
        }),
    })
}

/// Build the logical plan for `DROP INDEX <name> ON <table>`.
pub(crate) fn drop_index_plan(statement: DropIndexStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::DropIndexNode {
            table: object_name_value(&statement.table),
            name: object_name_value(&statement.name),
        }),
    })
}

/// Build the logical plan for `SHOW INDEXES ON <table>`.
pub(crate) fn show_indexes_plan(statement: ShowIndexesStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(logical::ShowIndexesNode {
            table: object_name_value(&statement.table),
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
    let (stream, _physical_plan) = execute_statement_plan_tracked(session_ctx, plan).await?;
    Ok(stream)
}

/// [`execute_statement_plan`], additionally returning the physical plan it built.
///
/// The plan is handed back so a caller can register it with a `MetricsTracker`:
/// per-node metrics are populated as the returned stream drains, so the same
/// `Arc` read after the stream ends carries the runtime metrics. `None` for
/// `Statement` plans, which never get a physical plan.
pub(crate) async fn execute_statement_plan_tracked(
    session_ctx: &Arc<SessionContext>,
    plan: LogicalPlan,
) -> anyhow::Result<(
    SendableRecordBatchStream,
    Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
)> {
    use futures::TryStreamExt;

    // Statements (e.g. `SET datafusion.execution.batch_size = …`) cannot be
    // physical-planned; DataFusion applies them to the session via
    // `execute_logical_plan`. Route them there so the session config actually
    // changes, then drain the (empty) result.
    if matches!(plan, LogicalPlan::Statement(_)) {
        let schema: arrow::datatypes::SchemaRef = Arc::new(arrow::datatypes::Schema::empty());
        session_ctx
            .execute_logical_plan(plan)
            .await?
            .collect()
            .await?;
        return Ok((
            Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::empty(),
                ),
            ),
            None,
        ));
    }

    let physical_plan = session_ctx.state().create_physical_plan(&plan).await?;
    // Order-preserving collect. DataFusion's `execute_stream` coalesces a
    // multi-partition plan in *completion* order, which makes results
    // non-reproducible run to run (most visibly `read_parquet(...) LIMIT 5`).
    // This variant concatenates the partitions in index order instead; they still
    // execute concurrently, bounded by the memory pool.
    let stream = beacon_datafusion_ext::ordered_union::execute_stream_ordered(
        physical_plan.clone(),
        session_ctx.task_ctx(),
    )?;
    let stream = CoalesceSqlStream::from_session(session_ctx).coalesce(stream);

    if stream.schema().fields().is_empty() {
        let schema = stream.schema();
        stream.try_collect::<Vec<_>>().await?;
        Ok((
            Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::empty(),
                ),
            ),
            Some(physical_plan),
        ))
    } else {
        Ok((stream, Some(physical_plan)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::parser::beacon_parser::BeaconParser;
    use crate::parser::statement::BeaconStatement;

    /// Lower a beacon statement the way `Runtime::lower_sql` does for the
    /// non-DataFusion arms, so the tests exercise the real node shapes.
    fn beacon_plan(sql: &str) -> LogicalPlan {
        let statement = BeaconParser::new(sql)
            .unwrap()
            .parse_statement()
            .expect("beacon statement should parse");
        match statement {
            BeaconStatement::Refresh(statement) => refresh_plan(statement),
            BeaconStatement::ShowCrawlers => show_crawlers_plan(),
            BeaconStatement::ShowIndexes(statement) => show_indexes_plan(statement),
            BeaconStatement::Auth(statement) => auth_plan(statement),
            other => panic!("unexpected statement for `{sql}`: {other:?}"),
        }
    }

    async fn df_plan(sql: &str) -> LogicalPlan {
        SessionContext::new()
            .state()
            .create_logical_plan(sql)
            .await
            .expect("SQL should plan")
    }

    /// A requested output format wraps the plan in a `COPY TO`, which only accepts
    /// a row-producing input. Row-producing statements must be exportable and
    /// side-effecting ones must not, or `run_query` either rejects a valid export
    /// or lets the `COPY TO` builder fail with a cryptic planner error.
    #[tokio::test]
    async fn only_row_producing_statements_are_exportable() {
        assert!(plan_produces_result_set(&df_plan("SELECT 1").await));
        assert!(plan_produces_result_set(&df_plan("VALUES (1), (2)").await));

        assert!(!plan_produces_result_set(
            &df_plan("CREATE TABLE t (a INT)").await
        ));
        assert!(!plan_produces_result_set(
            &df_plan("SET datafusion.execution.batch_size = 100").await
        ));
    }

    /// Beacon's extension nodes are invisible to DataFusion's plan inspection, so
    /// exportability is decided by the node's own schema: the side-effecting nodes
    /// expose an empty schema, the `SHOW ...` nodes expose real columns.
    #[test]
    fn extension_nodes_are_exportable_only_when_they_expose_columns() {
        assert!(!plan_produces_result_set(&beacon_plan("REFRESH t")));
        assert!(!plan_produces_result_set(&beacon_plan("CREATE ROLE reader")));

        assert!(plan_produces_result_set(&beacon_plan("SHOW CRAWLERS")));
        assert!(plan_produces_result_set(&beacon_plan("SHOW INDEXES ON t")));
    }

    /// The super-user gate is the single enforcement point for privileged
    /// statements. DDL/DML/`SET` are gated through DataFusion's `verify_plan`, and
    /// the failure must be reframed as a permissions error — the raw DataFusion
    /// message reads like a missing feature.
    #[tokio::test]
    async fn privileged_statements_require_super_user() {
        for sql in [
            "CREATE TABLE t (a INT)",
            "SET datafusion.execution.batch_size = 100",
        ] {
            let plan = df_plan(sql).await;
            let error = validate_query_plan(&plan, false)
                .expect_err("`{sql}` must be refused for a non-super-user");
            assert!(
                error.to_string().contains("operation not permitted"),
                "`{sql}` produced an unexpected error: {error}"
            );
            assert!(validate_query_plan(&plan, true).is_ok(), "super-user: {sql}");
        }
    }

    /// `verify_plan` cannot see through an `Extension` node, so every beacon
    /// extension node needs the separate super-user check — without it, statements
    /// such as `REFRESH` or auth DDL would pass validation for any user.
    #[test]
    fn extension_nodes_are_super_user_only() {
        for sql in ["REFRESH t", "CREATE ROLE reader", "SHOW CRAWLERS"] {
            let plan = beacon_plan(sql);
            // The first gate lets these through: they are not DDL/DML/statements
            // as far as DataFusion is concerned...
            let error = validate_query_plan(&plan, false)
                .expect_err("`{sql}` must be refused for a non-super-user");
            assert!(
                error.to_string().contains("operation not permitted"),
                "`{sql}` produced an unexpected error: {error}"
            );
            assert!(validate_query_plan(&plan, true).is_ok(), "super-user: {sql}");
        }
    }

    /// An extension node nested *below* the plan root (rather than at it) is just
    /// as privileged, so the check must walk the whole tree.
    #[tokio::test]
    async fn nested_extension_nodes_are_detected() {
        let projection = datafusion::logical_expr::LogicalPlanBuilder::from(beacon_plan(
            "SHOW CRAWLERS",
        ))
        .project(vec![datafusion::prelude::col("name")])
        .unwrap()
        .build()
        .unwrap();

        assert!(plan_contains_extension(&projection).unwrap());
        assert!(validate_query_plan(&projection, false).is_err());
        assert!(validate_query_plan(&projection, true).is_ok());
    }

    /// A plain query must remain runnable by ordinary users — the gate exists to
    /// stop privileged statements, not to make the runtime super-user-only.
    #[tokio::test]
    async fn plain_queries_are_allowed_without_super_user() {
        assert!(validate_query_plan(&df_plan("SELECT 1").await, false).is_ok());
    }
}
