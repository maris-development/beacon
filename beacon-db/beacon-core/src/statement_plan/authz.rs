//! Query-time authorization for **reads**: checks the tables/paths a logical plan scans against
//! the caller's roles. For every `TableScan` the caller needs `Select` on the resolved target (a
//! named table or its file paths). Deny-wins, default-deny (see [`beacon_auth`]).
//!
//! DDL/DML privileges are intentionally **not** handled here — those are gated by the super-user
//! check in [`validate_query_plan`](super::validate_query_plan).

use beacon_auth::{AuthContext, AuthIdentity, ConcreteTarget, Privilege};
use beacon_common::super_table::SuperListingTable;
use beacon_datafusion_ext::{
    file_collection::FileCollection,
    table_ext::{ExternalTable, INTERNAL_TABLE_PREFIX},
};
use datafusion::{
    common::tree_node::TreeNodeRecursion,
    datasource::{
        listing::{ListingTable, ListingTableUrl},
        source_as_provider,
    },
    logical_expr::{LogicalPlan, TableScan},
    prelude::SessionContext,
};

/// Authorizes the reads in a logical plan for `identity`. Returns `Ok(())` when allowed.
///
/// No-op when `enforce` is false or the caller is a super-user. Only `TableScan` reads are checked;
/// write/DDL authorization happens via the super-user gate in `validate_query_plan`.
pub(crate) fn authorize_logical_plan(
    plan: &LogicalPlan,
    session_ctx: &SessionContext,
    auth: &AuthContext,
    identity: &AuthIdentity,
    enforce: bool,
) -> anyhow::Result<()> {
    // Unconditional gate on beacon's internal auth tables, checked BEFORE the enforcement/super-user
    // early return below. Those tables hold Argon2 password hashes, and grant enforcement defaults
    // OFF — so a gate that relied on `enforce` would let any user `SELECT * FROM __beacon_users` on a
    // default runtime. Only the super-user may touch them, always. `tests/auth_persistence.rs` pins
    // that this fails closed with enforcement off.
    if !identity.is_super_user && plan_touches_internal_tables(plan) {
        anyhow::bail!(
            "permission denied: the internal '{}*' tables are restricted to the super-user",
            INTERNAL_TABLE_PREFIX
        );
    }

    // Same treatment, same reason, for beacon's metadata schemas. `beacon.system`
    // is the auth directory plus what every user ran; `information_schema` names
    // every table in every catalog. Both describe the instance rather than hold
    // user data, so they belong to the super-user — and, exactly as above, that
    // cannot depend on enforcement being on. Regular callers do not query them:
    // they enumerate the catalog through `Runtime::visible_tables`, which returns
    // only the tables their roles grant.
    if !identity.is_super_user {
        if let Some(schema) = metadata_schema_touched(plan) {
            anyhow::bail!(
                "permission denied: the '{schema}' schema is restricted to the super-user"
            );
        }
    }

    if !enforce || identity.is_super_user {
        return Ok(());
    }

    let mut targets: Vec<ConcreteTarget> = Vec::new();

    // Every table scan anywhere in the plan (including subqueries and write inputs) is a read.
    let _ = plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            targets.extend(scan_targets(scan, session_ctx));
        }
        Ok(TreeNodeRecursion::Continue)
    });

    for target in &targets {
        if !auth.is_allowed(&identity.roles, Privilege::Select, target) {
            anyhow::bail!("permission denied: SELECT on {}", describe_target(target));
        }
    }

    Ok(())
}

/// The name of the first metadata schema (`beacon.system` / `information_schema`)
/// any table scan in `plan` reads, or `None`.
///
/// Matched on the scan's schema so it catches the read however it is reached —
/// through a subquery, a view, or a rewrite (`SHOW TABLES` becomes a scan of
/// `information_schema.tables`).
fn metadata_schema_touched(plan: &LogicalPlan) -> Option<String> {
    let mut found = None;
    let _ = plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            if let Some(schema) = scan.table_name.schema() {
                if crate::system_schema::is_metadata_schema(schema) {
                    found = Some(schema.to_string());
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Whether any table scan in `plan` (including subqueries and write inputs) references one of
/// beacon's reserved `__beacon_*` internal tables. Name-based so it catches the table however it is
/// reached; the write path uses the session directly and never goes through this check.
fn plan_touches_internal_tables(plan: &LogicalPlan) -> bool {
    let mut touches = false;
    let _ = plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            if scan.table_name.table().starts_with(INTERNAL_TABLE_PREFIX) {
                touches = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    touches
}

/// Resolves the concrete resource(s) a table scan touches.
///
/// - Unintrospectable sources are exempt (empty).
/// - A scan of a registered catalog table → `Table(name)` (admins grant by name).
/// - An ad-hoc file scan (a `read_*` UDTF / listing) → `Path` per underlying listing URL.
///
/// The metadata schemas never reach here: a non-super-user's plan that touches
/// one is rejected above, and a super-user's plan returns before any target is
/// resolved.
fn scan_targets(scan: &TableScan, session_ctx: &SessionContext) -> Vec<ConcreteTarget> {
    if session_ctx
        .table_exist(scan.table_name.clone())
        .unwrap_or(false)
    {
        return vec![ConcreteTarget::Table(scan.table_name.table().to_string())];
    }

    let Ok(provider) = source_as_provider(&scan.source) else {
        return vec![];
    };

    // A `read_*` table function resolves to a `SuperListingTable` over its glob paths.
    if let Some(table) = provider.as_any().downcast_ref::<SuperListingTable>() {
        return paths_of(table.table_paths());
    }
    // Multi-glob ad-hoc scans merge their schemas behind a `FileCollection`.
    if let Some(collection) = provider.as_any().downcast_ref::<FileCollection>() {
        return paths_of(collection.table_paths());
    }
    // A single-glob `read_*` (and external tables) resolve to a self-refreshing `ExternalTable`
    // wrapping a `ListingTable`.
    if let Some(external) = provider.as_any().downcast_ref::<ExternalTable>() {
        return paths_of(external.inner().table_paths());
    }
    if let Some(listing) = provider.as_any().downcast_ref::<ListingTable>() {
        return paths_of(listing.table_paths());
    }

    vec![]
}

/// Maps listing URLs to `Path` targets.
fn paths_of(urls: &[ListingTableUrl]) -> Vec<ConcreteTarget> {
    urls.iter()
        .map(|url| ConcreteTarget::Path(listing_url_to_path(url)))
        .collect()
}

/// Reconstructs the datasets-root-relative path (matching `GRANT ... ON PATH`) from a listing URL.
fn listing_url_to_path(url: &ListingTableUrl) -> String {
    let prefix = url.prefix().to_string();
    match url.get_glob() {
        Some(glob) => {
            let glob = glob.as_str();
            if prefix.is_empty() {
                glob.to_string()
            } else {
                format!("{prefix}/{glob}")
            }
        }
        None => prefix,
    }
}

fn describe_target(target: &ConcreteTarget) -> String {
    match target {
        ConcreteTarget::Table(name) => format!("table '{name}'"),
        ConcreteTarget::Path(path) => format!("path '{path}'"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use beacon_auth::{AuthContext, AuthIdentity, BasicAuthProvider, Privilege, PrivilegeRule};
    use datafusion::{
        arrow::{
            array::Int32Array,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        },
        datasource::MemTable,
        prelude::{SessionConfig, SessionContext},
    };

    use super::*;

    fn identity(roles: &[&str]) -> AuthIdentity {
        AuthIdentity {
            username: "alice".to_string(),
            roles: roles.iter().map(|r| r.to_string()).collect(),
            is_super_user: false,
        }
    }

    async fn ctx_with_table(name: &str) -> SessionContext {
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table(name, Arc::new(table)).unwrap();
        ctx
    }

    async fn plan_for(ctx: &SessionContext, sql: &str) -> LogicalPlan {
        ctx.sql(sql).await.unwrap().into_unoptimized_plan()
    }

    async fn auth_with_reader_grant(grant: Option<PrivilegeRule>) -> AuthContext {
        let auth = AuthContext::new(Arc::new(BasicAuthProvider::new()));
        auth.create_role("reader").await.unwrap();
        if let Some(rule) = grant {
            auth.grant("reader", rule).await.unwrap();
        }
        auth
    }

    #[tokio::test]
    async fn named_table_denied_without_grant_allowed_with_grant() {
        let ctx = ctx_with_table("observations").await;
        let plan = plan_for(&ctx, "SELECT * FROM observations").await;

        let denied = auth_with_reader_grant(None).await;
        assert!(authorize_logical_plan(&plan, &ctx, &denied, &identity(&["reader"]), true).is_err());

        let allowed = auth_with_reader_grant(Some(PrivilegeRule::new(Privilege::Select, None))).await;
        assert!(authorize_logical_plan(&plan, &ctx, &allowed, &identity(&["reader"]), true).is_ok());
    }

    #[tokio::test]
    async fn enforce_off_and_super_user_bypass() {
        let ctx = ctx_with_table("observations").await;
        let plan = plan_for(&ctx, "SELECT * FROM observations").await;
        let auth = auth_with_reader_grant(None).await;

        // enforce=false bypasses.
        assert!(authorize_logical_plan(&plan, &ctx, &auth, &identity(&["reader"]), false).is_ok());

        // super-user bypasses even with enforce=true and no grants.
        let mut su = identity(&[]);
        su.is_super_user = true;
        assert!(authorize_logical_plan(&plan, &ctx, &auth, &su, true).is_ok());
    }

    #[tokio::test]
    async fn metadata_schemas_are_super_user_only_even_without_enforcement() {
        let ctx = ctx_with_table("observations").await;
        let auth = auth_with_reader_grant(Some(PrivilegeRule::new(Privilege::Select, None))).await;

        for sql in [
            "SELECT table_name FROM information_schema.tables",
            // `SHOW TABLES` is rewritten onto `information_schema.tables`, so the
            // scan-level gate catches it too.
            "SHOW TABLES",
            "SELECT count(*) FROM (SELECT * FROM information_schema.columns)",
        ] {
            let plan = plan_for(&ctx, sql).await;
            // Denied with enforcement on despite a blanket SELECT grant…
            let err = authorize_logical_plan(&plan, &ctx, &auth, &identity(&["reader"]), true)
                .err()
                .unwrap_or_else(|| panic!("non-super read should be rejected: {sql}"));
            assert!(
                err.to_string().contains("super-user"),
                "expected a super-user error for `{sql}`, got: {err}"
            );
            // …and denied with enforcement off, where a grant-based gate would leak.
            assert!(
                authorize_logical_plan(&plan, &ctx, &auth, &identity(&["reader"]), false).is_err(),
                "`{sql}` must stay denied with enforcement off"
            );
        }

        // The super-user still reads them.
        let mut su = identity(&[]);
        su.is_super_user = true;
        let plan = plan_for(&ctx, "SELECT table_name FROM information_schema.tables").await;
        assert!(authorize_logical_plan(&plan, &ctx, &auth, &su, true).is_ok());
    }
}
