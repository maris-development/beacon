//! Query-time authorization for **reads**: checks the tables/paths a logical plan scans against the
//! caller's roles. For every `TableScan` the caller needs `Select` on the resolved target (a named
//! table or its file paths). Deny-wins, default-deny (see [`beacon_auth`]).
//!
//! DDL/DML privileges are intentionally **not** handled here — `beacon-core` intercepts those
//! statements and authorizes them where it executes them.

use beacon_auth::{AuthContext, AuthIdentity, ConcreteTarget, Privilege};
use beacon_data_lake::files::collection::FileCollection;
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
/// write/DDL authorization happens in `beacon-core` at statement interception.
pub fn authorize_logical_plan(
    plan: &LogicalPlan,
    session_ctx: &SessionContext,
    auth: &AuthContext,
    identity: &AuthIdentity,
    enforce: bool,
) -> anyhow::Result<()> {
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

/// Resolves the concrete resource(s) a table scan touches.
///
/// - System schemas (`information_schema`) and unintrospectable sources are exempt (empty).
/// - A scan of a registered catalog table → `Table(name)` (admins grant by name).
/// - An ad-hoc file scan (`From::Format`, a `read_*` UDTF) → `Path` per underlying listing URL.
fn scan_targets(scan: &TableScan, session_ctx: &SessionContext) -> Vec<ConcreteTarget> {
    if let Some(schema) = scan.table_name.schema() {
        if schema.eq_ignore_ascii_case("information_schema") {
            return vec![];
        }
    }

    if session_ctx
        .table_exist(scan.table_name.clone())
        .unwrap_or(false)
    {
        return vec![ConcreteTarget::Table(scan.table_name.table().to_string())];
    }

    let Ok(provider) = source_as_provider(&scan.source) else {
        return vec![];
    };

    if let Some(collection) = provider.as_any().downcast_ref::<FileCollection>() {
        return collection
            .table_paths()
            .iter()
            .map(|url| ConcreteTarget::Path(listing_url_to_path(url)))
            .collect();
    }
    if let Some(listing) = provider.as_any().downcast_ref::<ListingTable>() {
        return listing
            .table_paths()
            .iter()
            .map(|url| ConcreteTarget::Path(listing_url_to_path(url)))
            .collect();
    }

    vec![]
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
    use beacon_common::listing_url::parse_listing_table_url;
    use datafusion::{
        arrow::{array::Int32Array, datatypes::{DataType, Field, Schema}, record_batch::RecordBatch},
        datasource::MemTable,
        execution::object_store::ObjectStoreUrl,
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
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
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

    fn auth_with_reader_grant(grant: Option<PrivilegeRule>) -> AuthContext {
        let auth = AuthContext::new(Arc::new(BasicAuthProvider::new()));
        auth.create_role("reader").unwrap();
        if let Some(rule) = grant {
            auth.grant("reader", rule).unwrap();
        }
        auth
    }

    #[tokio::test]
    async fn named_table_denied_without_grant_allowed_with_grant() {
        let ctx = ctx_with_table("observations").await;
        let plan = plan_for(&ctx, "SELECT * FROM observations").await;

        let denied = auth_with_reader_grant(None);
        assert!(authorize_logical_plan(&plan, &ctx, &denied, &identity(&["reader"]), true).is_err());

        let allowed = auth_with_reader_grant(Some(PrivilegeRule::new(Privilege::Select, None)));
        assert!(authorize_logical_plan(&plan, &ctx, &allowed, &identity(&["reader"]), true).is_ok());
    }

    #[tokio::test]
    async fn enforce_off_and_super_user_bypass() {
        let ctx = ctx_with_table("observations").await;
        let plan = plan_for(&ctx, "SELECT * FROM observations").await;
        let auth = auth_with_reader_grant(None);

        // enforce=false bypasses.
        assert!(authorize_logical_plan(&plan, &ctx, &auth, &identity(&["reader"]), false).is_ok());

        // super-user bypasses even with enforce=true and no grants.
        let mut su = identity(&[]);
        su.is_super_user = true;
        assert!(authorize_logical_plan(&plan, &ctx, &auth, &su, true).is_ok());
    }

    #[tokio::test]
    async fn information_schema_is_exempt() {
        let ctx = ctx_with_table("observations").await;
        let plan = plan_for(&ctx, "SELECT table_name FROM information_schema.tables").await;
        let auth = auth_with_reader_grant(None);
        assert!(authorize_logical_plan(&plan, &ctx, &auth, &identity(&["reader"]), true).is_ok());
    }

    #[test]
    fn listing_url_normalizes_to_grant_relative_path() {
        let store = ObjectStoreUrl::parse("datasets://").unwrap();
        let url = parse_listing_table_url(&store, "example_2/*").unwrap();
        assert_eq!(listing_url_to_path(&url), "example_2/*");

        let nested = parse_listing_table_url(&store, "argo/pub/**/*.nc").unwrap();
        assert_eq!(listing_url_to_path(&nested), "argo/pub/**/*.nc");

        let concrete = parse_listing_table_url(&store, "example/file.parquet").unwrap();
        assert_eq!(listing_url_to_path(&concrete), "example/file.parquet");
    }
}
