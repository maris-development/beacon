//! Query-time authorization for **reads**: checks the tables and files a logical plan scans against
//! the caller's roles. Deny-wins, default-deny (see [`beacon_auth`]).
//!
//! A read is checked on what it **reaches**, not on how the query spells it. A listing lets `*`
//! cross `/`, so the text of a glob says little about its files: the check lists the objects of
//! each scan and matches every one against the rules.
//!
//! - A registered table needs `Select` on its name, and no path deny may match one of its files.
//! - An ad-hoc file read needs `Select` on every file, and no deny on a table that holds one of
//!   them may apply.
//! - A source whose files the check cannot see is refused.
//!
//! DDL/DML privileges are intentionally **not** handled here — those are gated by the super-user
//! check in [`validate_query_plan`](super::validate_query_plan).

use beacon_auth::{AuthContext, AuthIdentity, ConcreteTarget, Privilege, PrivilegeTarget};
use beacon_datafusion_ext::{
    fast_object::FastObjectTable,
    listing_factory::ListingFactory,
    located_table::LocatedTable,
    table_ext::{ExternalTable, INTERNAL_TABLE_PREFIX},
};
use datafusion::{
    catalog::TableProvider,
    common::tree_node::TreeNodeRecursion,
    datasource::{
        listing::{ListingTable, ListingTableUrl},
        source_as_provider,
    },
    functions_table::generate_series::GenerateSeriesTable,
    logical_expr::{LogicalPlan, TableScan},
    prelude::SessionContext,
};
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;

/// Authorizes the reads in a logical plan for `identity`. Returns `Ok(())` when allowed.
///
/// No-op when `enforce` is false or the caller is a super-user. Only `TableScan` reads are checked;
/// write/DDL authorization happens via the super-user gate in `validate_query_plan`.
pub(crate) async fn authorize_logical_plan(
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
        if let Some(surface) = metadata_surface_touched(plan) {
            anyhow::bail!("permission denied: {surface} is restricted to the super-user");
        }
    }

    if !enforce || identity.is_super_user {
        return Ok(());
    }

    // `DESCRIBE <table>` plans no scan and keeps no table name, so no grant can be checked.
    if plan_describes(plan) {
        anyhow::bail!(
            "permission denied: DESCRIBE is restricted to the super-user when authorization is \
             enforced; read a table's schema with GET /api/table-schema"
        );
    }

    // Every table scan anywhere in the plan (including subqueries and write inputs) is a read.
    let mut reads: Vec<ScanRead> = Vec::new();
    let _ = plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            reads.push(scan_read(scan, session_ctx));
        }
        Ok(TreeNodeRecursion::Continue)
    });

    let checker = ReadChecker::new(session_ctx, auth, identity);
    for read in &reads {
        checker.check(read).await?;
    }
    Ok(())
}

/// Authorizes a read of the registered table `name`, served by `provider`, for `identity`.
///
/// The table-level half of [`authorize_logical_plan`], for a caller that reads a table without a
/// plan, such as a schema request. The caller has already applied the super-user gates.
pub(crate) async fn authorize_table_read(
    name: &str,
    provider: &dyn TableProvider,
    session_ctx: &SessionContext,
    auth: &AuthContext,
    identity: &AuthIdentity,
) -> anyhow::Result<()> {
    let read = ScanRead::Table {
        name: name.to_string(),
        locations: provider_locations(provider).unwrap_or_default(),
    };
    ReadChecker::new(session_ctx, auth, identity).check(&read).await
}

/// Whether any node of `plan` is a `DESCRIBE <table>`.
fn plan_describes(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply_with_subqueries(|node| {
        found = matches!(node, LogicalPlan::DescribeTable(_));
        Ok(if found { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
    });
    found
}

/// What one table scan reads, as the check sees it.
enum ScanRead {
    /// A registered table, with the locations of its files (empty when it has no files).
    Table { name: String, locations: Vec<Location> },
    /// An ad-hoc read of files: a `read_*` table function or a JSON-query file source.
    Files(Vec<Location>),
    /// A source whose files the check cannot see, by the name the plan gives it.
    Unknown(String),
}

/// Where the files of a scan are.
enum Location {
    /// A listing URL and the file extension its table filters on, listed as the scan lists it.
    Listing { url: ListingTableUrl, extension: String },
    /// A path the runtime's listing factory lists: a table directory (with a trailing `/`) or
    /// the glob of `list_datasets`.
    Pattern(String),
}

impl Location {
    /// The path as the query spells it, relative to the datasets root.
    fn spelling(&self) -> String {
        match self {
            Location::Listing { url, .. } => listing_url_to_path(url),
            Location::Pattern(pattern) => pattern.trim_end_matches('/').to_string(),
        }
    }
}

/// Resolves what a table scan reads.
fn scan_read(scan: &TableScan, session_ctx: &SessionContext) -> ScanRead {
    let provider = source_as_provider(&scan.source).ok();
    let locations = provider
        .as_ref()
        .and_then(|provider| provider_locations(provider.as_ref()));

    if session_ctx
        .table_exist(scan.table_name.clone())
        .unwrap_or(false)
    {
        return ScanRead::Table {
            name: scan.table_name.table().to_string(),
            locations: locations.unwrap_or_default(),
        };
    }
    match locations {
        Some(locations) => ScanRead::Files(locations),
        None => ScanRead::Unknown(scan.table_name.to_string()),
    }
}

/// The file locations `provider` reads, or `None` when the check cannot see them.
///
/// `Some(vec![])` is a provider that reads no files, such as `generate_series`.
fn provider_locations(provider: &dyn TableProvider) -> Option<Vec<Location>> {
    let any = provider.as_any();
    if let Some(table) = any.downcast_ref::<FastObjectTable>() {
        return Some(listing_locations(table.inner()));
    }
    if let Some(external) = any.downcast_ref::<ExternalTable>() {
        return Some(listing_locations(external.inner().inner()));
    }
    if let Some(listing) = any.downcast_ref::<ListingTable>() {
        return Some(listing_locations(listing));
    }
    // The table formats keep their files under one directory: every object there is theirs.
    if let Some(located) = any.downcast_ref::<LocatedTable>() {
        return Some(vec![directory(located.location())]);
    }
    if let Some(delta) = any.downcast_ref::<beacon_delta::BeaconDeltaTable>() {
        return Some(vec![directory(&delta.definition().location)]);
    }
    if let Some(iceberg) = any.downcast_ref::<beacon_iceberg::BeaconIcebergTable>() {
        return Some(vec![directory(&iceberg.definition().location)]);
    }
    if let Some(icechunk) = any.downcast_ref::<beacon_icechunk::IcechunkTable>() {
        return Some(vec![directory(&icechunk.definition().location)]);
    }
    if let Some(datasets) = any.downcast_ref::<beacon_functions::listing::provider::DatasetsTable>()
    {
        return Some(vec![Location::Pattern(datasets.pattern().to_string())]);
    }
    // A schema describes the files its reader would read, so it needs the same grant.
    if let Some(schema) =
        any.downcast_ref::<beacon_functions::file_formats::schema_function::ReaderSchemaTable>()
    {
        return provider_locations(schema.source().as_ref());
    }
    if any.is::<GenerateSeriesTable>() {
        return Some(Vec::new());
    }
    None
}

fn listing_locations(table: &ListingTable) -> Vec<Location> {
    let extension = table.options().file_extension.clone();
    table
        .table_paths()
        .iter()
        .map(|url| Location::Listing {
            url: url.clone(),
            extension: extension.clone(),
        })
        .collect()
}

/// A table directory as a listing pattern: every object below it.
fn directory(location: &str) -> Location {
    Location::Pattern(format!("{}/", location.trim_end_matches('/')))
}

/// Checks the reads of one caller.
struct ReadChecker<'a> {
    session_ctx: &'a SessionContext,
    auth: &'a AuthContext,
    identity: &'a AuthIdentity,
    /// Whether a role of the caller denies a path, so a table's files need a look.
    denies_paths: bool,
    /// The tables a role of the caller denies, whose files a path read must not reach.
    denied_tables: Vec<String>,
}

impl<'a> ReadChecker<'a> {
    fn new(session_ctx: &'a SessionContext, auth: &'a AuthContext, identity: &'a AuthIdentity) -> Self {
        let denies: Vec<_> = auth
            .list_roles()
            .into_iter()
            .filter(|role| identity.roles.contains(&role.name))
            .flat_map(|role| role.denies.into_iter())
            .filter(|rule| matches!(rule.privilege, Privilege::Select | Privilege::All))
            .collect();
        let denies_paths = denies
            .iter()
            .any(|rule| matches!(rule.target, Some(PrivilegeTarget::Path(_))));
        let denied_tables = denies
            .iter()
            .filter_map(|rule| match &rule.target {
                Some(PrivilegeTarget::Table(name)) => Some(name.clone()),
                _ => None,
            })
            .collect();
        Self {
            session_ctx,
            auth,
            identity,
            denies_paths,
            denied_tables,
        }
    }

    async fn check(&self, read: &ScanRead) -> anyhow::Result<()> {
        match read {
            ScanRead::Unknown(name) => anyhow::bail!(
                "permission denied: SELECT on '{name}': its files cannot be checked"
            ),
            ScanRead::Table { name, locations } => {
                self.require(&ConcreteTarget::Table(name.clone()))?;
                if self.denies_paths {
                    for file in self.files(locations).await? {
                        self.refuse_denied(&ConcreteTarget::Path(file.to_string()))?;
                    }
                }
                Ok(())
            }
            ScanRead::Files(locations) => {
                // The spelling can name a file the disk resolves in another case.
                for location in locations {
                    self.refuse_denied(&ConcreteTarget::Path(location.spelling()))?;
                }
                let files = self.files(locations).await?;
                for file in &files {
                    self.require(&ConcreteTarget::Path(file.to_string()))?;
                }
                self.refuse_denied_tables(&files).await
            }
        }
    }

    /// Fails unless the caller may read `target`.
    fn require(&self, target: &ConcreteTarget) -> anyhow::Result<()> {
        if self.auth.is_allowed(&self.identity.roles, Privilege::Select, target) {
            Ok(())
        } else {
            anyhow::bail!("permission denied: SELECT on {}", describe_target(target))
        }
    }

    /// Fails when a deny of the caller matches `target`.
    fn refuse_denied(&self, target: &ConcreteTarget) -> anyhow::Result<()> {
        if self.auth.is_denied(&self.identity.roles, Privilege::Select, target) {
            anyhow::bail!("permission denied: SELECT on {}", describe_target(target))
        }
        Ok(())
    }

    /// Fails when one of `files` belongs to a table a role of the caller denies.
    async fn refuse_denied_tables(&self, files: &[ObjectPath]) -> anyhow::Result<()> {
        for table in &self.denied_tables {
            let Ok(provider) = self.session_ctx.table_provider(table.as_str()).await else {
                continue;
            };
            let Some(locations) = provider_locations(provider.as_ref()) else {
                continue;
            };
            let covered = files.iter().any(|file| {
                locations.iter().any(|location| match location {
                    Location::Listing { url, .. } => url.contains(file, false),
                    Location::Pattern(pattern) => file.as_ref().starts_with(pattern.as_str()),
                })
            });
            if covered {
                anyhow::bail!("permission denied: SELECT on table '{table}'");
            }
        }
        Ok(())
    }

    /// Every object the locations reach, listed the way the scan lists them.
    async fn files(&self, locations: &[Location]) -> anyhow::Result<Vec<ObjectPath>> {
        let state = self.session_ctx.state();
        let mut files = Vec::new();
        for location in locations {
            match location {
                Location::Listing { url, extension } => {
                    let store = state.runtime_env().object_store(url.object_store())?;
                    let objects: Vec<_> = url
                        .list_all_files(&state, store.as_ref(), extension)
                        .await?
                        .try_collect()
                        .await?;
                    files.extend(objects.into_iter().map(|object| object.location));
                }
                Location::Pattern(pattern) => {
                    let factory = state.config().get_extension::<ListingFactory>().ok_or_else(
                        || anyhow::anyhow!("the listing factory is not registered on the session"),
                    )?;
                    let pattern = strip_default_scheme(&factory, pattern);
                    let objects: Vec<_> = factory
                        .listing(&state, &pattern)?
                        .stream()
                        .try_collect()
                        .await?;
                    files.extend(objects.into_iter().map(|object| object.location));
                }
            }
        }
        Ok(files)
    }
}

/// `pattern` without the scheme of the default store, which a table location can carry
/// (`datasets://argo`) but the listing factory refuses next to its own default.
fn strip_default_scheme(factory: &ListingFactory, pattern: &str) -> String {
    let Some(scheme) = factory
        .default_store_url()
        .and_then(|url| url.as_str().split("://").next().map(str::to_string))
    else {
        return pattern.to_string();
    };
    match pattern.strip_prefix(&format!("{scheme}://")) {
        Some(relative) => relative.trim_start_matches('/').to_string(),
        None => pattern.to_string(),
    }
}

/// The name of the first metadata schema (`beacon.system` / `information_schema`)
/// any table scan in `plan` reads, as a noun phrase for the error, or `None`.
///
/// Matched on the scan's schema so it catches the read however it is reached —
/// through a subquery, a view, or a rewrite (`SHOW TABLES` becomes a scan of
/// `information_schema.tables`) — and on the provider for the one metadata
/// surface that has no schema name, the `file_statistics` table function.
fn metadata_surface_touched(plan: &LogicalPlan) -> Option<String> {
    let mut found = None;
    let _ = plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            if let Some(schema) = scan.table_name.schema() {
                if crate::system_schema::is_metadata_schema(schema) {
                    found = Some(format!("the '{schema}' schema"));
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            // `file_statistics(...)` is the same metadata, reached through a
            // table function. A function has no schema in the plan — its scan is
            // named after the function itself — so the check above cannot see it
            // and the provider is matched instead.
            if source_as_provider(&scan.source).is_ok_and(|provider| {
                provider
                    .as_any()
                    .is::<crate::system_schema::FileStatisticsTable>()
            }) {
                found = Some(format!(
                    "the {} table function",
                    crate::system_schema::FILE_STATISTICS_FUNCTION
                ));
                return Ok(TreeNodeRecursion::Stop);
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
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();
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
        assert!(
            authorize_logical_plan(&plan, &ctx, &denied, &identity(&["reader"]), true).await.is_err()
        );

        let allowed =
            auth_with_reader_grant(Some(PrivilegeRule::new(Privilege::Select, None))).await;
        assert!(
            authorize_logical_plan(&plan, &ctx, &allowed, &identity(&["reader"]), true).await.is_ok()
        );
    }

    #[tokio::test]
    async fn enforce_off_and_super_user_bypass() {
        let ctx = ctx_with_table("observations").await;
        let plan = plan_for(&ctx, "SELECT * FROM observations").await;
        let auth = auth_with_reader_grant(None).await;

        // enforce=false bypasses.
        assert!(authorize_logical_plan(&plan, &ctx, &auth, &identity(&["reader"]), false).await.is_ok());

        // super-user bypasses even with enforce=true and no grants.
        let mut su = identity(&[]);
        su.is_super_user = true;
        assert!(authorize_logical_plan(&plan, &ctx, &auth, &su, true).await.is_ok());
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
            let err = authorize_logical_plan(&plan, &ctx, &auth, &identity(&["reader"]), true).await
                .err()
                .unwrap_or_else(|| panic!("non-super read should be rejected: {sql}"));
            assert!(
                err.to_string().contains("super-user"),
                "expected a super-user error for `{sql}`, got: {err}"
            );
            // …and denied with enforcement off, where a grant-based gate would leak.
            assert!(
                authorize_logical_plan(&plan, &ctx, &auth, &identity(&["reader"]), false).await.is_err(),
                "`{sql}` must stay denied with enforcement off"
            );
        }

        // The super-user still reads them.
        let mut su = identity(&[]);
        su.is_super_user = true;
        let plan = plan_for(&ctx, "SELECT table_name FROM information_schema.tables").await;
        assert!(authorize_logical_plan(&plan, &ctx, &auth, &su, true).await.is_ok());
    }
}
