//! The facts of a planned statement that an access decision needs.
//!
//! A read is described by what it **reaches**, not by how the query spells it. A listing lets `*`
//! cross `/`, so the text of a glob says little about its files. [`PlanFacts`] therefore gives the
//! tables a plan scans, and it lists the objects of each file scan on request.
//!
//! This module holds no rule. A [`PlanValidator`](crate::plan_validator::PlanValidator) reads the
//! facts and decides.

use beacon_datafusion_ext::{
    fast_object::FastObjectTable, listing_factory::ListingFactory, located_table::LocatedTable,
    table_ext::INTERNAL_TABLE_PREFIX,
};
use beacon_external_table::ExternalTable;
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
    sql::TableReference,
};
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;

/// The facts of one planned statement.
///
/// The lifetime `'a` ties the facts to the plan and the session they describe. In Python terms:
/// this is a view object, and it cannot outlive the plan it reads.
pub struct PlanFacts<'a> {
    plan: &'a LogicalPlan,
    session_ctx: &'a SessionContext,
}

impl<'a> PlanFacts<'a> {
    /// Collects the facts of `plan`, resolved against `session_ctx`.
    pub fn new(plan: &'a LogicalPlan, session_ctx: &'a SessionContext) -> Self {
        Self { plan, session_ctx }
    }

    /// The planned statement.
    pub fn plan(&self) -> &LogicalPlan {
        self.plan
    }

    /// Checks that the statement is one that a caller without privileges can run.
    ///
    /// # Errors
    ///
    /// Returns an error when the plan holds DDL, DML, `COPY`, a `SET` statement or a Beacon
    /// extension node.
    pub fn verify_unprivileged(&self) -> anyhow::Result<()> {
        super::validate_query_plan(self.plan, false)
    }

    /// Whether a table scan of the plan reads one of the reserved `__beacon_*` tables.
    ///
    /// The check is on the table name, so it finds the table through a subquery or a write input.
    pub fn touches_internal_tables(&self) -> bool {
        let mut touches = false;
        let _ = self.plan.apply_with_subqueries(|node| {
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

    /// The first metadata surface that the plan reads, as a noun phrase for an error message.
    ///
    /// A metadata surface is `beacon.system`, `information_schema` or the `table_statistics` table
    /// function. The check is on the schema of each scan, so it finds the read through a subquery,
    /// a view or a rewrite: `SHOW TABLES` becomes a scan of `information_schema.tables`.
    pub fn metadata_surface(&self) -> Option<String> {
        let mut found = None;
        let _ = self.plan.apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                if let Some(schema) = scan.table_name.schema() {
                    if crate::system_schema::is_metadata_schema(schema) {
                        found = Some(format!("the '{schema}' schema"));
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
                // A table function has no schema in the plan, so the provider is matched.
                if source_as_provider(&scan.source).is_ok_and(|provider| {
                    provider.is::<crate::system_schema::TableStatisticsTable>()
                }) {
                    found = Some(format!(
                        "the {} table function",
                        crate::system_schema::TABLE_STATISTICS_FUNCTION
                    ));
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        found
    }

    /// Whether a node of the plan is a `DESCRIBE <table>`.
    ///
    /// Such a node plans no scan and keeps no table name, so [`Self::reads`] cannot report it.
    pub fn describes_table(&self) -> bool {
        let mut found = false;
        let _ = self.plan.apply_with_subqueries(|node| {
            found = matches!(node, LogicalPlan::DescribeTable(_));
            Ok(if found {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        });
        found
    }

    /// Every read of the plan: one entry for each table scan, with subqueries and write inputs.
    pub fn reads(&self) -> Vec<PlanRead> {
        let mut reads = Vec::new();
        let _ = self.plan.apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                reads.push(scan_read(scan, self.session_ctx));
            }
            Ok(TreeNodeRecursion::Continue)
        });
        reads
    }

    /// Every object that `locations` reach, listed the way the scan lists them.
    ///
    /// This call does I/O. Call it only when a rule needs the file names.
    ///
    /// # Errors
    ///
    /// Returns an error when an object store is not registered or a listing fails.
    pub async fn files(&self, locations: &[FileLocation]) -> anyhow::Result<Vec<ObjectPath>> {
        let state = self.session_ctx.state();
        let mut files = Vec::new();
        for location in locations {
            match location {
                FileLocation::Listing { url, extension } => {
                    let store = state.runtime_env().object_store(url.object_store())?;
                    let objects: Vec<_> = url
                        .list_all_files(&state, store.as_ref(), extension)
                        .await?
                        .try_collect()
                        .await?;
                    files.extend(objects.into_iter().map(|object| object.location));
                }
                FileLocation::Pattern(pattern) => {
                    let factory = state
                        .config()
                        .get_extension::<ListingFactory>()
                        .ok_or_else(|| {
                            anyhow::anyhow!("the listing factory is not registered on the session")
                        })?;
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

    /// Whether one of `files` belongs to the registered table `table`.
    ///
    /// Returns `false` for a table that does not exist or whose files the engine cannot see.
    pub async fn table_holds_any(&self, table: &str, files: &[ObjectPath]) -> bool {
        let Ok(provider) = self.session_ctx.table_provider(table).await else {
            return false;
        };
        let Some(locations) = provider_locations(provider.as_ref()) else {
            return false;
        };
        files.iter().any(|file| {
            locations.iter().any(|location| match location {
                FileLocation::Listing { url, .. } => url.contains(file, false),
                FileLocation::Pattern(pattern) => file.as_ref().starts_with(pattern.as_str()),
            })
        })
    }
}

/// A table as an access rule can name it.
#[derive(Debug, Clone, PartialEq)]
pub struct TableFacts {
    reference: TableReference,
    names: Vec<String>,
}

impl TableFacts {
    /// Resolves the spellings of `reference` against the default catalog and schema.
    pub fn new(reference: TableReference, session_ctx: &SessionContext) -> Self {
        let names = table_names(&reference, session_ctx);
        Self { reference, names }
    }

    /// The reference as the plan or the caller wrote it.
    pub fn reference(&self) -> &TableReference {
        &self.reference
    }

    /// The table name without catalog and schema.
    pub fn name(&self) -> &str {
        self.reference.table()
    }

    /// Every spelling that a rule can use: `catalog.schema.table` always, `schema.table` in the
    /// default catalog, and `table` in the default schema.
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// Whether the table is one of the reserved `__beacon_*` tables.
    pub fn is_internal(&self) -> bool {
        self.name().starts_with(INTERNAL_TABLE_PREFIX)
    }

    /// The schema of the reference, when it is `beacon.system` or `information_schema`.
    pub fn metadata_schema(&self) -> Option<&str> {
        self.reference
            .schema()
            .filter(|schema| crate::system_schema::is_metadata_schema(schema))
    }
}

/// What one table scan reads.
#[derive(Debug, Clone)]
pub enum PlanRead {
    /// A registered table.
    Table(TableFacts),
    /// An ad-hoc read of files: a `read_*` table function or a JSON-query file source.
    Files(Vec<FileLocation>),
    /// A source whose files the engine cannot see, by the name the plan gives it.
    Opaque(String),
}

/// Where the files of a scan are.
#[derive(Debug, Clone)]
pub enum FileLocation {
    /// A listing URL and the file extension its table filters on, listed as the scan lists it.
    Listing {
        /// The listing URL of the scan. In a `Box`, so the enum stays small.
        url: Box<ListingTableUrl>,
        /// The file extension that the listing keeps.
        extension: String,
    },
    /// A path the runtime's listing factory lists: a table directory (with a trailing `/`) or
    /// the glob of `list_datasets`.
    Pattern(String),
}

impl FileLocation {
    /// The path as the query spells it, relative to the datasets root.
    pub fn spelling(&self) -> String {
        match self {
            FileLocation::Listing { url, .. } => listing_url_to_path(url),
            FileLocation::Pattern(pattern) => pattern.trim_end_matches('/').to_string(),
        }
    }
}

/// Every spelling of `reference` that a table rule can use.
///
/// A rule stores the name as the `GRANT` wrote it, so a check tries each spelling.
fn table_names(reference: &TableReference, session_ctx: &SessionContext) -> Vec<String> {
    let options = session_ctx.copied_config().options().catalog.clone();
    let resolved = reference
        .clone()
        .resolve(&options.default_catalog, &options.default_schema);
    let (catalog, schema, table) = (&*resolved.catalog, &*resolved.schema, &*resolved.table);

    let mut names = vec![format!("{catalog}.{schema}.{table}")];
    if catalog == options.default_catalog {
        names.push(format!("{schema}.{table}"));
        if schema == options.default_schema {
            names.push(table.to_string());
        }
    }
    names
}

/// Resolves what a table scan reads.
fn scan_read(scan: &TableScan, session_ctx: &SessionContext) -> PlanRead {
    // A table read needs the table grant only; path rules apply to reads of files.
    if session_ctx
        .table_exist(scan.table_name.clone())
        .unwrap_or(false)
    {
        return PlanRead::Table(TableFacts::new(scan.table_name.clone(), session_ctx));
    }
    let locations = source_as_provider(&scan.source)
        .ok()
        .and_then(|provider| provider_locations(provider.as_ref()));
    match locations {
        Some(locations) => PlanRead::Files(locations),
        None => PlanRead::Opaque(scan.table_name.to_string()),
    }
}

/// The file locations `provider` reads, or `None` when the engine cannot see them.
///
/// `Some(vec![])` is a provider that reads no files, such as `generate_series`.
fn provider_locations(provider: &dyn TableProvider) -> Option<Vec<FileLocation>> {
    if let Some(table) = provider.downcast_ref::<FastObjectTable>() {
        return Some(listing_locations(table.inner()));
    }
    if let Some(external) = provider.downcast_ref::<ExternalTable>() {
        return Some(listing_locations(&external.listing_table()));
    }
    if let Some(listing) = provider.downcast_ref::<ListingTable>() {
        return Some(listing_locations(listing));
    }
    // The table formats keep their files under one directory: every object there is theirs.
    if let Some(located) = provider.downcast_ref::<LocatedTable>() {
        return Some(vec![directory(located.location())]);
    }
    if let Some(delta) = provider.downcast_ref::<beacon_delta::BeaconDeltaTable>() {
        return Some(vec![directory(&delta.definition().location)]);
    }
    if let Some(iceberg) = provider.downcast_ref::<beacon_iceberg::BeaconIcebergTable>() {
        return Some(vec![directory(&iceberg.definition().location)]);
    }
    if let Some(icechunk) = provider.downcast_ref::<beacon_icechunk::IcechunkTable>() {
        return Some(vec![directory(&icechunk.definition().location)]);
    }
    if let Some(datasets) =
        provider.downcast_ref::<beacon_functions::listing::provider::DatasetsTable>()
    {
        return Some(vec![FileLocation::Pattern(datasets.pattern().to_string())]);
    }
    // A schema describes the files its reader would read, so it needs the same grant.
    if let Some(schema) = provider
        .downcast_ref::<beacon_functions::file_formats::schema_function::ReaderSchemaTable>()
    {
        return provider_locations(schema.source().as_ref());
    }
    if provider.is::<GenerateSeriesTable>() {
        return Some(Vec::new());
    }
    None
}

fn listing_locations(table: &ListingTable) -> Vec<FileLocation> {
    let extension = table.options().file_extension.clone();
    table
        .table_paths()
        .iter()
        .map(|url| FileLocation::Listing {
            url: Box::new(url.clone()),
            extension: extension.clone(),
        })
        .collect()
}

/// A table directory as a listing pattern: every object below it.
fn directory(location: &str) -> FileLocation {
    FileLocation::Pattern(format!("{}/", location.trim_end_matches('/')))
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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

    fn ctx_with_table(name: &str) -> SessionContext {
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

    #[tokio::test]
    async fn a_named_table_is_one_table_read_with_every_spelling() {
        let ctx = ctx_with_table("observations");
        let plan = plan_for(&ctx, "SELECT * FROM observations").await;

        let reads = PlanFacts::new(&plan, &ctx).reads();

        let [PlanRead::Table(table)] = reads.as_slice() else {
            panic!("expected one table read, got {reads:?}");
        };
        assert_eq!(table.name(), "observations");
        assert_eq!(
            table.names(),
            [
                "datafusion.public.observations",
                "public.observations",
                "observations"
            ]
        );
        assert!(!table.is_internal());
        assert_eq!(table.metadata_schema(), None);
    }

    #[tokio::test]
    async fn a_subquery_read_is_reported() {
        let ctx = ctx_with_table("observations");
        let plan = plan_for(
            &ctx,
            "SELECT 1 WHERE EXISTS (SELECT a FROM observations WHERE a > 0)",
        )
        .await;

        let reads = PlanFacts::new(&plan, &ctx).reads();

        assert!(reads
            .iter()
            .any(|read| matches!(read, PlanRead::Table(table) if table.name() == "observations")));
    }

    #[tokio::test]
    async fn a_metadata_schema_read_is_a_metadata_surface() {
        let ctx = ctx_with_table("observations");

        let show = plan_for(&ctx, "SHOW TABLES").await;
        let select = plan_for(&ctx, "SELECT * FROM observations").await;

        assert_eq!(
            PlanFacts::new(&show, &ctx).metadata_surface().as_deref(),
            Some("the 'information_schema' schema")
        );
        assert_eq!(PlanFacts::new(&select, &ctx).metadata_surface(), None);
    }

    #[tokio::test]
    async fn an_internal_table_read_is_flagged() {
        let ctx = ctx_with_table("__beacon_users");
        let plan = plan_for(&ctx, "SELECT * FROM __beacon_users").await;

        let facts = PlanFacts::new(&plan, &ctx);

        assert!(facts.touches_internal_tables());
    }

    #[tokio::test]
    async fn ddl_is_not_an_unprivileged_statement() {
        let ctx = ctx_with_table("observations");
        let select = plan_for(&ctx, "SELECT * FROM observations").await;
        let create = ctx
            .state()
            .create_logical_plan("CREATE TABLE other (a INT)")
            .await
            .unwrap();

        assert!(PlanFacts::new(&select, &ctx).verify_unprivileged().is_ok());
        assert!(PlanFacts::new(&create, &ctx).verify_unprivileged().is_err());
    }

    #[test]
    fn a_metadata_schema_reference_is_reported() {
        let ctx = ctx_with_table("observations");

        let system = TableFacts::new(TableReference::partial("system", "roles"), &ctx);
        let plain = TableFacts::new(TableReference::bare("observations"), &ctx);

        assert_eq!(system.metadata_schema(), Some("system"));
        assert_eq!(plain.metadata_schema(), None);
    }
}
