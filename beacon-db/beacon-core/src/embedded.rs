//! Opening a beacon database **in-process**, the way DuckDB and SQLite are opened.
//!
//! [`Runtime`] is deliberately narrow — authenticate a caller, then run a query — because it
//! is designed to sit behind a server that already knows who the caller is. An embedder has no
//! server: it is a Python notebook, a test, or an application that owns the file outright and
//! needs one call that turns a path into something it can query. That call is [`Database::open`].
//!
//! # The two auth modes
//!
//! The only decision an embedder has to make is whether beacon's RBAC applies, and the default
//! is that it does not:
//!
//! - [`AuthMode::Disabled`] (default) — every query runs as [`AuthIdentity::local`], a
//!   super-user, with grant enforcement off. Opening the file requires no credentials and
//!   permits everything. **This is the SQLite/DuckDB contract: possession of the file is
//!   full control.**
//! - [`AuthMode::Enabled`] — grant enforcement on, sessions resolve to the anonymous
//!   principal until a [`Credential`] is supplied, and DDL/DML requires the single configured
//!   super-user.
//!
//! Both switches have to move together, which is why this is one enum rather than two flags:
//! [`validate_query_plan`](crate::statement_plan) derives `allow_ddl`/`allow_dml` from
//! `is_super_user` *alone*, so enforcement-off with a non-super identity would be a broken
//! half-mode where reads work and `CREATE TABLE` does not.
//!
//! The mode is a property of *how the database was opened*, never of the file: auth tables are
//! created and bootstrapped in both modes, so users, roles and grants written by one mode are
//! still there in the other. A `beacon.db` governed by a server can therefore be opened locally
//! with auth disabled and read in full — RBAC is a boundary for *served* access, not against
//! local possession of the bytes.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use beacon_auth::{AuthIdentity, Credential, ANONYMOUS_USERNAME};
use beacon_datafusion_ext::listing_factory::DefaultStore;
use datafusion::scalar::ScalarValue;
use tokio::runtime::Handle;

use crate::crawler::CrawlerConfig;
use crate::query::Query;
use crate::query_result::QueryResult;
use crate::runtime::Runtime;
use crate::runtime_builder::RuntimeBuilder;

/// The spelling that selects an in-memory database, shared with DuckDB and SQLite.
pub const MEMORY_PATH: &str = ":memory:";

/// The URL bare dataset paths resolve against when [`OpenOptions::with_datasets_dir`] is used.
pub const DATASETS_STORE_URL: &str = "datasets://";

// Re-exported so an embedder configures the datasets store through this module alone, rather
// than depending on beacon-datafusion-ext and datafusion directly.
pub use beacon_datafusion_ext::listing_factory::RootStore;
pub use datafusion::execution::object_store::ObjectStoreUrl;

/// Where a database's container lives: one file, or nothing at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbPath {
    /// No file. The catalog and any managed data live in memory and vanish on close.
    Memory,
    /// A single `beacon.db`-style container file (a [`beacon_redb_store::RedbStore`]).
    File(PathBuf),
}

impl DbPath {
    /// Parses the DuckDB-style database spec: `":memory:"` (or empty) is in-memory, anything
    /// else is a filesystem path.
    pub fn parse(spec: &str) -> Self {
        if spec.is_empty() || spec == MEMORY_PATH {
            Self::Memory
        } else {
            Self::File(PathBuf::from(spec))
        }
    }

    /// The container file, or `None` for an in-memory database.
    pub fn as_path(&self) -> Option<&Path> {
        match self {
            Self::Memory => None,
            Self::File(path) => Some(path),
        }
    }

    pub fn is_memory(&self) -> bool {
        matches!(self, Self::Memory)
    }
}

impl From<&str> for DbPath {
    fn from(spec: &str) -> Self {
        Self::parse(spec)
    }
}

impl From<PathBuf> for DbPath {
    fn from(path: PathBuf) -> Self {
        Self::File(path)
    }
}

/// The single super-user credential, supplied by the embedder when auth is enabled.
#[derive(Clone)]
pub struct AdminCredentials {
    pub username: String,
    pub password: String,
}

impl AdminCredentials {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

// Hand-written so a stray `{:?}` on `OpenOptions` (a panic message, a tracing span, a Python
// repr) can never print the super-user's password.
impl std::fmt::Debug for AdminCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminCredentials")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// How an auth-enabled database resolves principals.
#[derive(Debug, Clone, Default)]
pub struct AuthSettings {
    /// The one super-user. `None` means no principal can perform DDL/DML — useful for a
    /// strictly read-only embedding, useless otherwise.
    pub admin: Option<AdminCredentials>,
    /// The principal unauthenticated sessions resolve to. `None` disables anonymous access,
    /// making credentials mandatory.
    pub anonymous_username: Option<String>,
}

/// Whether beacon's RBAC model applies to this database.
#[derive(Debug, Clone, Default)]
pub enum AuthMode {
    /// No authentication, no authorization: every session is [`AuthIdentity::local`].
    #[default]
    Disabled,
    /// Full RBAC. See the module docs.
    Enabled(AuthSettings),
}

impl AuthMode {
    /// RBAC on, with the default anonymous principal and no super-user.
    pub fn enabled() -> Self {
        Self::Enabled(AuthSettings {
            admin: None,
            anonymous_username: Some(ANONYMOUS_USERNAME.to_string()),
        })
    }

    /// RBAC on, with the default anonymous principal and the given super-user.
    pub fn enabled_with_admin(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::Enabled(AuthSettings {
            admin: Some(AdminCredentials::new(username, password)),
            anonymous_username: Some(ANONYMOUS_USERNAME.to_string()),
        })
    }

    /// Requires credentials on every session by disabling the anonymous principal.
    /// No-op when auth is disabled — there are no principals to require.
    pub fn without_anonymous(self) -> Self {
        match self {
            Self::Disabled => Self::Disabled,
            Self::Enabled(settings) => Self::Enabled(AuthSettings {
                anonymous_username: None,
                ..settings
            }),
        }
    }

    pub fn is_enabled(&self) -> bool {
        matches!(self, Self::Enabled(_))
    }
}

/// Everything an embedder can configure at open time.
///
/// Defaults are chosen for an interactive, single-user embedding: auth off, crawlers available,
/// dataset paths resolved dynamically by their own scheme (or against the cwd).
#[derive(Debug, Clone, Default)]
pub struct OpenOptions {
    /// Whether RBAC applies. Defaults to [`AuthMode::Disabled`].
    pub auth: AuthMode,
    /// The Tokio runtime the engine schedules on. Defaults to the ambient runtime, which means
    /// [`Database::open`] must then be called from inside one.
    pub runtime_handle: Option<Handle>,
    pub batch_size: Option<usize>,
    pub memory_limit: Option<usize>,
    pub cpu_limit: Option<usize>,
    pub nd_pipeline: bool,
    /// Crawler subsystem config. Enabled by default; nothing is scheduled until a crawler
    /// exists, so this costs an empty database nothing.
    pub crawlers: CrawlerConfig,
    /// Where bare dataset paths resolve. `None` leaves the runtime in dynamic mode: paths
    /// resolve by their own scheme (`s3://`, `https://`) or against the current directory.
    pub datasets: Option<DefaultStore>,
    /// Scratch directory for query output files. Defaults to the system temp directory.
    pub tmp_dir: Option<PathBuf>,
}

impl OpenOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_auth(mut self, auth: AuthMode) -> Self {
        self.auth = auth;
        self
    }

    pub fn with_runtime_handle(mut self, handle: Handle) -> Self {
        self.runtime_handle = Some(handle);
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = Some(size);
        self
    }

    pub fn with_memory_limit(mut self, bytes: usize) -> Self {
        self.memory_limit = Some(bytes);
        self
    }

    pub fn with_cpu_limit(mut self, cpus: usize) -> Self {
        self.cpu_limit = Some(cpus);
        self
    }

    pub fn with_nd_pipeline(mut self, enabled: bool) -> Self {
        self.nd_pipeline = enabled;
        self
    }

    pub fn with_crawlers(mut self, crawlers: CrawlerConfig) -> Self {
        self.crawlers = crawlers;
        self
    }

    pub fn with_datasets(mut self, datasets: DefaultStore) -> Self {
        self.datasets = Some(datasets);
        self
    }

    /// Resolves bare dataset paths against a local directory, registered under
    /// [`DATASETS_STORE_URL`].
    ///
    /// The common case: `read_parquet("obs/*.parquet")` then means `obs/*.parquet` under `dir`,
    /// independent of the process's working directory.
    pub fn with_datasets_dir(self, dir: impl Into<PathBuf>) -> Self {
        let url = ObjectStoreUrl::parse(DATASETS_STORE_URL)
            .expect("DATASETS_STORE_URL is a valid object store URL");
        self.with_datasets(DefaultStore::new(url, RootStore::FileSystem(dir.into())))
    }

    pub fn with_tmp_dir(mut self, dir: PathBuf) -> Self {
        self.tmp_dir = Some(dir);
        self
    }
}

/// An open beacon database: a [`Runtime`] plus the identity policy it was opened under.
///
/// Cheap to clone-share via [`Database::runtime`]; the container file is held by an exclusive
/// lock for the lifetime of this value, so one process opens one `beacon.db` once.
pub struct Database {
    runtime: Arc<Runtime>,
    auth_enabled: bool,
    /// The identity a session gets without presenting credentials. `None` only when auth is
    /// enabled *and* anonymous access is disabled, i.e. credentials are mandatory.
    default_identity: Option<AuthIdentity>,
}

// `Runtime` is not `Debug` (it owns a session context), so this reports the parts an embedder
// actually wants in a panic message or a Python `repr`: which mode the database is in and who
// a credential-less session is.
impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("auth_enabled", &self.auth_enabled)
            .field(
                "default_identity",
                &self
                    .default_identity
                    .as_ref()
                    .map(|identity| identity.username.as_str()),
            )
            .finish_non_exhaustive()
    }
}

impl Database {
    /// Opens (or creates) a database.
    ///
    /// With [`DbPath::File`] the catalog and all managed data live in that one container file;
    /// with [`DbPath::Memory`] they live in memory. Fails if another process already holds the
    /// file's exclusive lock.
    pub async fn open(path: impl Into<DbPath>, options: OpenOptions) -> anyhow::Result<Self> {
        let path = path.into();
        let auth_enabled = options.auth.is_enabled();

        let mut builder = RuntimeBuilder::new()
            .with_auth_enforcement(auth_enabled)
            .with_crawler(options.crawlers.clone());

        if let Some(file) = path.as_path() {
            builder = builder.with_db_path(file.to_path_buf());
        }
        if let Some(handle) = options.runtime_handle.clone() {
            builder = builder.with_runtime_handle(handle);
        }
        if let Some(size) = options.batch_size {
            builder = builder.with_batch_size(size);
        }
        if let Some(limit) = options.memory_limit {
            builder = builder.with_vm_memory_limit(limit);
        }
        if let Some(limit) = options.cpu_limit {
            builder = builder.with_vm_cpu_limit(limit);
        }
        if options.nd_pipeline {
            builder = builder.with_nd_pipeline();
        }
        if let Some(datasets) = &options.datasets {
            builder = builder.with_default_store(datasets.url.clone(), datasets.root.clone());
        }
        if let Some(dir) = options.tmp_dir.clone() {
            builder = builder.with_tmp_dir_path(dir);
        }
        if let AuthMode::Enabled(settings) = &options.auth {
            if let Some(admin) = &settings.admin {
                builder =
                    builder.with_admin_credentials(admin.username.clone(), admin.password.clone());
            }
            if let Some(anonymous) = &settings.anonymous_username {
                builder = builder.with_anonymous_user(anonymous.clone());
            }
        }

        let runtime = Arc::new(builder.build().await.map_err(|source| {
            match path.as_path() {
                // The overwhelmingly common failure here is a second open of a file another
                // process (usually a running beacon server) already locked. Name the file, so
                // the message points at the fix instead of at redb internals.
                Some(file) => source.context(format!(
                    "failed to open beacon database at {}",
                    file.display()
                )),
                None => source.context("failed to open in-memory beacon database"),
            }
        })?);

        // Resolve the credential-less identity once, at open time, so a misconfiguration fails
        // here rather than on the first query.
        let default_identity = if auth_enabled {
            if runtime.anonymous_enabled() {
                Some(runtime.authenticate_anonymous().await?)
            } else {
                None
            }
        } else {
            Some(AuthIdentity::local())
        };

        Ok(Self {
            runtime,
            auth_enabled,
            default_identity,
        })
    }

    /// The underlying runtime, for callers that need the full query surface.
    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    /// Whether this database was opened with RBAC applied.
    pub fn auth_enabled(&self) -> bool {
        self.auth_enabled
    }

    /// The identity a session gets without credentials: [`AuthIdentity::local`] when auth is
    /// disabled, the anonymous principal when it is enabled, and `None` when auth is enabled
    /// with anonymous access turned off.
    pub fn default_identity(&self) -> Option<&AuthIdentity> {
        self.default_identity.as_ref()
    }

    /// [`Self::default_identity`], erroring when credentials are mandatory.
    pub fn require_default_identity(&self) -> anyhow::Result<AuthIdentity> {
        self.default_identity.clone().ok_or_else(|| {
            anyhow::anyhow!(
                "this database requires credentials: it was opened with auth enabled and \
                 anonymous access disabled"
            )
        })
    }

    /// Resolves a credential to an identity.
    ///
    /// Errors when auth is disabled — there is nothing to authenticate against, and silently
    /// accepting credentials would imply a restriction that does not exist.
    pub async fn authenticate(&self, credential: &Credential) -> anyhow::Result<AuthIdentity> {
        if !self.auth_enabled {
            anyhow::bail!(
                "this database was opened with auth disabled, so credentials cannot be used: \
                 every session already has full access. Reopen with auth enabled to \
                 authenticate users."
            );
        }
        self.runtime.authenticate(credential).await
    }

    /// Runs a query as `identity`.
    pub async fn run_query(
        &self,
        query: Query,
        identity: AuthIdentity,
    ) -> anyhow::Result<QueryResult> {
        self.runtime.run_query(query, identity).await
    }

    /// Runs SQL as `identity`, streaming the result (no output format applied).
    pub async fn sql(
        &self,
        sql: impl Into<String>,
        identity: AuthIdentity,
    ) -> anyhow::Result<QueryResult> {
        self.run_query(Query::sql(sql.into()), identity).await
    }

    /// Runs SQL whose `$1..$n` placeholders are bound to `params`, as `identity`.
    ///
    /// The values are bound to the lowered plan rather than substituted into the SQL text, so
    /// there is no interpolation and no injection surface.
    pub async fn sql_with_params(
        &self,
        sql: impl Into<String>,
        params: Vec<ScalarValue>,
        identity: AuthIdentity,
    ) -> anyhow::Result<QueryResult> {
        self.run_query(Query::sql_with_params(sql.into(), params), identity)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_spec_parses_to_memory() {
        assert_eq!(DbPath::parse(":memory:"), DbPath::Memory);
        assert_eq!(DbPath::parse(""), DbPath::Memory);
        assert_eq!(
            DbPath::parse("beacon.db"),
            DbPath::File(PathBuf::from("beacon.db"))
        );
    }

    #[test]
    fn admin_credentials_never_debug_print_the_password() {
        let creds = AdminCredentials::new("admin", "hunter2");
        let rendered = format!("{creds:?}");
        assert!(!rendered.contains("hunter2"), "password leaked: {rendered}");
        assert!(rendered.contains("admin"));
    }

    #[test]
    fn auth_defaults_to_disabled() {
        assert!(!OpenOptions::default().auth.is_enabled());
        assert!(AuthMode::enabled().is_enabled());
    }
}
