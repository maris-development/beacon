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
use crate::schema_persistence::PersistentSchemaProvider;

/// The spelling that selects an in-memory database, shared with DuckDB and SQLite.
pub const MEMORY_PATH: &str = ":memory:";

/// Double-quotes a SQL identifier, escaping embedded quotes, so a name with spaces or a
/// reserved word cannot break the statement it is spliced into.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The URL bare dataset paths resolve against when [`OpenOptions::with_datasets_dir`] is used.
pub const DATASETS_STORE_URL: &str = "datasets://";

// Re-exported so an embedder configures the datasets store through this module alone, rather
// than depending on beacon-datafusion-ext and datafusion directly.
pub use beacon_datafusion_ext::listing_factory::RootStore;
// The credential for [`Database::attach_remote`], re-exported so an embedder names it through this
// module rather than depending on beacon-datafusion-ext directly.
pub use beacon_datafusion_ext::remote::RemoteCredential;
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
    /// The 32-byte master key that encrypts persisted secrets (`CREATE PERSISTENT SECRET`, external
    /// database passwords) at rest. `None` falls back to the base64 `BEACON_SECRETS_KEY` env var;
    /// with neither set, persisting a credential fails closed.
    pub secrets_key: Option<[u8; 32]>,
    /// Open read-only: every write (DDL/DML and beacon's side-effecting statements) is refused.
    /// The container file is still opened with an exclusive lock, so this is a per-connection
    /// writability guarantee, not (yet) multi-process concurrent access.
    pub read_only: bool,
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

    /// Set the 32-byte master key used to encrypt persisted secrets at rest.
    pub fn with_secrets_key(mut self, key: [u8; 32]) -> Self {
        self.secrets_key = Some(key);
        self
    }

    /// Open read-only: refuse every write on the resulting database.
    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
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
            .with_read_only(options.read_only)
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
        // The master key for at-rest secret encryption: explicit option, else `BEACON_SECRETS_KEY`.
        if let Some(key) = options.secrets_key.or_else(secrets_key_from_env) {
            builder = builder.with_secrets_encryption(key);
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

    /// Attach another Beacon instance as a catalog named `name`, so its tables are queryable as
    /// `name.<schema>.<table>` and joins/filters/aggregates push down to it over Flight SQL.
    ///
    /// `url` is the remote's Flight SQL endpoint — `beacon://host:port`, `grpc://…`, `http://…`,
    /// `https://…`, or a bare `host:port`. `tls` (or an explicit `https://`) selects TLS. `token`,
    /// if given, is the remote's bearer credential; without one the remote must permit anonymous
    /// access. The remote's schemas and tables are enumerated once here, so an unreachable or
    /// unauthorized endpoint fails now rather than on first query.
    pub async fn attach_remote(
        &self,
        name: &str,
        url: &str,
        credential: beacon_datafusion_ext::remote::RemoteCredential,
        tls: bool,
    ) -> anyhow::Result<()> {
        attach_remote_catalog(&self.runtime.session_ctx, name, url, credential, tls).await
    }

    /// Detach a previously [`attach_remote`](Self::attach_remote)ed catalog. Returns whether one
    /// was attached under that name.
    pub fn detach(&self, name: &str) -> anyhow::Result<bool> {
        detach_remote_catalog(&self.runtime.session_ctx, name)
    }

    /// The names of the remote Beacon instances currently attached as catalogs.
    pub fn attached_catalogs(&self) -> Vec<String> {
        attached_remote_catalogs(&self.runtime.session_ctx)
    }

    /// Resolve a stored `TYPE BEACON` secret into a remote credential, for
    /// [`attach_remote`](Self::attach_remote)ing with `secret=…`.
    pub fn secret_credential(
        &self,
        name: &str,
    ) -> anyhow::Result<beacon_datafusion_ext::remote::RemoteCredential> {
        let store = self
            .runtime
            .session_ctx
            .state()
            .config()
            .get_extension::<beacon_datafusion_ext::secrets::SecretStore>()
            .ok_or_else(|| anyhow::anyhow!("secret store is unavailable"))?;
        let secret = store
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("no secret named '{name}'"))?;
        beacon_datafusion_ext::remote::RemoteCredential::from_secret(&secret)
    }

    /// Whether this database was opened with RBAC applied.
    pub fn auth_enabled(&self) -> bool {
        self.auth_enabled
    }

    /// Whether this database was opened read-only (all writes refused).
    pub fn is_read_only(&self) -> bool {
        self.runtime.read_only
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

    /// Registers `batches` as a **session-only** in-memory table named `name`, queryable by
    /// that bare name for the life of the process.
    ///
    /// This is how an embedder makes a pandas/pyarrow/polars frame queryable. The table is a
    /// [`MemTable`] and is **not persisted** into `beacon.db` (see
    /// [`PersistentSchemaProvider::register_temporary_table`]): copying the file does not carry
    /// it, and reopening the database does not see it. A prior table under `name` is replaced.
    pub fn register_batches(
        &self,
        name: &str,
        schema: arrow::datatypes::SchemaRef,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> anyhow::Result<()> {
        let table = datafusion::datasource::MemTable::try_new(schema, vec![batches])
            .map_err(|e| anyhow::anyhow!("failed to build the in-memory table `{name}`: {e}"))?;
        self.with_default_schema(|schema| {
            schema
                .register_temporary_table(name.to_string(), Arc::new(table))
                .map_err(|e| anyhow::anyhow!("failed to register `{name}`: {e}"))?;
            Ok(())
        })
    }

    /// Creates a **persisted** managed table named `name` from `batches`, as `identity`.
    ///
    /// Unlike [`Self::register_batches`] (a session-only [`MemTable`]), this writes the data into
    /// `beacon.db` through the default managed-table engine — so it survives a reopen and travels
    /// with the file. Implemented by reusing beacon's `CREATE TABLE … AS SELECT` path: the batches
    /// are staged as a temporary table and copied into a managed one. Being real DDL, it requires
    /// the identity's DDL privileges (a super-user), and it fails if `name` already exists — drop
    /// it first to replace, rather than silently overwriting persisted data.
    pub async fn persist_batches(
        &self,
        name: &str,
        schema: arrow::datatypes::SchemaRef,
        batches: Vec<arrow::record_batch::RecordBatch>,
        identity: AuthIdentity,
    ) -> anyhow::Result<()> {
        use futures::TryStreamExt;

        // A fixed staging name is safe: registration overwrites any stale entry, opens are
        // single-writer, and the embedded API is driven single-threaded from Python. It does not
        // match `INTERNAL_TABLE_PREFIX` (`__beacon_`), so it is not treated as an internal table.
        const STAGING: &str = "__beacondb_register_staging";

        self.register_batches(STAGING, schema, batches)?;

        let ctas = format!(
            "CREATE TABLE {} AS SELECT * FROM {}",
            quote_ident(name),
            quote_ident(STAGING)
        );
        let outcome = async {
            let stream = self.sql(ctas, identity).await?.into_record_stream()?;
            // Draining the statement's stream is what runs the write to completion.
            stream.try_collect::<Vec<_>>().await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;

        // Remove the staging table whether or not the CTAS succeeded, then surface its result.
        let _ = self.deregister_table(STAGING);
        outcome
    }

    /// Append `batches` to an existing managed table `name` (`INSERT INTO name SELECT * FROM …`).
    ///
    /// The insert runs through the normal query path, so it is subject to the same authorization
    /// (write privileges) and read-only checks as any other write, and fails clearly if `name` does
    /// not exist or its schema is incompatible.
    pub async fn append_batches(
        &self,
        name: &str,
        schema: arrow::datatypes::SchemaRef,
        batches: Vec<arrow::record_batch::RecordBatch>,
        identity: AuthIdentity,
    ) -> anyhow::Result<()> {
        use futures::TryStreamExt;

        const STAGING: &str = "__beacondb_append_staging";
        self.register_batches(STAGING, schema, batches)?;

        let insert = format!(
            "INSERT INTO {} SELECT * FROM {}",
            quote_ident(name),
            quote_ident(STAGING)
        );
        let outcome = async {
            let stream = self.sql(insert, identity).await?.into_record_stream()?;
            stream.try_collect::<Vec<_>>().await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;

        let _ = self.deregister_table(STAGING);
        outcome
    }

    /// Removes a table registered with [`Self::register_batches`]. Returns whether one existed.
    pub fn deregister_table(&self, name: &str) -> anyhow::Result<bool> {
        self.with_default_schema(|schema| {
            let removed = schema
                .deregister_temporary_table(name)
                .map_err(|e| anyhow::anyhow!("failed to unregister `{name}`: {e}"))?;
            Ok(removed.is_some())
        })
    }

    /// Runs `f` with the default schema, downcast to the concrete
    /// [`PersistentSchemaProvider`] (which owns the temporary-table registration path).
    fn with_default_schema<R>(
        &self,
        f: impl FnOnce(&PersistentSchemaProvider) -> anyhow::Result<R>,
    ) -> anyhow::Result<R> {
        let ctx = &self.runtime.session_ctx;
        let state = ctx.state();
        let options = state.config().options();
        let catalog_name = options.catalog.default_catalog.clone();
        let schema_name = options.catalog.default_schema.clone();

        let catalog = ctx
            .catalog(&catalog_name)
            .ok_or_else(|| anyhow::anyhow!("default catalog `{catalog_name}` is not registered"))?;
        let schema = catalog
            .schema(&schema_name)
            .ok_or_else(|| anyhow::anyhow!("default schema `{schema_name}` is not registered"))?;
        let provider = schema
            .as_any()
            .downcast_ref::<PersistentSchemaProvider>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "the default schema is not beacon's persistent provider; cannot register an \
                     in-memory table"
                )
            })?;
        f(provider)
    }
}

/// Decode a base64 string into a 32-byte master key. Errors if it is not valid base64 or not
/// exactly 32 bytes.
pub fn decode_secrets_key(encoded: &str) -> anyhow::Result<[u8; 32]> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .map_err(|e| anyhow::anyhow!("secrets key is not valid base64: {e}"))?;
    <[u8; 32]>::try_from(bytes.as_slice())
        .map_err(|_| anyhow::anyhow!("secrets key must be exactly 32 bytes (got {})", bytes.len()))
}

/// Decode the base64 `BEACON_SECRETS_KEY` env var into a 32-byte master key, if set and valid.
///
/// A malformed value is ignored (treated as absent) rather than failing the open — a persisted
/// secret would then fail closed with a clear "no encryption key" message, which is safer than
/// refusing to open the database at all.
fn secrets_key_from_env() -> Option<[u8; 32]> {
    let encoded = std::env::var("BEACON_SECRETS_KEY").ok()?;
    decode_secrets_key(&encoded).ok()
}

/// Attach a remote Beacon as a catalog named `name` on `session_ctx`.
///
/// Shared by [`Database::attach_remote`] (the embedded API) and the SQL `ATTACH` statement, so both
/// register the identical [`RemoteCatalogProvider`](beacon_datafusion_ext::remote::RemoteCatalogProvider)
/// and are observed the same way by [`attached_remote_catalogs`].
pub(crate) async fn attach_remote_catalog(
    session_ctx: &datafusion::prelude::SessionContext,
    name: &str,
    url: &str,
    credential: beacon_datafusion_ext::remote::RemoteCredential,
    tls: bool,
) -> anyhow::Result<()> {
    anyhow::ensure!(!name.trim().is_empty(), "attach name must not be empty");
    let endpoint = normalize_remote_endpoint(url, tls)?;
    let connection =
        beacon_datafusion_ext::remote::RemoteConnection::with_credential(endpoint, credential);
    let catalog = beacon_datafusion_ext::remote::RemoteCatalogProvider::connect(connection).await?;
    session_ctx.register_catalog(name.to_string(), Arc::new(catalog));
    Ok(())
}

/// Detach a remote catalog by shadowing it with an empty one (DataFusion cannot deregister a
/// catalog). Returns whether a *remote* catalog was attached under that name — detaching a
/// non-remote catalog (e.g. `beacon`) is refused by returning `false`.
pub(crate) fn detach_remote_catalog(
    session_ctx: &datafusion::prelude::SessionContext,
    name: &str,
) -> anyhow::Result<bool> {
    if !is_remote_catalog(session_ctx, name) {
        return Ok(false);
    }
    session_ctx.register_catalog(
        name.to_string(),
        Arc::new(datafusion::catalog::MemoryCatalogProvider::new()),
    );
    Ok(true)
}

/// The names of the remote Beacon catalogs currently attached — derived from the session's
/// registered catalogs (those that are a `RemoteCatalogProvider`), so it is a single source of
/// truth shared by the embedded API and SQL `ATTACH`.
pub(crate) fn attached_remote_catalogs(
    session_ctx: &datafusion::prelude::SessionContext,
) -> Vec<String> {
    let mut names: Vec<String> = session_ctx
        .catalog_names()
        .into_iter()
        .filter(|name| is_remote_catalog(session_ctx, name))
        .collect();
    names.sort();
    names
}

/// Whether the catalog registered under `name` is a remote-Beacon catalog.
fn is_remote_catalog(session_ctx: &datafusion::prelude::SessionContext, name: &str) -> bool {
    session_ctx
        .catalog(name)
        .map(|catalog| {
            catalog
                .as_any()
                .downcast_ref::<beacon_datafusion_ext::remote::RemoteCatalogProvider>()
                .is_some()
        })
        .unwrap_or(false)
}

/// Normalize an attach URL into a tonic gRPC endpoint (`http(s)://host:port`).
///
/// Accepts the `beacon://`/`grpc://` schemes used by `CREATE EXTERNAL TABLE … STORED AS BEACON`,
/// plain `http://`/`https://`, or a bare `host:port`. `tls` (or an explicit `https://`) selects
/// `https`.
pub(crate) fn normalize_remote_endpoint(url: &str, tls: bool) -> anyhow::Result<String> {
    let url = url.trim();
    let secure = tls || url.starts_with("https://");
    let authority = url
        .strip_prefix("beacon://")
        .or_else(|| url.strip_prefix("grpc://"))
        .or_else(|| url.strip_prefix("https://"))
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url)
        .trim_end_matches('/');

    anyhow::ensure!(!authority.is_empty(), "attach url missing host:port");

    let scheme = if secure { "https" } else { "http" };
    Ok(format!("{scheme}://{authority}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_endpoint_maps_schemes_and_tls() {
        // beacon:// / grpc:// / bare host:port default to http unless tls is asked for
        assert_eq!(
            normalize_remote_endpoint("beacon://host:50051", false).unwrap(),
            "http://host:50051"
        );
        assert_eq!(
            normalize_remote_endpoint("grpc://host:50051", true).unwrap(),
            "https://host:50051"
        );
        assert_eq!(
            normalize_remote_endpoint("host:50051", false).unwrap(),
            "http://host:50051"
        );
        // an explicit https:// implies tls even without the flag; a trailing slash is trimmed
        assert_eq!(
            normalize_remote_endpoint("https://host:50051/", false).unwrap(),
            "https://host:50051"
        );
        assert_eq!(
            normalize_remote_endpoint("http://host:50051", false).unwrap(),
            "http://host:50051"
        );
        assert!(normalize_remote_endpoint("beacon://", false).is_err());
    }

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
