use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use envconfig::Envconfig;

pub mod error;

pub use error::ConfigError;
use error::Result;

// Per-format and storage config types are owned by their crates; beacon-config
// composes them here and fills them from the environment.
pub use beacon_arrow_atlas::datafusion::AtlasConfig;
pub use beacon_arrow_bbf::datafusion::BbfConfig;
pub use beacon_arrow_netcdf::datafusion::NetcdfConfig;
pub use beacon_common::CrawlerConfig;

#[derive(Debug, Clone)]
pub struct Config {
    pub admin: AdminConfig,
    pub auth: AuthConfig,
    pub oidc: OidcConfig,
    pub server: ServerConfig,
    pub runtime: RuntimeConfig,
    pub sql: SqlConfig,
    pub flight_sql: FlightSqlConfig,
    pub cors: CorsConfig,
    pub netcdf: NetcdfConfig,
    pub atlas: AtlasConfig,
    pub bbf: BbfConfig,
    pub crawler: CrawlerConfig,
    pub api_docs: ApiDocsConfig,
    /// Resolved data-directory paths (root + sub-directories).
    pub data: DataDirsConfig,
    pub secrets: SecretsConfig,
}

#[derive(Debug, Clone)]
pub struct AdminConfig {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Whether the built-in anonymous user (empty password) is seeded so unauthenticated requests
    /// resolve to its roles. When disabled, unauthenticated requests have no roles.
    pub anonymous_enabled: bool,
    /// Whether query-time authorization (read enforcement) is applied. When false, queries are not
    /// privilege-checked beyond the existing super-user DDL/DML gate — backwards compatible default.
    pub enforce: bool,
}

#[derive(Debug, Clone)]
pub struct OidcConfig {
    /// Whether an external OIDC/OAuth2 provider is enabled alongside local users. When enabled,
    /// `Bearer` JWT access tokens are validated against `jwks_url` and mapped to roles.
    pub enabled: bool,
    /// Expected token issuer (`iss` claim).
    pub issuer: String,
    /// URL of the issuer's JWKS document (signing keys).
    pub jwks_url: String,
    /// Expected audience (`aud` claim); empty disables audience validation.
    pub audience: String,
    /// Dotted path to the claim holding the principal's role names (e.g. `realm_access.roles`).
    pub roles_claim: String,
    /// Dotted path to the claim holding the principal's username (e.g. `preferred_username`).
    pub username_claim: String,
    /// How long (seconds) a fetched JWKS document is cached before being re-fetched.
    pub jwks_cache_ttl_secs: u64,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub port: u16,
    pub host: String,
    pub log_level: String,
    pub worker_threads: usize,
    /// URL prefix for all HTTP routes, e.g. `/base-path`. Empty string means serve at `/`.
    pub base_path: String,
    /// Directory holding the built admin web UI (Vite `dist/`). Served at
    /// `{base_path}/admin` when the directory exists; skipped otherwise.
    pub web_ui_dir: String,
    /// Maximum size, in bytes, accepted for a single dataset upload. `0` disables
    /// the cap. From `BEACON_MAX_UPLOAD_BYTES`.
    pub max_upload_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Query memory pool size, in **megabytes** (the runtime builder takes bytes).
    pub vm_memory_size: usize,
    pub sanitize_schema: bool,
    pub st_within_point_cache_size: usize,
    pub enable_sys_info: bool,
    pub batch_size: usize,
}

#[derive(Debug, Clone)]
pub struct SqlConfig {
    pub enable: bool,
    pub default_table: String,
    pub enable_pushdown_projection: bool,
    /// Enable the N-dimensional pipeline optimizer: the physical rule that
    /// replaces plan nodes above the nd broadcast (e.g. sinking element-wise
    /// projections into an `NdProjectionExec`) when it can. The base nd pipeline
    /// (`NdSourceExec` → `NdBroadcastExec`) always runs regardless; this only
    /// gates the node-rewriting optimizations.
    pub enable_nd_pipeline: bool,
    pub stream_coalesce: SqlStreamCoalesceConfig,
}

#[derive(Debug, Clone)]
pub struct SqlStreamCoalesceConfig {
    pub enabled: bool,
    pub target_rows: usize,
    pub flush_timeout_ms: u64,
    pub max_rows: usize,
}

#[derive(Debug, Clone)]
pub struct FlightSqlConfig {
    pub enable: bool,
    pub allow_anonymous: bool,
    pub host: String,
    pub port: u16,
    pub token_ttl_secs: u64,
    pub statement_ttl_secs: u64,
    pub prepared_statement_ttl_secs: u64,
}

#[derive(Debug, Clone)]
pub struct CorsConfig {
    pub allowed_methods: String,
    pub allowed_origins: String,
    pub allowed_headers: String,
    /// Response headers exposed to browser JS on cross-origin requests. Defaults
    /// to `x-beacon-query-id` so a cross-origin UI (e.g. the Vite dev server) can
    /// read the query id the SDK surfaces; same-origin requests can already.
    pub expose_headers: String,
    pub allowed_credentials: bool,
    pub max_age: u64,
}

/// Metadata exposed at the top level of the OpenAPI document (and the Swagger /
/// Scalar UIs). All fields are configurable so deployments can brand their own
/// API docs without recompiling.
#[derive(Debug, Clone)]
pub struct ApiDocsConfig {
    pub title: String,
    pub description: String,
    pub terms_of_service: Option<String>,
    pub contact_name: Option<String>,
    pub contact_url: Option<String>,
    pub contact_email: Option<String>,
    pub license_name: Option<String>,
    pub license_url: Option<String>,
    pub license_identifier: Option<String>,
}

/// Master key material for encrypting secrets (e.g. external-database
/// credentials) at rest. Sourced from `BEACON_SECRETS_KEY` (base64 of 32
/// bytes). When absent, features that persist credentials must fail closed
/// rather than write plaintext.
#[derive(Clone)]
pub struct SecretsConfig {
    master_key: Option<[u8; 32]>,
}

impl SecretsConfig {
    /// The decoded 32-byte master key, or `None` if `BEACON_SECRETS_KEY` is unset.
    pub fn master_key(&self) -> Option<&[u8; 32]> {
        self.master_key.as_ref()
    }
}

// Never print key material, even via `{:?}`.
impl std::fmt::Debug for SecretsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretsConfig")
            .field(
                "master_key",
                &self.master_key.map(|_| "<set>").unwrap_or("<unset>"),
            )
            .finish()
    }
}

/// Resolved data-directory paths, all derived from `BEACON_DATA_DIR` (default
/// `./data`). The directories are created when the config is loaded.
///
/// Every path the data lake and the runtime need lives here, so the layout is
/// decided in exactly one place and a single environment variable relocates all
/// of it. (These used to be `lazy_static`s hardcoded to `./data`, which silently
/// ignored `BEACON_DATA_DIR` for everything except `indexes` and `cache`.)
#[derive(Debug, Clone)]
pub struct DataDirsConfig {
    /// The root every other path below is derived from.
    pub root: PathBuf,
    /// Datasets store: the files the lake uploads, downloads, and queries.
    pub datasets: PathBuf,
    /// Directory holding the single-file tables store.
    pub tables: PathBuf,
    /// The tables store itself: catalog plus managed table data, one redb file.
    pub db_file: PathBuf,
    /// Scratch space for query output files.
    pub tmp: PathBuf,
    pub indexes: PathBuf,
    pub cache: PathBuf,
}

#[derive(Debug, Envconfig)]
struct RawConfig {
    #[envconfig(from = "BEACON_ADMIN_USERNAME", default = "beacon-admin")]
    admin_username: String,
    #[envconfig(from = "BEACON_ADMIN_PASSWORD", default = "beacon-password")]
    admin_password: String,
    #[envconfig(from = "BEACON_AUTH_ANONYMOUS_ENABLED", default = "true")]
    auth_anonymous_enabled: bool,
    #[envconfig(from = "BEACON_AUTH_ENFORCE", default = "false")]
    auth_enforce: bool,
    #[envconfig(from = "BEACON_OIDC_ENABLED", default = "false")]
    oidc_enabled: bool,
    #[envconfig(from = "BEACON_OIDC_ISSUER", default = "")]
    oidc_issuer: String,
    #[envconfig(from = "BEACON_OIDC_JWKS_URL", default = "")]
    oidc_jwks_url: String,
    #[envconfig(from = "BEACON_OIDC_AUDIENCE", default = "")]
    oidc_audience: String,
    #[envconfig(from = "BEACON_OIDC_ROLES_CLAIM", default = "realm_access.roles")]
    oidc_roles_claim: String,
    #[envconfig(from = "BEACON_OIDC_USERNAME_CLAIM", default = "preferred_username")]
    oidc_username_claim: String,
    #[envconfig(from = "BEACON_OIDC_JWKS_CACHE_TTL_SECS", default = "300")]
    oidc_jwks_cache_ttl_secs: u64,
    #[envconfig(from = "BEACON_PORT", default = "5001")]
    port: u16,
    #[envconfig(from = "BEACON_HOST", default = "0.0.0.0")]
    host: String,
    #[envconfig(from = "BEACON_LOG_LEVEL", default = "info")]
    log_level: String,

    //VM Settings
    /// Query memory pool size, in **megabytes**.
    #[envconfig(from = "BEACON_VM_MEMORY_SIZE", default = "8192")]
    vm_memory_size: usize,
    #[envconfig(from = "BEACON_DEFAULT_TABLE", default = "default")]
    default_table: String,
    #[envconfig(from = "BEACON_SANITIZE_SCHEMA", default = "false")]
    sanitize_schema: bool,
    #[envconfig(from = "BEACON_ENABLE_SQL", default = "true")]
    enable_sql: bool,
    #[envconfig(from = "BEACON_FLIGHT_SQL_ENABLE", default = "true")]
    flight_sql_enable: bool,
    #[envconfig(from = "BEACON_FLIGHT_SQL_ALLOW_ANONYMOUS", default = "false")]
    flight_sql_allow_anonymous: bool,
    #[envconfig(from = "BEACON_FLIGHT_SQL_HOST", default = "0.0.0.0")]
    flight_sql_host: String,
    #[envconfig(from = "BEACON_FLIGHT_SQL_PORT", default = "32011")]
    flight_sql_port: u16,
    #[envconfig(from = "BEACON_FLIGHT_SQL_TOKEN_TTL_SECS", default = "3600")]
    flight_sql_token_ttl_secs: u64,
    #[envconfig(from = "BEACON_FLIGHT_SQL_STATEMENT_TTL_SECS", default = "300")]
    flight_sql_statement_ttl_secs: u64,
    #[envconfig(
        from = "BEACON_FLIGHT_SQL_PREPARED_STATEMENT_TTL_SECS",
        default = "900"
    )]
    flight_sql_prepared_statement_ttl_secs: u64,
    #[envconfig(from = "BEACON_SQL_STREAM_COALESCE_ENABLED", default = "true")]
    sql_stream_coalesce_enabled: bool,
    #[envconfig(from = "BEACON_SQL_STREAM_COALESCE_TARGET_ROWS", default = "65536")]
    sql_stream_coalesce_target_rows: usize,
    #[envconfig(from = "BEACON_SQL_STREAM_COALESCE_FLUSH_TIMEOUT_MS", default = "25")]
    sql_stream_coalesce_flush_timeout_ms: u64,
    #[envconfig(from = "BEACON_SQL_STREAM_COALESCE_MAX_ROWS", default = "262144")]
    sql_stream_coalesce_max_rows: usize,
    #[envconfig(from = "BEACON_ST_WITHIN_POINT_CACHE_SIZE", default = "10000")]
    st_within_point_cache_size: usize,
    #[envconfig(from = "BEACON_WORKER_THREADS", default = "8")]
    worker_threads: usize,
    #[envconfig(from = "BEACON_BASE_PATH", default = "")]
    base_path: String,
    /// Directory containing the built admin web UI. Defaults to `web` (resolved
    /// relative to the working directory; `/beacon/web` in the Docker image).
    #[envconfig(from = "BEACON_WEB_UI_DIR", default = "web")]
    web_ui_dir: String,

    #[envconfig(from = "BEACON_S3_BUCKET")]
    s3_bucket: Option<String>,
    #[envconfig(from = "BEACON_S3_ENABLE_VIRTUAL_HOSTING", default = "false")]
    s3_enable_virtual_hosting: bool,
    #[envconfig(from = "BEACON_S3_DATA_LAKE", default = "false")]
    s3_data_lake: bool,
    // S3-compatible endpoint and region. Read from the standard AWS env vars so
    // they can be captured into `S3Config` (the single source of truth) instead
    // of being re-read from the environment at use time.
    #[envconfig(from = "AWS_ENDPOINT")]
    aws_endpoint: Option<String>,
    #[envconfig(from = "AWS_REGION")]
    aws_region: Option<String>,
    #[envconfig(from = "BEACON_S3_ALLOW_HTTP", default = "true")]
    s3_allow_http: bool,
    // Filesystem change events for the local datasets store. Off by default: the
    // OS watcher (notify/FSEvents) can lag or replay stale events, and listings now
    // read straight through to the backing store, so the watcher-maintained cache
    // is not needed for correctness. Enable (BEACON_ENABLE_FS_EVENTS=true) to get
    // live auto-refresh of external tables and event-driven crawler triggering when
    // files change on disk (crawlers otherwise still run on their interval).
    #[envconfig(from = "BEACON_ENABLE_FS_EVENTS", default = "false")]
    enable_fs_events: bool,
    #[envconfig(from = "BEACON_ENABLE_S3_EVENTS", default = "false")]
    enable_s3_events: bool,
    // Maximum size, in bytes, accepted for a single dataset upload through the
    // admin API. `0` disables the cap. Default ~5 GiB.
    #[envconfig(from = "BEACON_MAX_UPLOAD_BYTES", default = "5368709120")]
    max_upload_bytes: u64,
    // Part size advertised for chunked (resumable) uploads. Default 8 MiB (≥ S3's
    // 5 MiB minimum part size).
    #[envconfig(from = "BEACON_UPLOAD_PART_SIZE", default = "8388608")]
    upload_part_size: usize,
    // Idle timeout (seconds) before an abandoned chunked upload session is aborted.
    #[envconfig(from = "BEACON_UPLOAD_SESSION_TTL_SECS", default = "3600")]
    upload_session_ttl_secs: u64,

    // Others
    #[envconfig(from = "BEACON_ENABLE_SYS_INFO", default = "false")]
    enable_sys_info: bool,
    /// CORS CONFIG
    #[envconfig(
        from = "BEACON_CORS_ALLOWED_METHODS",
        default = "GET,POST,PUT,DELETE,OPTIONS"
    )]
    allowed_methods: String,
    #[envconfig(from = "BEACON_CORS_ALLOWED_ORIGINS", default = "*")]
    allowed_origins: String,
    #[envconfig(
        from = "BEACON_CORS_ALLOWED_HEADERS",
        default = "Content-Type,Authorization"
    )]
    allowed_headers: String,
    #[envconfig(from = "BEACON_CORS_EXPOSE_HEADERS", default = "x-beacon-query-id")]
    expose_headers: String,
    #[envconfig(from = "BEACON_CORS_ALLOWED_CREDENTIALS", default = "false")]
    allowed_credentials: bool,
    #[envconfig(from = "BEACON_CORS_MAX_AGE", default = "3600")]
    max_age: u64,
    #[envconfig(from = "BEACON_ENABLE_PUSHDOWN_PROJECTION", default = "true")]
    enable_pushdown_projection: bool,
    #[envconfig(from = "BEACON_ENABLE_ND_PIPELINE", default = "false")]
    enable_nd_pipeline: bool,

    /// Root directory for Beacon's local data (datasets, tables, tmp, etc.).
    #[envconfig(from = "BEACON_DATA_DIR", default = "./data")]
    data_dir: String,

    #[envconfig(from = "BEACON_NETCDF_ENABLE_STATISTICS", default = "true")]
    netcdf_enable_statistics: bool,

    #[envconfig(from = "BEACON_NETCDF_USE_READER_CACHE", default = "true")]
    netcdf_use_reader_cache: bool,
    #[envconfig(from = "BEACON_NETCDF_READER_CACHE_SIZE", default = "128")]
    netcdf_reader_cache_size: usize,

    #[envconfig(from = "BEACON_ATLAS_USE_READER_CACHE", default = "true")]
    atlas_use_reader_cache: bool,
    #[envconfig(from = "BEACON_ATLAS_READER_CACHE_SIZE", default = "32")]
    atlas_reader_cache_size: u64,

    /// The batch size for NetCDF reads, in number of rows. This is used for both local and MPIO reads.
    #[envconfig(from = "BEACON_BATCH_SIZE", default = "64000")]
    beacon_batch_size: usize,

    /// Whether to split streams into 16k row slices for better memory management and parallelism.
    #[envconfig(from = "BEACON_ENABLE_BBF_SPLIT_STREAMS_SLICE", default = "false")]
    bbf_split_streams_slice: bool,

    // Base64-encoded 32-byte master key for encrypting persisted secrets
    // (external-database credentials). Optional; validated in `Config::load`.
    #[envconfig(from = "BEACON_SECRETS_KEY")]
    secrets_key: Option<String>,

    // Crawler subsystem
    #[envconfig(from = "BEACON_CRAWLER_ENABLE", default = "true")]
    crawler_enable: bool,
    #[envconfig(from = "BEACON_CRAWLER_DEFAULT_INTERVAL_SECS", default = "900")]
    crawler_default_interval_secs: u64,

    // OpenAPI documentation metadata
    #[envconfig(from = "BEACON_API_TITLE", default = "Beacon Rest API")]
    api_title: String,
    #[envconfig(
        from = "BEACON_API_DESCRIPTION",
        default = "Beacon HTTP API. Exposes read-only client endpoints for querying the Beacon runtime (datasets, tables, functions, SQL queries) and authenticated admin endpoints for managing tables and data lake files."
    )]
    api_description: String,
    #[envconfig(from = "BEACON_API_TERMS_OF_SERVICE")]
    api_terms_of_service: Option<String>,
    #[envconfig(from = "BEACON_API_CONTACT_NAME")]
    api_contact_name: Option<String>,
    #[envconfig(from = "BEACON_API_CONTACT_URL")]
    api_contact_url: Option<String>,
    #[envconfig(from = "BEACON_API_CONTACT_EMAIL")]
    api_contact_email: Option<String>,
    #[envconfig(from = "BEACON_API_LICENSE_NAME")]
    api_license_name: Option<String>,
    #[envconfig(from = "BEACON_API_LICENSE_URL")]
    api_license_url: Option<String>,
    #[envconfig(from = "BEACON_API_LICENSE_IDENTIFIER")]
    api_license_identifier: Option<String>,
}

impl From<RawConfig> for Config {
    fn from(raw: RawConfig) -> Self {
        Self {
            admin: AdminConfig {
                username: raw.admin_username,
                password: raw.admin_password,
            },
            auth: AuthConfig {
                anonymous_enabled: raw.auth_anonymous_enabled,
                enforce: raw.auth_enforce,
            },
            oidc: OidcConfig {
                enabled: raw.oidc_enabled,
                issuer: raw.oidc_issuer,
                jwks_url: raw.oidc_jwks_url,
                audience: raw.oidc_audience,
                roles_claim: raw.oidc_roles_claim,
                username_claim: raw.oidc_username_claim,
                jwks_cache_ttl_secs: raw.oidc_jwks_cache_ttl_secs,
            },
            server: ServerConfig {
                port: raw.port,
                host: raw.host,
                log_level: raw.log_level,
                worker_threads: raw.worker_threads,
                base_path: raw.base_path,
                web_ui_dir: raw.web_ui_dir,
                max_upload_bytes: raw.max_upload_bytes,
            },
            runtime: RuntimeConfig {
                vm_memory_size: raw.vm_memory_size,
                sanitize_schema: raw.sanitize_schema,
                st_within_point_cache_size: raw.st_within_point_cache_size,
                enable_sys_info: raw.enable_sys_info,
                batch_size: raw.beacon_batch_size,
            },
            sql: SqlConfig {
                enable: raw.enable_sql,
                default_table: raw.default_table,
                enable_pushdown_projection: raw.enable_pushdown_projection,
                enable_nd_pipeline: raw.enable_nd_pipeline,
                stream_coalesce: SqlStreamCoalesceConfig {
                    enabled: raw.sql_stream_coalesce_enabled,
                    target_rows: raw.sql_stream_coalesce_target_rows,
                    flush_timeout_ms: raw.sql_stream_coalesce_flush_timeout_ms,
                    max_rows: raw.sql_stream_coalesce_max_rows,
                },
            },
            flight_sql: FlightSqlConfig {
                enable: raw.flight_sql_enable,
                allow_anonymous: raw.flight_sql_allow_anonymous,
                host: raw.flight_sql_host,
                port: raw.flight_sql_port,
                token_ttl_secs: raw.flight_sql_token_ttl_secs,
                statement_ttl_secs: raw.flight_sql_statement_ttl_secs,
                prepared_statement_ttl_secs: raw.flight_sql_prepared_statement_ttl_secs,
            },
            cors: CorsConfig {
                allowed_methods: raw.allowed_methods,
                allowed_origins: raw.allowed_origins,
                allowed_headers: raw.allowed_headers,
                expose_headers: raw.expose_headers,
                allowed_credentials: raw.allowed_credentials,
                max_age: raw.max_age,
            },
            netcdf: NetcdfConfig {
                use_reader_cache: raw.netcdf_use_reader_cache,
                reader_cache_size: raw.netcdf_reader_cache_size,
                enable_statistics: raw.netcdf_enable_statistics,
            },
            atlas: AtlasConfig {
                use_reader_cache: raw.atlas_use_reader_cache,
                reader_cache_size: raw.atlas_reader_cache_size,
            },
            bbf: BbfConfig {
                split_streams_slice: raw.bbf_split_streams_slice,
            },
            crawler: CrawlerConfig {
                enable: raw.crawler_enable,
                default_interval_secs: raw.crawler_default_interval_secs,
            },
            api_docs: ApiDocsConfig {
                title: raw.api_title,
                description: raw.api_description,
                terms_of_service: raw.api_terms_of_service,
                contact_name: raw.api_contact_name,
                contact_url: raw.api_contact_url,
                contact_email: raw.api_contact_email,
                license_name: raw.api_license_name,
                license_url: raw.api_license_url,
                license_identifier: raw.api_license_identifier,
            },
            data: {
                let root = PathBuf::from(&raw.data_dir);
                DataDirsConfig {
                    datasets: root.join("datasets"),
                    tables: root.join("tables"),
                    db_file: root.join("tables").join("beacon.db"),
                    tmp: root.join("tmp"),
                    indexes: root.join("indexes"),
                    cache: root.join("cache"),
                    root,
                }
            },
            // Decoded and validated in `Config::load` (see `secrets_key`).
            secrets: SecretsConfig { master_key: None },
        }
    }
}

/// Decode a base64-encoded 32-byte master key from `BEACON_SECRETS_KEY`.
fn decode_master_key(b64: &str) -> std::result::Result<[u8; 32], String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .map_err(|e| format!("not valid base64: {e}"))?;
    bytes
        .try_into()
        .map_err(|v: Vec<u8>| format!("expected 32 bytes, got {}", v.len()))
}

/// Normalizes and validates a configured base path. Returns the canonical form:
/// exactly one leading `/` and no trailing `/`. A blank value yields `""` (root).
/// Errors (with a descriptive message) if the path contains characters outside the
/// URL "unreserved" set or has an empty internal segment, instead of letting an
/// invalid value reach axum/utoipa, which panic on malformed paths.
fn normalize_base_path(raw: &str) -> std::result::Result<String, String> {
    let trimmed = raw.trim().trim_matches('/');
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    for segment in trimmed.split('/') {
        if segment.is_empty() {
            return Err(format!("'{raw}' contains an empty path segment"));
        }
        if let Some(bad) = segment
            .chars()
            .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~')))
        {
            return Err(format!(
                "'{raw}' contains invalid character '{bad}'; only letters, digits, \
                 '-', '_', '.', '~' and '/' are allowed"
            ));
        }
    }
    Ok(format!("/{trimmed}"))
}

impl Config {
    /// Loads the configuration from the environment, normalizing and validating
    /// fields. Returns a descriptive error instead of panicking, so callers can
    /// report the problem cleanly and exit.
    pub fn load() -> Result<Config> {
        let raw = RawConfig::init_from_env().map_err(|e| ConfigError::EnvLoad(e.to_string()))?;
        // Capture the secrets key before `raw` is consumed; decode/validate below.
        let secrets_key_b64 = raw.secrets_key.clone();
        let mut config: Config = raw.into();
        if let Some(b64) = secrets_key_b64 {
            config.secrets.master_key =
                Some(decode_master_key(&b64).map_err(ConfigError::InvalidSecretsKey)?);
        }
        config.server.base_path =
            normalize_base_path(&config.server.base_path).map_err(ConfigError::InvalidBasePath)?;
        // Create the configured data directories (idempotent). `db_file` is a file,
        // not a directory — its parent (`tables`) is created here.
        for dir in [
            &config.data.root,
            &config.data.datasets,
            &config.data.tables,
            &config.data.tmp,
            &config.data.indexes,
            &config.data.cache,
        ] {
            create_dir(dir)?;
        }
        tracing::debug!(
            host = %config.server.host,
            port = config.server.port,
            base_path = %config.server.base_path,
            "loaded Beacon configuration from environment"
        );
        Ok(config)
    }
}

static CONFIG_CELL: OnceLock<Config> = OnceLock::new();

/// Loads, normalizes, and validates the configuration and stores it in the
/// process-global cell. Returns a descriptive error instead of panicking, so the
/// binary can surface configuration problems and exit cleanly.
///
/// Call this once early in `main`. It is idempotent: subsequent calls return the
/// already-initialized [`Config`].
#[deprecated(
    note = "Config is no longer process-global; load it with `Config::load()` and pass \
            `Arc<Config>` to `Runtime::new`. This remains only for legacy unit tests."
)]
pub fn init() -> Result<&'static Config> {
    if let Some(config) = CONFIG_CELL.get() {
        return Ok(config);
    }
    let config = Config::load()?;
    // A concurrent caller may have won the race; either value is equally valid.
    let _ = CONFIG_CELL.set(config);
    Ok(CONFIG_CELL.get().expect("config cell populated above"))
}

/// Zero-sized handle that dereferences to the process-global [`Config`].
///
/// All `beacon_config::CONFIG.<field>` accesses go through this. Binaries should
/// call [`init`] in `main` to surface configuration errors cleanly; this handle
/// falls back to lazy loading for code paths (e.g. unit tests in other crates)
/// that do not call [`init`] first.
pub struct ConfigHandle;

impl std::ops::Deref for ConfigHandle {
    type Target = Config;

    fn deref(&self) -> &Self::Target {
        CONFIG_CELL.get_or_init(|| {
            Config::load().expect("failed to load Beacon configuration from environment")
        })
    }
}

/// Process-global configuration handle. Dereferences to [`Config`].
///
/// Deprecated: configuration is no longer process-global. Load it with
/// [`Config::load`] and pass an `Arc<Config>` into `Runtime::new`. This handle
/// remains only as a fallback for legacy unit tests.
#[deprecated(
    note = "Config is no longer process-global; load it with `Config::load()` and pass \
            `Arc<Config>` to `Runtime::new`. This remains only for legacy unit tests."
)]
pub static CONFIG: ConfigHandle = ConfigHandle;

/// Creates `path` (and any missing parents), returning a structured
/// [`ConfigError::CreateDir`] and logging the failure on error.
fn create_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path).map_err(|source| {
        tracing::error!(path = %path.display(), error = %source, "failed to create data directory");
        ConfigError::CreateDir {
            path: path.to_path_buf(),
            source,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{Config, PathBuf, RawConfig, decode_master_key, normalize_base_path};
    use envconfig::Envconfig;
    use std::collections::HashMap;

    /// Parses a `RawConfig` from an explicit variable map instead of the process
    /// environment, so these tests never race with each other (or with other
    /// crates) over global env state.
    fn raw(vars: &[(&str, &str)]) -> std::result::Result<RawConfig, envconfig::Error> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        RawConfig::init_from_hashmap(&map)
    }

    /// The `RawConfig -> Config` mapping for a given variable map. This is
    /// everything `Config::load` does apart from base-path normalization, secret
    /// decoding, and creating the data directories.
    fn config(vars: &[(&str, &str)]) -> Config {
        Config::from(raw(vars).expect("config should parse"))
    }

    /// Every data path derives from `BEACON_DATA_DIR`. This is a regression guard:
    /// the paths used to be `lazy_static`s pinned to `./data`, so setting the
    /// variable relocated only `indexes` and `cache` while the datasets, tables,
    /// and tmp directories silently stayed behind.
    #[test]
    fn every_data_path_follows_the_configured_root() {
        let config = config(&[("BEACON_DATA_DIR", "/srv/beacon")]);
        let root = PathBuf::from("/srv/beacon");

        assert_eq!(config.data.root, root);
        assert_eq!(config.data.datasets, root.join("datasets"));
        assert_eq!(config.data.tables, root.join("tables"));
        assert_eq!(config.data.db_file, root.join("tables").join("beacon.db"));
        assert_eq!(config.data.tmp, root.join("tmp"));
        assert_eq!(config.data.indexes, root.join("indexes"));
        assert_eq!(config.data.cache, root.join("cache"));
    }

    /// The out-of-the-box deployment posture. These defaults decide how a Beacon
    /// with no environment at all behaves, so they are pinned deliberately.
    #[test]
    fn defaults_of_an_empty_environment() {
        let config = config(&[]);

        // The single config-defined super-user exists even with no env set.
        assert_eq!(config.admin.username, "beacon-admin");
        assert_eq!(config.admin.password, "beacon-password");

        // Anonymous access is on, query-time enforcement is off (documented as
        // the backwards-compatible default), OIDC is off.
        assert!(config.auth.anonymous_enabled);
        assert!(!config.auth.enforce);
        assert!(!config.oidc.enabled);
        assert_eq!(config.oidc.roles_claim, "realm_access.roles");
        assert_eq!(config.oidc.username_claim, "preferred_username");
        assert_eq!(config.oidc.jwks_cache_ttl_secs, 300);

        assert_eq!(config.server.port, 5001);
        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.base_path, "");
        assert_eq!(config.cors.allowed_origins, "*");
        assert!(!config.cors.allowed_credentials);
        assert!(!config.flight_sql.allow_anonymous);

        // No secrets key configured: features that persist credentials must fail
        // closed rather than write plaintext.
        assert!(config.secrets.master_key().is_none());
    }

    /// Every data sub-directory hangs off `BEACON_DATA_DIR`; the conversion only
    /// derives the paths (creation happens in `Config::load`).
    #[test]
    fn data_dirs_derive_from_data_dir() {
        let default = config(&[]);
        assert_eq!(default.data.indexes, PathBuf::from("./data/indexes"));
        assert_eq!(default.data.cache, PathBuf::from("./data/cache"));

        let custom = config(&[("BEACON_DATA_DIR", "/srv/beacon")]);
        assert_eq!(custom.data.indexes, PathBuf::from("/srv/beacon/indexes"));
        assert_eq!(custom.data.cache, PathBuf::from("/srv/beacon/cache"));
    }

    /// Booleans are parsed by `bool::from_str`, which accepts only the exact
    /// lowercase literals. Anything else is a hard error — a security-relevant
    /// setting is never silently coerced to `false`.
    #[test]
    fn boolean_vars_accept_only_exact_true_false() {
        assert!(config(&[("BEACON_AUTH_ENFORCE", "true")]).auth.enforce);
        assert!(!config(&[("BEACON_AUTH_ENFORCE", "false")]).auth.enforce);
        for bad in ["1", "TRUE", "True", "yes", "on", ""] {
            assert!(
                raw(&[("BEACON_AUTH_ENFORCE", bad)]).is_err(),
                "'{bad}' must be rejected, not coerced"
            );
        }
    }

    /// Numeric values are range-checked by their target type and are never
    /// clamped: an out-of-range port fails to load instead of wrapping.
    #[test]
    fn numeric_vars_are_range_checked_not_clamped() {
        assert_eq!(config(&[("BEACON_PORT", "65535")]).server.port, 65535);
        assert!(raw(&[("BEACON_PORT", "65536")]).is_err());
        assert!(raw(&[("BEACON_PORT", "-1")]).is_err());
        assert!(raw(&[("BEACON_PORT", "5001.0")]).is_err());
        assert!(raw(&[("BEACON_WORKER_THREADS", "-1")]).is_err());
        // Zero is accepted as-is (no floor is applied anywhere).
        assert_eq!(config(&[("BEACON_PORT", "0")]).server.port, 0);
        assert_eq!(
            config(&[("BEACON_BATCH_SIZE", "0")]).runtime.batch_size,
            0
        );
    }

    /// A parse failure names the offending variable, so `ConfigError::EnvLoad`
    /// tells the operator which value to fix.
    #[test]
    fn parse_errors_name_the_variable() {
        let err = raw(&[("BEACON_FLIGHT_SQL_PORT", "not-a-port")])
            .unwrap_err()
            .to_string();
        assert!(err.contains("BEACON_FLIGHT_SQL_PORT"), "got: {err}");
    }

    /// Key material must never reach a log line, so `SecretsConfig`'s `Debug`
    /// (and the `Config` debug output that embeds it) only says whether it is set.
    #[test]
    fn secrets_key_is_never_printed() {
        let mut config = config(&[]);
        assert!(format!("{:?}", config.secrets).contains("<unset>"));

        config.secrets.master_key = Some([0xAB; 32]);
        let printed = format!("{:?}", config);
        assert!(printed.contains("<set>"), "got: {printed}");
        assert!(!printed.contains("171"), "raw key bytes leaked: {printed}");
        assert!(!printed.contains("ab, ab"), "raw key bytes leaked: {printed}");
    }

    #[test]
    fn empty_and_blank_serve_at_root() {
        assert_eq!(normalize_base_path(""), Ok(String::new()));
        assert_eq!(normalize_base_path("   "), Ok(String::new()));
        assert_eq!(normalize_base_path("/"), Ok(String::new()));
        assert_eq!(normalize_base_path("///"), Ok(String::new()));
    }

    #[test]
    fn normalizes_to_single_leading_slash_no_trailing() {
        assert_eq!(normalize_base_path("mybeacon"), Ok("/mybeacon".to_string()));
        assert_eq!(normalize_base_path("foo"), Ok("/foo".to_string()));
        assert_eq!(normalize_base_path("/foo"), Ok("/foo".to_string()));
        assert_eq!(normalize_base_path("/foo/"), Ok("/foo".to_string()));
        assert_eq!(normalize_base_path("///foo///"), Ok("/foo".to_string()));
        assert_eq!(normalize_base_path("  /foo/  "), Ok("/foo".to_string()));
    }

    #[test]
    fn preserves_nested_segments_and_unreserved_chars() {
        assert_eq!(normalize_base_path("foo/bar"), Ok("/foo/bar".to_string()));
        assert_eq!(normalize_base_path("/foo/bar/"), Ok("/foo/bar".to_string()));
        assert_eq!(
            normalize_base_path("my-app_v2.1~beta"),
            Ok("/my-app_v2.1~beta".to_string())
        );
    }

    #[test]
    fn rejects_invalid_characters() {
        assert!(normalize_base_path("my path").is_err());
        assert!(normalize_base_path("foo?bar").is_err());
        assert!(normalize_base_path("foo#bar").is_err());
        assert!(normalize_base_path("foo%20bar").is_err());
    }

    #[test]
    fn rejects_empty_internal_segment() {
        assert!(normalize_base_path("a//b").is_err());
    }

    #[test]
    fn decode_master_key_accepts_exactly_32_bytes() {
        use base64::Engine;
        let raw = [7u8; 32];
        let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
        assert_eq!(decode_master_key(&b64), Ok(raw));
        // Surrounding whitespace is trimmed before decoding.
        assert_eq!(decode_master_key(&format!("  {b64}\n")), Ok(raw));
    }

    #[test]
    fn decode_master_key_rejects_invalid_base64() {
        let err = decode_master_key("not valid base64!!!").unwrap_err();
        assert!(err.contains("not valid base64"), "got: {err}");
    }

    #[test]
    fn decode_master_key_rejects_wrong_length() {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode([1u8, 2, 3, 4]);
        let err = decode_master_key(&b64).unwrap_err();
        assert!(err.contains("expected 32 bytes, got 4"), "got: {err}");
    }
}
