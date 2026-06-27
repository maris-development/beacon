use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use envconfig::Envconfig;
use lazy_static::lazy_static;

pub mod error;

pub use error::ConfigError;
use error::Result;

// Per-format and storage config types are owned by their crates; beacon-config
// composes them here and fills them from the environment.
pub use beacon_arrow_atlas::datafusion::AtlasConfig;
pub use beacon_arrow_bbf::datafusion::BbfConfig;
pub use beacon_arrow_netcdf::datafusion::NetcdfConfig;
pub use beacon_object_storage::{S3Config, StorageConfig};

#[derive(Debug, Clone)]
pub struct Config {
    pub admin: AdminConfig,
    pub auth: AuthConfig,
    pub oidc: OidcConfig,
    pub server: ServerConfig,
    pub runtime: RuntimeConfig,
    pub sql: SqlConfig,
    pub flight_sql: FlightSqlConfig,
    pub storage: StorageConfig,
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
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
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
    pub stream_coalesce: SqlStreamCoalesceConfig,
    /// Storage engine used for managed `CREATE TABLE` when the statement does not
    /// override it (via `SET beacon.table_engine = '…'`).
    pub default_table_engine: TableEngine,
}

/// The storage engine backing a beacon-managed table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TableEngine {
    /// Lance: local-filesystem, columnar, versioned (default).
    #[default]
    Lance,
    /// Apache Iceberg: object-store-backed lakehouse table.
    Iceberg,
}

impl TableEngine {
    pub fn as_str(&self) -> &'static str {
        match self {
            TableEngine::Lance => "lance",
            TableEngine::Iceberg => "iceberg",
        }
    }
}

impl std::str::FromStr for TableEngine {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "lance" => Ok(TableEngine::Lance),
            "iceberg" => Ok(TableEngine::Iceberg),
            other => Err(format!(
                "unknown table engine '{other}', expected 'lance' or 'iceberg'"
            )),
        }
    }
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

/// Settings for the Glue-style crawler subsystem.
#[derive(Debug, Clone)]
pub struct CrawlerConfig {
    /// Master switch for crawler scheduling/event triggers. When false, crawlers
    /// can still be defined and run on demand, but no background tasks are spawned.
    pub enable: bool,
    /// Fallback poll interval (seconds) applied to a crawler that requests
    /// event-driven crawling on a deployment where storage events are unavailable.
    pub default_interval_secs: u64,
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

/// Resolved data-directory paths, derived from `BEACON_DATA_DIR` (default
/// `./data`). The directories are created when the config is loaded.
#[derive(Debug, Clone)]
pub struct DataDirsConfig {
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
    #[envconfig(from = "BEACON_VM_MEMORY_SIZE", default = "8192")]
    vm_memory_size: usize,
    #[envconfig(from = "BEACON_DEFAULT_TABLE", default = "default")]
    default_table: String,
    #[envconfig(from = "BEACON_DEFAULT_TABLE_ENGINE", default = "lance")]
    default_table_engine: String,
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
    // Filesystem change events are on by default for the local datasets store;
    // set BEACON_ENABLE_FS_EVENTS=false to disable the watcher.
    #[envconfig(from = "BEACON_ENABLE_FS_EVENTS", default = "true")]
    enable_fs_events: bool,
    #[envconfig(from = "BEACON_ENABLE_S3_EVENTS", default = "false")]
    enable_s3_events: bool,

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
    #[envconfig(
        from = "BEACON_CORS_EXPOSE_HEADERS",
        default = "x-beacon-query-id"
    )]
    expose_headers: String,
    #[envconfig(from = "BEACON_CORS_ALLOWED_CREDENTIALS", default = "false")]
    allowed_credentials: bool,
    #[envconfig(from = "BEACON_CORS_MAX_AGE", default = "3600")]
    max_age: u64,
    #[envconfig(from = "BEACON_ENABLE_PUSHDOWN_PROJECTION", default = "true")]
    enable_pushdown_projection: bool,

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
                stream_coalesce: SqlStreamCoalesceConfig {
                    enabled: raw.sql_stream_coalesce_enabled,
                    target_rows: raw.sql_stream_coalesce_target_rows,
                    flush_timeout_ms: raw.sql_stream_coalesce_flush_timeout_ms,
                    max_rows: raw.sql_stream_coalesce_max_rows,
                },
                default_table_engine: match raw.default_table_engine.parse() {
                    Ok(engine) => engine,
                    Err(e) => {
                        tracing::warn!("invalid BEACON_DEFAULT_TABLE_ENGINE: {e}; defaulting to lance");
                        TableEngine::default()
                    }
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
            storage: {
                let root = PathBuf::from(&raw.data_dir);
                // S3 presence *is* the backend switch: `Some` => datasets on S3.
                let s3 = if raw.s3_data_lake {
                    Some(S3Config {
                        bucket: raw.s3_bucket.unwrap_or_default(),
                        endpoint: raw.aws_endpoint,
                        region: raw.aws_region,
                        enable_virtual_hosting: raw.s3_enable_virtual_hosting,
                        allow_http: raw.s3_allow_http,
                    })
                } else {
                    None
                };
                StorageConfig {
                    datasets_dir: root.join("datasets"),
                    tables_dir: root.join("tables"),
                    tmp_dir: root.join("tmp"),
                    data_dir: root,
                    enable_fs_events: raw.enable_fs_events,
                    enable_s3_events: raw.enable_s3_events,
                    s3,
                }
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
                    indexes: root.join("indexes"),
                    cache: root.join("cache"),
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
        // S3 always needs a bucket (object_store requires it; it is never inferred
        // from the endpoint).
        if let Some(s3) = &config.storage.s3 {
            if s3.bucket.trim().is_empty() {
                return Err(ConfigError::InvalidStorage(
                    "BEACON_S3_BUCKET is required when BEACON_S3_DATA_LAKE=true".to_string(),
                ));
            }
        }
        // Create the configured data directories (idempotent).
        for dir in [
            &config.storage.data_dir,
            &config.storage.datasets_dir,
            &config.storage.tables_dir,
            &config.storage.tmp_dir,
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

/// [`create_dir`] for the `lazy_static` data-directory accessors below, which
/// cannot return a `Result`. On failure it panics with the structured
/// [`ConfigError`] message (path + underlying I/O error) after logging it.
fn ensure_dir(path: PathBuf) -> PathBuf {
    if let Err(e) = create_dir(&path) {
        panic!("{e}");
    }
    path
}

lazy_static! {
    pub static ref DATA_DIR: PathBuf = ensure_dir(PathBuf::from("./data"));
    /// The path to the datasets directory
    pub static ref DATASETS_DIR_PATH: PathBuf = ensure_dir(DATA_DIR.join("datasets"));
    /// The prefix for the datasets directory for object store paths
    pub static ref DATASETS_DIR_PREFIX: object_store::path::Path =
        object_store::path::Path::from("datasets");

    pub static ref TABLES_DIR_PREFIX: object_store::path::Path =
        object_store::path::Path::from("tables");
    pub static ref TABLES_DIR: PathBuf = ensure_dir(DATA_DIR.join("tables"));

    /// The path to the users directory, holding the persisted auth directory
    /// database (users, roles, and privilege grants), next to the tables directory.
    pub static ref USERS_DIR: PathBuf = ensure_dir(DATA_DIR.join("users"));

    pub static ref TMP_DIR: PathBuf = ensure_dir(DATA_DIR.join("tmp"));

    /// The path to the indexes directory
    pub static ref INDEX_DIR_PATH: PathBuf = ensure_dir(DATA_DIR.join("indexes"));
    /// The prefix for the indexes directory for object store paths
    pub static ref INDEX_DIR_PREFIX: object_store::path::Path =
        object_store::path::Path::from("indexes");

    /// The path to the cache directory
    pub static ref CACHE_DIR_PATH: PathBuf = ensure_dir(DATA_DIR.join("cache"));
    /// The prefix for the cache directory for object store paths
    pub static ref CACHE_DIR_PREFIX: object_store::path::Path =
        object_store::path::Path::from("cache");
}

#[cfg(test)]
mod tests {
    use super::{decode_master_key, normalize_base_path, TableEngine};

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
    fn table_engine_parses_case_insensitively_and_trims() {
        assert_eq!("lance".parse::<TableEngine>(), Ok(TableEngine::Lance));
        assert_eq!("ICEBERG".parse::<TableEngine>(), Ok(TableEngine::Iceberg));
        assert_eq!("  Iceberg  ".parse::<TableEngine>(), Ok(TableEngine::Iceberg));
        assert!("postgres".parse::<TableEngine>().is_err());
    }

    #[test]
    fn table_engine_as_str_round_trips_and_defaults_to_lance() {
        assert_eq!(TableEngine::Lance.as_str(), "lance");
        assert_eq!(TableEngine::Iceberg.as_str(), "iceberg");
        assert_eq!(TableEngine::default(), TableEngine::Lance);
        for e in [TableEngine::Lance, TableEngine::Iceberg] {
            assert_eq!(e.as_str().parse::<TableEngine>(), Ok(e));
        }
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
