use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use envconfig::Envconfig;

pub mod error;

pub use error::ConfigError;
use error::Result;

// Per-format and storage config types are owned by their crates; beacon-config
// composes them here and fills them from the environment.
pub use beacon_arrow_bbf::datafusion::BbfConfig;
pub use beacon_arrow_hdf5::Hdf5Config;
pub use beacon_arrow_netcdf::datafusion::NetcdfConfig;
pub use beacon_arrow_zarr::ZarrConfig;
pub use beacon_common::CrawlerConfig;
pub use beacon_common::FileStatsConfig;

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
    pub hdf5: Hdf5Config,
    pub zarr: ZarrConfig,
    pub bbf: BbfConfig,
    pub crawler: CrawlerConfig,
    pub file_stats: FileStatsConfig,
    pub api_docs: ApiDocsConfig,
    /// Resolved data-directory paths (root + sub-directories).
    pub data: DataDirsConfig,
    pub s3: S3Config,
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
    pub worker_threads: usize,
    /// URL prefix for all HTTP routes, e.g. `/base-path`. Empty string means serve at `/`.
    pub base_path: String,
    /// Directory holding the built admin web UI (Vite `dist/`). Served at
    /// `{base_path}/admin` when the directory exists; skipped otherwise.
    pub web_ui_dir: String,
    /// Maximum size, in bytes, accepted for a single dataset upload. `0` disables
    /// the cap. From `BEACON_MAX_UPLOAD_BYTES`.
    pub max_upload_bytes: u64,
    /// Log level for Beacon's own crates, lowercase and already validated: one of
    /// `trace`, `debug`, `info`, `warn`, `error`, `off`. From `BEACON_LOG_LEVEL`.
    /// `RUST_LOG` overrides it when set.
    pub log_level: String,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Query memory pool size, in **megabytes** (the runtime builder takes bytes).
    pub vm_memory_size: usize,
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
/// Every path the server and the runtime need lives here, so the layout is
/// decided in exactly one place and a single environment variable relocates all
/// of it. (These used to be `lazy_static`s hardcoded to `./data`, which silently
/// ignored `BEACON_DATA_DIR` for everything except `indexes` and `cache`.)
#[derive(Debug, Clone)]
pub struct DataDirsConfig {
    /// Datasets store: the files the server uploads, downloads, and queries.
    pub datasets: PathBuf,
    /// The tables store itself: catalog plus managed table data, one redb file.
    pub db_file: PathBuf,
    pub tmp: PathBuf,
}

/// Settings for backing the **datasets** store with an S3-compatible bucket
/// instead of the local `datasets/` directory.
///
/// Credentials, endpoint and region are *not* re-read here: the store is built
/// with `AmazonS3Builder::from_env()`, so the whole standard `AWS_*` chain
/// applies. `endpoint` and `region` are captured only to reconstruct the same
/// bucket's base URL for native readers (netCDF-c), which open by URL rather
/// than going through the object store.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Whether the datasets store is an S3 bucket. When false every other field
    /// here is inert and the datasets store is `BEACON_DATA_DIR/datasets`.
    pub datasets_on_s3: bool,
    /// Bucket holding the datasets. Required when `datasets_on_s3` is set;
    /// [`Config::load`] rejects the combination if it is missing.
    pub bucket: Option<String>,
    /// Virtual-hosted-style addressing (`{bucket}.{host}`) instead of path-style
    /// (`{endpoint}/{bucket}`).
    pub enable_virtual_hosting: bool,
    /// Allow plain `http://` endpoints. Useful for a local MinIO; leave off in
    /// production.
    pub allow_http: bool,
    /// `AWS_ENDPOINT`, when set. Absent means real AWS.
    pub endpoint: Option<String>,
    /// `AWS_REGION`, when set.
    pub region: Option<String>,
}

impl S3Config {
    /// The base URL of the datasets bucket, for readers that open by URL instead
    /// of through the object store.
    ///
    /// Mirrors the addressing `AmazonS3Builder` will use, so a native read and an
    /// object-store read of the same dataset resolve to the same bytes. Returns
    /// `None` when no bucket is configured.
    pub fn native_base_url(&self) -> Option<String> {
        let bucket = self.bucket.as_deref()?;
        let base = match self.endpoint.as_deref() {
            Some(endpoint) => {
                let endpoint = endpoint.trim_end_matches('/');
                if self.enable_virtual_hosting {
                    // Splice the bucket in as the leading host label:
                    // `https://minio:9000` -> `https://bucket.minio:9000`.
                    match endpoint.split_once("://") {
                        Some((scheme, host)) => format!("{scheme}://{bucket}.{host}"),
                        None => format!("{bucket}.{endpoint}"),
                    }
                } else {
                    format!("{endpoint}/{bucket}")
                }
            }
            // No endpoint configured: address the bucket on real AWS. Region is
            // part of the hostname there, defaulting to us-east-1 as the AWS SDKs do.
            None => {
                let region = self.region.as_deref().unwrap_or("us-east-1");
                if self.enable_virtual_hosting {
                    format!("https://{bucket}.s3.{region}.amazonaws.com")
                } else {
                    format!("https://s3.{region}.amazonaws.com/{bucket}")
                }
            }
        };
        Some(base)
    }
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
    /// Level for Beacon's own crates. Validated in [`Config::load`], so a typo
    /// stops the process instead of silently logging at the default level.
    #[envconfig(from = "BEACON_LOG_LEVEL", default = "info")]
    log_level: String,

    //VM Settings
    /// Query memory pool size, in **megabytes**.
    #[envconfig(from = "BEACON_VM_MEMORY_SIZE", default = "8192")]
    vm_memory_size: usize,
    #[envconfig(from = "BEACON_DEFAULT_TABLE", default = "default")]
    default_table: String,
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
    #[envconfig(from = "BEACON_WORKER_THREADS", default = "8")]
    worker_threads: usize,
    #[envconfig(from = "BEACON_BASE_PATH", default = "")]
    base_path: String,
    /// Directory containing the built admin web UI. Defaults to `web` (resolved
    /// relative to the working directory; `/beacon/web` in the Docker image).
    #[envconfig(from = "BEACON_WEB_UI_DIR", default = "web")]
    web_ui_dir: String,

    // S3-backed datasets store. Off by default: the datasets store is the local
    // `datasets/` directory unless BEACON_S3_DATASETS is set.
    #[envconfig(from = "BEACON_S3_DATASETS", default = "false")]
    s3_datasets: bool,
    // Former name of `BEACON_S3_DATASETS`, kept so existing deployments keep working.
    // `Config::load` warns when it is the one that turned the S3 store on. Remove it
    // one major version after 2.0.
    #[envconfig(from = "BEACON_S3_DATASETS", default = "false")]
    s3_data_lake_deprecated: bool,
    #[envconfig(from = "BEACON_S3_BUCKET")]
    s3_bucket: Option<String>,
    #[envconfig(from = "BEACON_S3_ENABLE_VIRTUAL_HOSTING", default = "false")]
    s3_enable_virtual_hosting: bool,
    #[envconfig(from = "BEACON_S3_ALLOW_HTTP", default = "true")]
    s3_allow_http: bool,
    // S3-compatible endpoint and region. The store itself is built with
    // `AmazonS3Builder::from_env()`, which reads these directly; they are captured
    // here only so the native-reader base URL is derived from the same values.
    #[envconfig(from = "AWS_ENDPOINT")]
    aws_endpoint: Option<String>,
    #[envconfig(from = "AWS_REGION")]
    aws_region: Option<String>,

    // Maximum size, in bytes, accepted for a single dataset upload through the
    // admin API. `0` disables the cap. Default ~5 GiB.
    #[envconfig(from = "BEACON_MAX_UPLOAD_BYTES", default = "5368709120")]
    max_upload_bytes: u64,

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

    /// Read netCDF with the pure-Rust `oxcdf` reader instead of netcdf-c.
    ///
    /// On by default. It reads in parallel. It opens a netCDF file in an object
    /// store (s3, gs or az), which netcdf-c cannot. It reports the statistics of
    /// each file.
    ///
    /// Set it to false to read with netcdf-c. Writes always use netcdf-c.
    #[envconfig(from = "BEACON_NETCDF_USE_RUST_READER", default = "true")]
    netcdf_use_rust_reader: bool,

    /// Read HDF5 with the pure-Rust reader instead of netcdf-c.
    ///
    /// On by default. It reads in parallel. It opens an HDF5 file in an object
    /// store (s3, gs or az). It reports the statistics of each file. It also
    /// reports two layouts that netcdf-c cannot: a nested group and a compound
    /// dataset.
    ///
    /// Set it to false to read with netcdf-c. A NetCDF-4 file is an HDF5 file,
    /// so the netcdf-c HDF5 dispatch opens a plain HDF5 file too. Writes always
    /// use netcdf-c.
    ///
    /// This flag is separate from `BEACON_NETCDF_USE_RUST_READER`. A server can
    /// change one format at a time.
    #[envconfig(from = "BEACON_HDF5_USE_RUST_READER", default = "true")]
    hdf5_use_rust_reader: bool,
    #[envconfig(from = "BEACON_HDF5_ENABLE_STATISTICS", default = "true")]
    hdf5_enable_statistics: bool,
    #[envconfig(from = "BEACON_HDF5_USE_READER_CACHE", default = "true")]
    hdf5_use_reader_cache: bool,
    #[envconfig(from = "BEACON_HDF5_READER_CACHE_SIZE", default = "128")]
    hdf5_reader_cache_size: usize,

    /// Compute per-file statistics for Zarr stores.
    ///
    /// On by default. A store answers from its `actual_range` metadata where it
    /// can, and otherwise reads only its rank-0 and rank-1 arrays — the
    /// coordinates a `WHERE` clause names. A data grid of rank 2 or higher is
    /// never read, so a scan costs what it always did. Turn it off for a
    /// collection of many small stores, where even a rank-1 read per store adds
    /// up.
    #[envconfig(from = "BEACON_ZARR_ENABLE_STATISTICS", default = "true")]
    zarr_enable_statistics: bool,

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

    // File statistics subsystem
    //
    // A pass records the column range of each file. A query then skips a file
    // that its predicate cannot match.
    //
    // On by default. netcdf-c reports no range, because it holds one lock for
    // each call in the process. Both Rust readers are the default now, so a
    // pass records a real range. The same store holds the schema cache. With
    // the subsystem off, a query reads the schema of each file again.
    //
    // Set it to false for an archive of formats that supply no range. ODV, CSV
    // and TIFF record zero columns, so a pass costs and returns nothing.
    #[envconfig(from = "BEACON_FILE_STATS_ENABLE", default = "true")]
    file_stats_enable: bool,
    #[envconfig(from = "BEACON_FILE_STATS_INTERVAL_SECS", default = "900")]
    file_stats_interval_secs: u64,
    /// Collect at boot. Do not wait for the first tick.
    ///
    /// Set it for a fresh server, or for one that restarts more often than the
    /// interval. The first tick is one interval after boot, and the interval
    /// starts again on each boot. Such a server holds no statistics at all.
    ///
    /// Off by default. The pass holds the database file while it reads a batch.
    /// A caller that drops a runtime and opens the same file again gets a lock
    /// error. A server that exits does not see this, and can set the flag.
    #[envconfig(from = "BEACON_FILE_STATS_ON_STARTUP", default = "false")]
    file_stats_on_startup: bool,
    /// Files analyzed at once. Empty takes a quarter of the cores, which leaves
    /// room for queries. Raise it well above the core count for datasets in
    /// object storage, where the work is waiting rather than parsing.
    #[envconfig(from = "BEACON_FILE_STATS_CONCURRENCY")]
    file_stats_concurrency: Option<usize>,
    #[envconfig(from = "BEACON_FILE_STATS_BATCH_FILES", default = "10000")]
    file_stats_batch_files: usize,
    #[envconfig(from = "BEACON_FILE_STATS_TARGET_GROUP_FILES", default = "10000")]
    file_stats_target_group_files: usize,
    #[envconfig(from = "BEACON_FILE_STATS_MIN_GROUP_FILES", default = "500")]
    file_stats_min_group_files: usize,
    /// Fix the segment grouping at this directory depth. Leave unset: the
    /// derivation handles roots of differing shape, which one depth cannot.
    #[envconfig(from = "BEACON_FILE_STATS_PREFIX_DEPTH")]
    file_stats_prefix_depth: Option<usize>,
    #[envconfig(from = "BEACON_FILE_STATS_SCAN_PREFIX", default = "")]
    file_stats_scan_prefix: String,
    #[envconfig(from = "BEACON_FILE_STATS_DISCOVERY_CHUNK", default = "10000")]
    file_stats_discovery_chunk: usize,
    /// Keep the schema of each file, so a query reads it instead of the file.
    /// On by default. A pass computes each schema already, and dropped it
    /// before. Only a pass writes an entry, so this flag does nothing while
    /// `BEACON_FILE_STATS_ENABLE` is false. Set it to false to remove the cache
    /// from the query path, and keep the ranges.
    #[envconfig(from = "BEACON_FILE_STATS_SCHEMA_CACHE", default = "true")]
    file_stats_schema_cache: bool,

    // OpenAPI documentation metadata
    #[envconfig(from = "BEACON_API_TITLE", default = "Beacon Rest API")]
    api_title: String,
    #[envconfig(
        from = "BEACON_API_DESCRIPTION",
        default = "Beacon HTTP API. Exposes read-only client endpoints for querying the Beacon runtime (datasets, tables, functions, SQL queries) and authenticated admin endpoints for managing tables and dataset files."
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
                worker_threads: raw.worker_threads,
                base_path: raw.base_path,
                web_ui_dir: raw.web_ui_dir,
                max_upload_bytes: raw.max_upload_bytes,
                log_level: raw.log_level,
            },
            runtime: RuntimeConfig {
                vm_memory_size: raw.vm_memory_size,
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
                use_rust_reader: raw.netcdf_use_rust_reader,
            },
            hdf5: Hdf5Config {
                use_rust_reader: raw.hdf5_use_rust_reader,
                use_reader_cache: raw.hdf5_use_reader_cache,
                reader_cache_size: raw.hdf5_reader_cache_size,
                enable_statistics: raw.hdf5_enable_statistics,
            },
            zarr: ZarrConfig {
                enable_statistics: raw.zarr_enable_statistics,
            },
            bbf: BbfConfig {
                split_streams_slice: raw.bbf_split_streams_slice,
            },
            file_stats: FileStatsConfig {
                enable: raw.file_stats_enable,
                interval_secs: raw.file_stats_interval_secs,
                on_startup: raw.file_stats_on_startup,
                concurrency: raw
                    .file_stats_concurrency
                    .filter(|n| *n > 0)
                    .unwrap_or_else(beacon_common::file_stats_config::default_concurrency),
                batch_files: raw.file_stats_batch_files.max(1),
                target_group_files: raw.file_stats_target_group_files.max(1),
                min_group_files: raw.file_stats_min_group_files,
                prefix_depth: raw.file_stats_prefix_depth,
                scan_prefix: raw.file_stats_scan_prefix.clone(),
                discovery_chunk: raw.file_stats_discovery_chunk.max(1),
                schema_cache: raw.file_stats_schema_cache,
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
                    db_file: root.join("tables").join("beacon.db"),
                    tmp: root.join("tmp"),
                }
            },
            s3: S3Config {
                datasets_on_s3: raw.s3_datasets || raw.s3_data_lake_deprecated,
                bucket: raw.s3_bucket,
                enable_virtual_hosting: raw.s3_enable_virtual_hosting,
                allow_http: raw.s3_allow_http,
                endpoint: raw.aws_endpoint,
                region: raw.aws_region,
            },
            // Decoded and validated in `Config::load` (see `secrets_key`).
            secrets: SecretsConfig { master_key: None },
        }
    }
}

/// Rejects storage settings that cannot produce a working datasets store.
///
/// An S3 datasets store without a bucket would otherwise surface as an opaque object-store
/// error on the first query rather than at startup.
fn validate_storage(s3: &S3Config) -> Result<()> {
    if s3.datasets_on_s3 && s3.bucket.is_none() {
        return Err(ConfigError::InvalidStorage(
            "BEACON_S3_DATASETS is set but BEACON_S3_BUCKET is missing; \
             the bucket is never inferred from AWS_ENDPOINT"
                .to_string(),
        ));
    }
    Ok(())
}

/// Levels accepted by `BEACON_LOG_LEVEL`, in the spelling `tracing` expects.
const LOG_LEVELS: [&str; 6] = ["trace", "debug", "info", "warn", "error", "off"];

/// Lowercases and validates `BEACON_LOG_LEVEL`, so `DEBUG`, `Debug`, and `debug`
/// all work.
///
/// Errors on an unknown level instead of falling back to the default: a typo that
/// silently keeps the server at `info` is the failure this variable exists to
/// avoid.
fn normalize_log_level(raw: &str) -> std::result::Result<String, String> {
    let level = raw.trim().to_ascii_lowercase();
    if LOG_LEVELS.contains(&level.as_str()) {
        return Ok(level);
    }
    Err(format!(
        "`{raw}` is not a log level; expected one of {}",
        LOG_LEVELS.join(", ")
    ))
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
        config.server.log_level =
            normalize_log_level(&config.server.log_level).map_err(ConfigError::InvalidLogLevel)?;

        validate_storage(&config.s3)?;

        // Create the configured data directories (idempotent). `db_file` is a file,
        // not a directory — its parent is created here. The local datasets
        // directory is skipped when the datasets store lives in S3.
        let mut dirs = vec![config.data.tmp.clone()];
        if let Some(parent) = config.data.db_file.parent() {
            dirs.push(parent.to_path_buf());
        }
        if !config.s3.datasets_on_s3 {
            dirs.push(config.data.datasets.clone());
        }
        for dir in &dirs {
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
    use super::{
        Config, PathBuf, RawConfig, decode_master_key, normalize_base_path, normalize_log_level,
        validate_storage,
    };
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

        assert_eq!(config.data.datasets, root.join("datasets"));
        assert_eq!(config.data.db_file, root.join("tables").join("beacon.db"));
        assert_eq!(config.data.tmp, root.join("tmp"));
    }

    /// The datasets store is local unless `BEACON_S3_DATASETS` opts in, and the
    /// bucket is never guessed from the endpoint.
    #[test]
    fn s3_datasets_store_is_opt_in() {
        assert!(!config(&[]).s3.datasets_on_s3);
        assert!(config(&[]).s3.bucket.is_none());

        let s3 = config(&[
            ("BEACON_S3_DATASETS", "true"),
            ("BEACON_S3_BUCKET", "my-bucket"),
        ])
        .s3;
        assert!(s3.datasets_on_s3);
        assert_eq!(s3.bucket.as_deref(), Some("my-bucket"));
        // Path-style addressing and plain HTTP are the defaults (local MinIO).
        assert!(!s3.enable_virtual_hosting);
        assert!(s3.allow_http);
    }

    /// The native-reader base URL must address the same bucket the object store
    /// does, under every combination of endpoint and addressing style.
    #[test]
    fn native_base_url_mirrors_the_object_store_addressing() {
        let with = |vars: &[(&str, &str)]| config(vars).s3.native_base_url();

        // A custom endpoint (MinIO): path-style appends the bucket, virtual-hosted
        // splices it in as the leading host label — port and scheme preserved.
        let minio = [
            ("BEACON_S3_DATASETS", "true"),
            ("BEACON_S3_BUCKET", "datasets"),
            ("AWS_ENDPOINT", "http://minio:9000"),
        ];
        assert_eq!(with(&minio).as_deref(), Some("http://minio:9000/datasets"));

        let mut virtual_minio = minio.to_vec();
        virtual_minio.push(("BEACON_S3_ENABLE_VIRTUAL_HOSTING", "true"));
        assert_eq!(
            with(&virtual_minio).as_deref(),
            Some("http://datasets.minio:9000")
        );

        // A trailing slash on the endpoint must not double up.
        let mut slashed = minio.to_vec();
        slashed[2] = ("AWS_ENDPOINT", "http://minio:9000/");
        assert_eq!(
            with(&slashed).as_deref(),
            Some("http://minio:9000/datasets")
        );

        // No endpoint: real AWS, where the region is part of the hostname.
        let aws = [
            ("BEACON_S3_DATASETS", "true"),
            ("BEACON_S3_BUCKET", "datasets"),
            ("AWS_REGION", "eu-west-1"),
        ];
        assert_eq!(
            with(&aws).as_deref(),
            Some("https://s3.eu-west-1.amazonaws.com/datasets")
        );

        let mut virtual_aws = aws.to_vec();
        virtual_aws.push(("BEACON_S3_ENABLE_VIRTUAL_HOSTING", "true"));
        assert_eq!(
            with(&virtual_aws).as_deref(),
            Some("https://datasets.s3.eu-west-1.amazonaws.com")
        );

        // Without a bucket there is nothing to address.
        assert_eq!(with(&[]), None);
    }

    /// An S3 datasets store with no bucket is rejected at load time rather than failing as
    /// an opaque object-store error on the first query.
    #[test]
    fn s3_lake_without_a_bucket_is_rejected() {
        let err = validate_storage(&config(&[("BEACON_S3_DATASETS", "true")]).s3)
            .expect_err("an S3 datasets store with no bucket must not load");
        assert!(err.to_string().contains("BEACON_S3_BUCKET"), "got: {err}");

        // With a bucket it passes, and a local datasets store never needs one.
        validate_storage(
            &config(&[
                ("BEACON_S3_DATASETS", "true"),
                ("BEACON_S3_BUCKET", "my-bucket"),
            ])
            .s3,
        )
        .expect("bucket supplied");
        validate_storage(&config(&[]).s3).expect("local server needs no bucket");
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
        assert_eq!(config(&[("BEACON_BATCH_SIZE", "0")]).runtime.batch_size, 0);
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
        assert!(
            !printed.contains("ab, ab"),
            "raw key bytes leaked: {printed}"
        );
    }

    /// Statistics default on. The collection at boot does not. It holds the
    /// database file after a drop, so it stays off until teardown waits for it.
    #[test]
    fn statistics_default_on_and_the_startup_collection_stays_opt_in() {
        assert!(config(&[]).file_stats.enable);
        assert!(!config(&[]).file_stats.on_startup);
        assert!(
            config(&[("BEACON_FILE_STATS_ON_STARTUP", "true")])
                .file_stats
                .on_startup
        );
        assert!(
            !config(&[("BEACON_FILE_STATS_ENABLE", "false")])
                .file_stats
                .enable
        );
    }

    #[test]
    fn log_level_defaults_to_info() {
        assert_eq!(config(&[]).server.log_level, "info");
    }

    #[test]
    fn log_level_accepts_any_case() {
        assert_eq!(normalize_log_level("DEBUG"), Ok("debug".to_string()));
        assert_eq!(normalize_log_level("Trace"), Ok("trace".to_string()));
        assert_eq!(normalize_log_level(" warn "), Ok("warn".to_string()));
    }

    /// A typo must stop the server, not leave it quietly at `info`.
    #[test]
    fn unknown_log_level_is_an_error() {
        assert!(normalize_log_level("verbose").is_err());
        assert!(normalize_log_level("").is_err());
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
