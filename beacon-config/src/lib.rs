use std::path::PathBuf;

use envconfig::Envconfig;
use lazy_static::lazy_static;

#[derive(Debug)]
pub struct Config {
    pub admin: AdminConfig,
    pub server: ServerConfig,
    pub runtime: RuntimeConfig,
    pub sql: SqlConfig,
    pub flight_sql: FlightSqlConfig,
    pub storage: StorageConfig,
    pub cors: CorsConfig,
    pub netcdf: NetcdfConfig,
}

#[derive(Debug)]
pub struct AdminConfig {
    pub username: String,
    pub password: String,
}

#[derive(Debug)]
pub struct ServerConfig {
    pub port: u16,
    pub host: String,
    pub log_level: String,
    pub worker_threads: usize,
}

#[derive(Debug)]
pub struct RuntimeConfig {
    pub vm_memory_size: usize,
    pub table_sync_interval_secs: u64,
    pub sanitize_schema: bool,
    pub st_within_point_cache_size: usize,
    pub enable_sys_info: bool,
    pub batch_size: usize,
    pub bbf_split_streams_slice: bool,
}

#[derive(Debug)]
pub struct SqlConfig {
    pub enable: bool,
    pub default_table: String,
    pub enable_pushdown_projection: bool,
    pub stream_coalesce: SqlStreamCoalesceConfig,
}

#[derive(Debug)]
pub struct SqlStreamCoalesceConfig {
    pub enabled: bool,
    pub target_rows: usize,
    pub flush_timeout_ms: u64,
    pub max_rows: usize,
}

#[derive(Debug)]
pub struct FlightSqlConfig {
    pub enable: bool,
    pub allow_anonymous: bool,
    pub host: String,
    pub port: u16,
    pub token_ttl_secs: u64,
    pub statement_ttl_secs: u64,
    pub prepared_statement_ttl_secs: u64,
}

#[derive(Debug)]
pub struct StorageConfig {
    pub enable_fs_events: bool,
    pub enable_s3_events: bool,
    pub s3: S3Config,
}

#[derive(Debug)]
pub struct S3Config {
    pub bucket: Option<String>,
    pub enable_virtual_hosting: bool,
    pub data_lake: bool,
}

#[derive(Debug)]
pub struct CorsConfig {
    pub allowed_methods: String,
    pub allowed_origins: String,
    pub allowed_headers: String,
    pub allowed_credentials: bool,
    pub max_age: u64,
}

#[derive(Debug)]
pub struct NetcdfConfig {
    pub use_schema_cache: bool,
    pub schema_cache_size: u64,
    pub use_reader_cache: bool,
    pub reader_cache_size: usize,
}

#[derive(Debug)]
pub struct NetcdfMultiplexerConfig {
    pub enabled: bool,
    pub processes: Option<usize>,
}

#[derive(Debug, Envconfig)]
struct RawConfig {
    #[envconfig(from = "BEACON_ADMIN_USERNAME", default = "beacon-admin")]
    admin_username: String,
    #[envconfig(from = "BEACON_ADMIN_PASSWORD", default = "beacon-password")]
    admin_password: String,
    #[envconfig(from = "BEACON_PORT", default = "5001")]
    port: u16,
    #[envconfig(from = "BEACON_HOST", default = "0.0.0.0")]
    host: String,
    #[envconfig(from = "BEACON_LOG_LEVEL", default = "info")]
    log_level: String,

    //VM Settings
    #[envconfig(from = "BEACON_VM_MEMORY_SIZE", default = "4096")]
    vm_memory_size: usize,
    #[envconfig(from = "BEACON_DEFAULT_TABLE", default = "default")]
    default_table: String,
    #[envconfig(from = "BEACON_TABLE_SYNC_INTERVAL_SECS", default = "300")] // 5 minutes
    table_sync_interval_secs: u64,
    #[envconfig(from = "BEACON_SANITIZE_SCHEMA", default = "false")]
    sanitize_schema: bool,
    #[envconfig(from = "BEACON_ENABLE_SQL", default = "false")]
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

    #[envconfig(from = "BEACON_S3_BUCKET")]
    s3_bucket: Option<String>,
    #[envconfig(from = "BEACON_S3_ENABLE_VIRTUAL_HOSTING", default = "false")]
    s3_enable_virtual_hosting: bool,
    #[envconfig(from = "BEACON_S3_DATA_LAKE", default = "false")]
    s3_data_lake: bool,
    #[envconfig(from = "BEACON_ENABLE_FS_EVENTS", default = "false")]
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
    #[envconfig(from = "BEACON_CORS_ALLOWED_CREDENTIALS", default = "false")]
    allowed_credentials: bool,
    #[envconfig(from = "BEACON_CORS_MAX_AGE", default = "3600")]
    max_age: u64,
    #[envconfig(from = "BEACON_ENABLE_PUSHDOWN_PROJECTION", default = "false")]
    enable_pushdown_projection: bool,

    #[envconfig(from = "BEACON_NETCDF_USE_SCHEMA_CACHE", default = "true")]
    netcdf_use_schema_cache: bool,
    #[envconfig(from = "BEACON_NETCDF_SCHEMA_CACHE_SIZE", default = "1024")]
    netcdf_schema_cache_size: u64,

    #[envconfig(from = "BEACON_NETCDF_USE_READER_CACHE", default = "true")]
    netcdf_use_reader_cache: bool,
    #[envconfig(from = "BEACON_NETCDF_READER_CACHE_SIZE", default = "128")]
    netcdf_reader_cache_size: usize,

    /// The batch size for NetCDF reads, in number of rows. This is used for both local and MPIO reads.
    #[envconfig(from = "BEACON_BATCH_SIZE", default = "64000")]
    beacon_batch_size: usize,

    /// Whether to split streams into 16k row slices for better memory management and parallelism.
    #[envconfig(from = "BEACON_ENABLE_BBF_SPLIT_STREAMS_SLICE", default = "false")]
    bbf_split_streams_slice: bool,
}

impl From<RawConfig> for Config {
    fn from(raw: RawConfig) -> Self {
        Self {
            admin: AdminConfig {
                username: raw.admin_username,
                password: raw.admin_password,
            },
            server: ServerConfig {
                port: raw.port,
                host: raw.host,
                log_level: raw.log_level,
                worker_threads: raw.worker_threads,
            },
            runtime: RuntimeConfig {
                vm_memory_size: raw.vm_memory_size,
                table_sync_interval_secs: raw.table_sync_interval_secs,
                sanitize_schema: raw.sanitize_schema,
                st_within_point_cache_size: raw.st_within_point_cache_size,
                enable_sys_info: raw.enable_sys_info,
                batch_size: raw.beacon_batch_size,
                bbf_split_streams_slice: raw.bbf_split_streams_slice,
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
            storage: StorageConfig {
                enable_fs_events: raw.enable_fs_events,
                enable_s3_events: raw.enable_s3_events,
                s3: S3Config {
                    bucket: raw.s3_bucket,
                    enable_virtual_hosting: raw.s3_enable_virtual_hosting,
                    data_lake: raw.s3_data_lake,
                },
            },
            cors: CorsConfig {
                allowed_methods: raw.allowed_methods,
                allowed_origins: raw.allowed_origins,
                allowed_headers: raw.allowed_headers,
                allowed_credentials: raw.allowed_credentials,
                max_age: raw.max_age,
            },
            netcdf: NetcdfConfig {
                use_schema_cache: raw.netcdf_use_schema_cache,
                schema_cache_size: raw.netcdf_schema_cache_size,
                use_reader_cache: raw.netcdf_use_reader_cache,
                reader_cache_size: raw.netcdf_reader_cache_size,
            },
        }
    }
}

impl Config {
    pub fn init() -> Config {
        RawConfig::init_from_env()
            .map(Into::into)
            .expect("Failed to load config")
    }
}

lazy_static! {
    pub static ref CONFIG: Config = Config::init();
    pub static ref DATA_DIR: PathBuf = {
        std::fs::create_dir_all("./data").expect("Failed to create data dir");
        PathBuf::from("./data")
    };
    /// The path to the datasets directory
    pub static ref DATASETS_DIR_PATH: PathBuf = {
        //Create the dir if it doesn't exist
        let dir = DATA_DIR.join("datasets");
        std::fs::create_dir_all(&dir).expect("Failed to create datasets dir");
        dir
    };
    /// The prefix for the datasets directory for object store paths
    pub static ref DATASETS_DIR_PREFIX: object_store::path::Path =
        object_store::path::Path::from("datasets");

    pub static ref TABLES_DIR_PREFIX: object_store::path::Path =
        object_store::path::Path::from("tables");
    pub static ref TABLES_DIR: PathBuf = {
        let dir = DATA_DIR.join("tables");
        std::fs::create_dir_all(&dir).expect("Failed to create tables dir");
        dir
    };

    pub static ref TMP_DIR: PathBuf = {
        let dir = DATA_DIR.join("tmp");
        std::fs::create_dir_all(&dir).expect("Failed to create tmp dir");
        dir
    };


    /// The path to the indexes directory
    pub static ref INDEX_DIR_PATH: PathBuf = {
        //Create the dir if it doesn't exist
        let dir = DATA_DIR.join("indexes");
        std::fs::create_dir_all(&dir).expect("Failed to create indexes dir");
        dir
    };
    /// The prefix for the indexes directory for object store paths
    pub static ref INDEX_DIR_PREFIX: object_store::path::Path =
        object_store::path::Path::from("indexes");

    /// The path to the cache directory
    pub static ref CACHE_DIR_PATH: PathBuf = {
        //Create the dir if it doesn't exist
        let dir = DATA_DIR.join("cache");
        std::fs::create_dir_all(&dir).expect("Failed to create cache dir");
        dir
    };
    /// The prefix for the cache directory for object store paths
    pub static ref CACHE_DIR_PREFIX: object_store::path::Path =
        object_store::path::Path::from("cache");
}
