use std::path::PathBuf;

use envconfig::Envconfig;
use lazy_static::lazy_static;

#[derive(Debug, Envconfig)]
pub struct Config {
    // Server settings
    #[envconfig(from = "BEACON_ADMIN_USERNAME", default = "beacon-admin")]
    pub admin_username: String,
    #[envconfig(from = "BEACON_ADMIN_PASSWORD", default = "beacon-password")]
    pub admin_password: String,
    #[envconfig(from = "BEACON_PORT", default = "5001")]
    pub port: u16,
    #[envconfig(from = "BEACON_HOST", default = "0.0.0.0")]
    pub host: String,
    #[envconfig(from = "BEACON_LOG_LEVEL", default = "info")]
    pub log_level: String,

    //VM Settings
    #[envconfig(from = "BEACON_VM_MEMORY_SIZE", default = "4096")]
    pub vm_memory_size: usize,
    #[envconfig(from = "BEACON_DEFAULT_TABLE", default = "default")]
    pub default_table: String,
    #[envconfig(from = "BEACON_TABLE_SYNC_INTERVAL_SECS", default = "300")] // 5 minutes
    pub table_sync_interval_secs: u64,
    #[envconfig(from = "BEACON_SANITIZE_SCHEMA", default = "false")]
    pub sanitize_schema: bool,
    #[envconfig(from = "BEACON_ENABLE_SQL", default = "false")]
    pub enable_sql: bool,
    #[envconfig(from = "BEACON_FLIGHT_SQL_ENABLE", default = "true")]
    pub flight_sql_enable: bool,
    #[envconfig(from = "BEACON_FLIGHT_SQL_ALLOW_ANONYMOUS", default = "false")]
    pub flight_sql_allow_anonymous: bool,
    #[envconfig(from = "BEACON_FLIGHT_SQL_HOST", default = "0.0.0.0")]
    pub flight_sql_host: String,
    #[envconfig(from = "BEACON_FLIGHT_SQL_PORT", default = "32011")]
    pub flight_sql_port: u16,
    #[envconfig(from = "BEACON_FLIGHT_SQL_TOKEN_TTL_SECS", default = "3600")]
    pub flight_sql_token_ttl_secs: u64,
    #[envconfig(from = "BEACON_FLIGHT_SQL_STATEMENT_TTL_SECS", default = "300")]
    pub flight_sql_statement_ttl_secs: u64,
    #[envconfig(
        from = "BEACON_FLIGHT_SQL_PREPARED_STATEMENT_TTL_SECS",
        default = "900"
    )]
    pub flight_sql_prepared_statement_ttl_secs: u64,
    #[envconfig(from = "BEACON_SQL_STREAM_COALESCE_ENABLED", default = "true")]
    pub sql_stream_coalesce_enabled: bool,
    #[envconfig(from = "BEACON_SQL_STREAM_COALESCE_TARGET_ROWS", default = "65536")]
    pub sql_stream_coalesce_target_rows: usize,
    #[envconfig(from = "BEACON_SQL_STREAM_COALESCE_FLUSH_TIMEOUT_MS", default = "25")]
    pub sql_stream_coalesce_flush_timeout_ms: u64,
    #[envconfig(from = "BEACON_SQL_STREAM_COALESCE_MAX_ROWS", default = "262144")]
    pub sql_stream_coalesce_max_rows: usize,
    #[envconfig(from = "BEACON_ST_WITHIN_POINT_CACHE_SIZE", default = "10000")]
    pub st_within_point_cache_size: usize,
    #[envconfig(from = "BEACON_WORKER_THREADS", default = "8")]
    pub worker_threads: usize,

    #[envconfig(from = "BEACON_S3_BUCKET")]
    pub s3_bucket: Option<String>,
    #[envconfig(from = "BEACON_S3_ENABLE_VIRTUAL_HOSTING", default = "false")]
    pub s3_enable_virtual_hosting: bool,
    #[envconfig(from = "BEACON_S3_DATA_LAKE", default = "false")]
    pub s3_data_lake: bool,
    #[envconfig(from = "BEACON_ENABLE_FS_EVENTS", default = "false")]
    pub enable_fs_events: bool,
    #[envconfig(from = "BEACON_ENABLE_S3_EVENTS", default = "false")]
    pub enable_s3_events: bool,

    // Others
    #[envconfig(from = "BEACON_ENABLE_SYS_INFO", default = "false")]
    pub enable_sys_info: bool,
    /// CORS CONFIG
    #[envconfig(
        from = "BEACON_CORS_ALLOWED_METHODS",
        default = "GET,POST,PUT,DELETE,OPTIONS"
    )]
    pub allowed_methods: String,
    #[envconfig(from = "BEACON_CORS_ALLOWED_ORIGINS", default = "*")]
    pub allowed_origins: String,
    #[envconfig(
        from = "BEACON_CORS_ALLOWED_HEADERS",
        default = "Content-Type,Authorization"
    )]
    pub allowed_headers: String,
    #[envconfig(from = "BEACON_CORS_ALLOWED_CREDENTIALS", default = "false")]
    pub allowed_credentials: bool,
    #[envconfig(from = "BEACON_CORS_MAX_AGE", default = "3600")]
    pub max_age: u64,
    #[envconfig(from = "BEACON_ENABLE_PUSHDOWN_PROJECTION", default = "false")]
    pub enable_pushdown_projection: bool,

    #[envconfig(from = "BEACON_NETCDF_USE_SCHEMA_CACHE", default = "true")]
    pub netcdf_use_schema_cache: bool,
    #[envconfig(from = "BEACON_NETCDF_SCHEMA_CACHE_SIZE", default = "1024")]
    pub netcdf_schema_cache_size: u64,

    #[envconfig(from = "BEACON_NETCDF_USE_READER_CACHE", default = "true")]
    pub netcdf_use_reader_cache: bool,
    #[envconfig(from = "BEACON_NETCDF_READER_CACHE_SIZE", default = "128")]
    pub netcdf_reader_cache_size: usize,

    #[envconfig(from = "BEACON_ENABLE_MULTIPLEXER_NETCDF", default = "false")]
    pub enable_multiplexer_netcdf: bool,
    #[envconfig(from = "BEACON_NETCDF_MULTIPLEXER_PROCESSES")]
    pub netcdf_multiplexer_processes: Option<usize>,

    /// gRPC address of a running `beacon-arrow-netcdf-mpio` Arrow Flight server.
    ///
    /// Used by `beacon_formats::netcdf::source::mpio` for `read_schema`,
    /// `read_file_as_stream`, and `read_file_as_batch`.
    /// Example: `http://127.0.0.1:50051`.
    #[envconfig(from = "BEACON_NETCDF_FLIGHT_ADDR")]
    pub netcdf_flight_addr: Option<String>,

    /// The batch size for NetCDF reads, in number of rows. This is used for both local and MPIO reads.
    #[envconfig(from = "BEACON_BATCH_SIZE", default = "64000")]
    pub beacon_batch_size: usize,

    /// Whether to split streams into 16k row slices for better memory management and parallelism.
    #[envconfig(from = "BEACON_ENABLE_BBF_SPLIT_STREAMS_SLICE", default = "false")]
    pub bbf_split_streams_slice: bool,
}

impl Config {
    pub fn init() -> Config {
        Config::init_from_env().expect("Failed to load config")
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
