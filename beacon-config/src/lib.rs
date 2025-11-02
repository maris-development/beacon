use std::{env, path::PathBuf, sync::Arc};

use envconfig::Envconfig;
use lazy_static::lazy_static;
use object_store::local::LocalFileSystem;

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
    #[envconfig(from = "BEACON_ENABLE_SQL", default = "false")]
    pub enable_sql: bool,
    #[envconfig(from = "BEACON_SANITIZE_SCHEMA", default = "false")]
    pub sanitize_schema: bool,
    #[envconfig(from = "BEACON_ST_WITHIN_POINT_CACHE_SIZE", default = "10000")]
    pub st_within_point_cache_size: usize,
    #[envconfig(from = "BEACON_WORKER_THREADS", default = "8")]
    pub worker_threads: usize,
    #[envconfig(from = "BEACON_DEFERRED_STREAM_CAPACITY", default = "128")]
    pub deferred_stream_capacity: usize,

    // S3 Settings
    #[envconfig(from = "BEACON_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,
    #[envconfig(from = "BEACON_S3_REGION")]
    pub s3_region: Option<String>,
    #[envconfig(from = "BEACON_S3_BUCKET")]
    pub s3_bucket: Option<String>,
    #[envconfig(from = "BEACON_S3_ACCESS_KEY_ID")]
    pub s3_access_key_id: Option<String>,
    #[envconfig(from = "BEACON_S3_SECRET_ACCESS_KEY")]
    pub s3_secret_access_key: Option<String>,
    #[envconfig(from = "BEACON_S3_DATA_LAKE", default = "false")]
    pub s3_data_lake: bool,

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
}

impl Config {
    pub fn init() -> Config {
        Config::init_from_env().expect("Failed to load config")
    }
}

lazy_static! {
    pub static ref CONFIG: Config = Config::init();
    pub static ref DATA_DIR: PathBuf = PathBuf::from("./data/");
    pub static ref OBJECT_STORE_LOCAL_FS: Arc<LocalFileSystem> = {
        //Create the dir if it doesn't exist
        std::fs::create_dir_all(DATA_DIR.as_path()).expect("Failed to create data dir");
        Arc::new(LocalFileSystem::new_with_prefix(DATA_DIR.clone())
            .expect("Failed to create local file system. Is the data dir set correctly?"))
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
