use std::{path::PathBuf, sync::Arc};

use envconfig::Envconfig;
use lazy_static::lazy_static;
use object_store::local::LocalFileSystem;

#[derive(Debug, Envconfig)]
pub struct Config {
    #[envconfig(from = "BEACON_PORT", default = "5001")]
    pub port: u16,
    #[envconfig(from = "BEACON_HOST", default = "0.0.0.0")]
    pub host: String,
    #[envconfig(from = "BEACON_LOG_LEVEL", default = "info")]
    pub log_level: String,
    //Memory size in MB
    #[envconfig(from = "BEACON_VM_MEMORY_SIZE", default = "512")]
    pub vm_memory_size: usize,
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
        Arc::new(LocalFileSystem::new_with_prefix(DATA_DIR.clone())
            .expect("Failed to create local file system. Is the data dir set correctly?"))
    };
    // Prefixes for the different directories
    /// The path to the temporary directory
    pub static ref TMP_DIR_PATH: PathBuf = DATA_DIR.join("tmp");
    /// The prefix for the temporary directory for object store paths
    pub static ref TMP_DIR_PATH_PREFIX: object_store::path::Path =
        object_store::path::Path::from("/tmp/");

    /// The path to the datasets directory
    pub static ref DATASETS_DIR_PATH: PathBuf = DATA_DIR.join("datasets/");
    /// The prefix for the datasets directory for object store paths
    pub static ref DATASETS_DIR_PREFIX: object_store::path::Path =
        object_store::path::Path::from("datasets");

    /// The path to the indexes directory
    pub static ref INDEX_DIR_PATH: PathBuf = DATA_DIR.join("indexes");
    /// The prefix for the indexes directory for object store paths
    pub static ref INDEX_DIR_PREFIX: object_store::path::Path =
        object_store::path::Path::from("indexes");

    /// The path to the cache directory
    pub static ref CACHE_DIR_PATH: PathBuf = DATA_DIR.join("cache");
    /// The prefix for the cache directory for object store paths
    pub static ref CACHE_DIR_PREFIX: object_store::path::Path =
        object_store::path::Path::from("cache");
}
