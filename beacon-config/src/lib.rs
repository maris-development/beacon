use envconfig::Envconfig;
use lazy_static::lazy_static;

#[derive(Debug, Envconfig)]
pub struct Config {
    #[envconfig(from = "BEACON_PORT", default = "5001")]
    pub port: u16,
    #[envconfig(from = "BEACON_HOST", default = "0.0.0.0")]
    pub host: String,
    #[envconfig(from = "BEACON_LOG_LEVEL", default = "info")]
    pub log_level: String,
    #[envconfig(from = "BEACON_DATASETS_DIR", default = "./datasets")]
    pub datasets_dir: String,
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
}
