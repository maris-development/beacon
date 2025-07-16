#[derive(Debug, serde::Serialize)]
pub struct SystemInfo {
    pub beacon_version: String,
    pub system_info: Option<sysinfo::System>,
}

impl SystemInfo {
    pub fn new() -> Self {
        let sys_info = if beacon_config::CONFIG.enable_sys_info {
            Some(sysinfo::System::new_all())
        } else {
            None
        };

        Self {
            beacon_version: env!("CARGO_PKG_VERSION").to_string(),
            system_info: sys_info,
        }
    }
}
