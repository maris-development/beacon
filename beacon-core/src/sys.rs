#[derive(Debug, serde::Serialize)]
pub struct SystemInfo {
    pub version: String,
    pub system_info: sysinfo::System,
}

impl SystemInfo {
    pub fn new() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            system_info: sysinfo::System::new_all(),
        }
    }
}
