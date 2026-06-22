/// Beacon runtime system information returned by `GET /api/info`.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct SystemInfo {
    /// The running Beacon build version (the `beacon-core` crate version).
    pub beacon_version: String,
    /// Host resource snapshot (CPUs, memory, processes) gathered via `sysinfo`.
    /// Present only when the runtime is configured to expose host metrics;
    /// otherwise `null`.
    #[schema(value_type = Option<Object>)]
    pub system_info: Option<sysinfo::System>,
}

impl SystemInfo {
    pub fn new(enable_sys_info: bool) -> Self {
        let sys_info = if enable_sys_info {
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

impl Default for SystemInfo {
    fn default() -> Self {
        Self::new(false)
    }
}
