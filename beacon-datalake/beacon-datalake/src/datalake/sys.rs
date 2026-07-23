//! Host and build information reported by `GET /api/info`.
//!
//! This is application telemetry, not engine state: the runtime has no opinion
//! about the machine it runs on, so the lake owns it.

/// Beacon system information returned by `GET /api/info`.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct SystemInfo {
    /// The running beacon build version.
    pub beacon_version: String,
    /// Host resource snapshot (CPUs, memory, processes) gathered via `sysinfo`.
    /// Present only when `BEACON_ENABLE_SYS_INFO` is set; otherwise `null`.
    #[schema(value_type = Option<Object>)]
    pub system_info: Option<sysinfo::System>,
}

impl SystemInfo {
    pub fn new(enable_sys_info: bool) -> Self {
        Self {
            beacon_version: env!("CARGO_PKG_VERSION").to_string(),
            system_info: enable_sys_info.then(sysinfo::System::new_all),
        }
    }
}

impl Default for SystemInfo {
    fn default() -> Self {
        Self::new(false)
    }
}
