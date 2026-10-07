//! Model Context Protocol (MCP) server for beacon.
//!
//! Exposes beacon as an MCP server over the streamable-HTTP transport so MCP
//! clients (e.g. Claude) can discover tables and run read-only queries. The tool
//! surface is fixed. The agent learns what each table and column means from
//! their comments (`COMMENT ON`, see [`beacon_core::comments`]).
//!
//! All execution flows through [`beacon_core::runtime::Runtime::run_query`] as a
//! non-super-user, so only read-only `SELECT`s are permitted.

mod catalog;
mod result;
mod server;

use std::sync::Arc;

use beacon_core::runtime::Runtime;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};

pub use server::BeaconMcpServer;

/// Build the MCP streamable-HTTP tower service, ready to mount in an axum router
/// (e.g. `Router::route_service("/mcp", beacon_mcp::streamable_http_service(rt, &[]))`).
///
/// # Arguments
///
/// * `runtime` - The runtime that runs every tool call.
/// * `allowed_hosts` - `Host` values accepted in addition to the loopback hosts,
///   as `host` or `host:port`. An entry `*` accepts every host.
pub fn streamable_http_service(
    runtime: Arc<Runtime>,
    allowed_hosts: &[String],
) -> StreamableHttpService<BeaconMcpServer, LocalSessionManager> {
    StreamableHttpService::new(
        move || Ok(BeaconMcpServer::new(runtime.clone())),
        Arc::new(LocalSessionManager::default()),
        http_config(allowed_hosts),
    )
}

/// The transport config: rmcp accepts only loopback hosts by default, against DNS
/// rebinding, so a public name must be listed.
fn http_config(allowed_hosts: &[String]) -> StreamableHttpServerConfig {
    let mut config = StreamableHttpServerConfig::default();
    if allowed_hosts.iter().any(|host| host == "*") {
        // An empty list switches the check off in rmcp.
        config = config.disable_allowed_hosts();
    } else {
        config.allowed_hosts.extend(allowed_hosts.iter().cloned());
    }
    config
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_keeps_only_the_loopback_hosts() {
        let config = http_config(&[]);

        assert_eq!(config.allowed_hosts, ["localhost", "127.0.0.1", "::1"]);
    }

    #[test]
    fn listed_hosts_join_the_loopback_hosts() {
        let config = http_config(&["beacon.example.org".to_string()]);

        assert_eq!(
            config.allowed_hosts,
            ["localhost", "127.0.0.1", "::1", "beacon.example.org"]
        );
    }

    #[test]
    fn wildcard_switches_the_host_check_off() {
        let config = http_config(&["a.example.org".to_string(), "*".to_string()]);

        assert!(config.allowed_hosts.is_empty());
    }
}
