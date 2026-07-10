//! Model Context Protocol (MCP) server for beacon.
//!
//! Exposes beacon as an MCP server over the streamable-HTTP transport so MCP
//! clients (e.g. Claude) can discover tables and run read-only queries. The tool
//! surface is generated from the runtime: a few generic tools plus one tool per
//! table that opts in via its `mcp` table extension (see
//! [`beacon_core::extensions`]). Presets declared on a table become a typed
//! `preset` parameter that expands to the stored filters.
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
/// (e.g. `Router::route_service("/mcp", beacon_mcp::streamable_http_service(rt))`).
pub fn streamable_http_service(
    runtime: Arc<Runtime>,
) -> StreamableHttpService<BeaconMcpServer, LocalSessionManager> {
    StreamableHttpService::new(
        move || Ok(BeaconMcpServer::new(runtime.clone())),
        Arc::new(LocalSessionManager::default()),
        // rmcp's default host allowlist only accepts loopback (DNS-rebinding
        // protection for locally-bound servers). Beacon is served publicly
        // behind TLS + its own auth/RBAC, so accept any Host header rather than
        // reject requests to the deployment's real hostname with a 403.
        StreamableHttpServerConfig::default().disable_allowed_hosts(),
    )
}
