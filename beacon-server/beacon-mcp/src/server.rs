//! The [`ServerHandler`] implementation: capabilities, tool listing, dispatch.

use std::sync::Arc;

use beacon_core::runtime::Runtime;
use beacon_core::AuthIdentity;
use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, Content, ListToolsResult, PaginatedRequestParams,
    ServerCapabilities, ServerInfo,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer};

/// Sent in `initialize`. Some clients hide or cut it, so `get_guide` holds the full text.
const INSTRUCTIONS: &str = "Beacon is a SQL engine for scientific data. It reads NetCDF, Zarr, \
    Parquet and other files in place. Call `get_guide` once: it explains how Beacon turns arrays \
    into rows and how to use the beacon-api Python client. Call `list_tables` to find the tables, \
    and `describe_table` before you write SQL for a table. `run_sql` is a read-only preview of \
    1000 rows or fewer. Use `export_query` for a large result. Put double quotes around a name \
    with upper case or a dot, such as \"Temperature\" or \"temperature.units\".";

/// MCP server backed by a beacon [`Runtime`]. Cloned per session by the
/// transport; the runtime handle is shared.
#[derive(Clone)]
pub struct BeaconMcpServer {
    runtime: Arc<Runtime>,
}

impl BeaconMcpServer {
    pub fn new(runtime: Arc<Runtime>) -> Self {
        Self { runtime }
    }
}

impl ServerHandler for BeaconMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions(INSTRUCTIONS)
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult::with_all_items(crate::catalog::tools()))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = request.arguments.unwrap_or_default();
        let identity = identity_from_context(&context);
        match crate::catalog::dispatch(&self.runtime, request.name.as_ref(), args, identity).await {
            Ok(text) => Ok(CallToolResult::success(vec![Content::text(text)])),
            // Surface tool failures as an error result (not a protocol error) so
            // the model can read and react to the message.
            Err(error) => Ok(CallToolResult::error(vec![Content::text(error.to_string())])),
        }
    }
}

/// Recover the caller's [`AuthIdentity`], resolved by the `resolve_identity`
/// middleware and carried in the HTTP request parts that the streamable-HTTP
/// transport injects into the MCP request context. Falls back to a role-less
/// identity (no access) when absent.
///
/// The MCP surface is strictly read-only: the returned identity always has
/// `is_super_user` cleared, so the query planner rejects any DDL/DML regardless
/// of the caller's privileges. The caller's `roles` are preserved so per-user
/// read grants (RBAC) still apply.
fn identity_from_context(context: &RequestContext<RoleServer>) -> AuthIdentity {
    let mut identity = context
        .extensions
        .get::<http::request::Parts>()
        .and_then(|parts| parts.extensions.get::<AuthIdentity>().cloned())
        .unwrap_or_else(AuthIdentity::empty);
    identity.is_super_user = false;
    identity
}
