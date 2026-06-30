//! The [`ServerHandler`] implementation: capabilities, tool listing, dispatch.

use std::sync::Arc;

use beacon_core::runtime::Runtime;
use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, Content, ListToolsResult, PaginatedRequestParams,
    ServerCapabilities, ServerInfo,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer};

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
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            "Beacon data lake. Call `list_tables` to discover tables, \
             `describe_table` for a table's schema and available presets, the \
             per-table tools to query curated datasets (optionally via a named \
             preset), and `run_sql` for read-only SQL (SELECT only).",
        )
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let tools = crate::catalog::build_tools(&self.runtime)
            .await
            .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;
        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = request.arguments.unwrap_or_default();
        match crate::catalog::dispatch(&self.runtime, request.name.as_ref(), args).await {
            Ok(text) => Ok(CallToolResult::success(vec![Content::text(text)])),
            // Surface tool failures as an error result (not a protocol error) so
            // the model can read and react to the message.
            Err(error) => Ok(CallToolResult::error(vec![Content::text(error.to_string())])),
        }
    }
}
