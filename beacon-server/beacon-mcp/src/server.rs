//! The [`ServerHandler`] implementation: capabilities, tool listing, dispatch.

use std::sync::Arc;

use beacon_core::runtime::Runtime;
use beacon_core::plan_validator::PlanValidator;
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
            "Beacon server. Call `list_tables` to discover tables and what they hold, \
             `describe_table` for a table's columns and what each one means, `run_sql` \
             for a read-only SQL preview (SELECT only), and `export_query` for large results.",
        )
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
        let Some(validator) = validator_from_context(&context) else {
            return Ok(CallToolResult::error(vec![Content::text(
                "the request has no caller: the server did not resolve one",
            )]));
        };
        match crate::catalog::dispatch(&self.runtime, request.name.as_ref(), args, validator).await
        {
            Ok(text) => Ok(CallToolResult::success(vec![Content::text(text)])),
            // Surface tool failures as an error result (not a protocol error) so
            // the model can read and react to the message.
            Err(error) => Ok(CallToolResult::error(vec![Content::text(error.to_string())])),
        }
    }
}

/// Recover the caller's validator, stored by the middleware of the server and
/// carried in the HTTP request parts that the streamable-HTTP transport injects
/// into the MCP request context. `None` when the middleware stored no validator.
///
/// The MCP surface is strictly read-only. The server guarantees this: it stores
/// a validator with no super-user rights, which refuses any DDL/DML regardless
/// of the caller's privileges. The per-user read grants (RBAC) still apply.
fn validator_from_context(
    context: &RequestContext<RoleServer>,
) -> Option<Arc<dyn PlanValidator>> {
    context
        .extensions
        .get::<http::request::Parts>()
        .and_then(|parts| parts.extensions.get::<Arc<dyn PlanValidator>>().cloned())
}
