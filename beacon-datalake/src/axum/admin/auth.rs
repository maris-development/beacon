//! Read-only admin endpoints exposing the auth model (users + roles) so the web
//! UI can render current state. Mutations (CREATE USER / ROLE, GRANT, DENY, …)
//! are performed through the SQL query endpoint as the super-user; there is no
//! write endpoint here.

use std::sync::Arc;

use ::axum::{extract::State, http::StatusCode, Json};
use beacon_core::api::{AuthRoleView, AuthUserView};
use crate::datalake::DataLake;

use super::bad_request;

/// Lists all principals: the config super-user (flagged) plus stored local users
/// with their roles.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    get,
    path = "/api/admin/auth/users",
    responses(
        (status = 200, description = "Users with their roles", body = Vec<AuthUserView>),
        (status = 400, description = "Failed to enumerate users")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn list_users(
    State(state): State<Arc<DataLake>>,
) -> Result<Json<Vec<AuthUserView>>, (StatusCode, String)> {
    state.list_auth_users().map(Json).map_err(bad_request)
}

/// Lists all roles with their grant and deny rules.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    get,
    path = "/api/admin/auth/roles",
    responses((status = 200, description = "Roles with their rules", body = Vec<AuthRoleView>)),
    security(("basic-auth" = []))
)]
pub(crate) async fn list_roles(State(state): State<Arc<DataLake>>) -> Json<Vec<AuthRoleView>> {
    Json(state.list_auth_roles())
}
