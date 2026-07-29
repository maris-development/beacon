//! Read-only admin endpoints exposing the auth model (users + roles) so the web
//! UI can render current state. Mutations (CREATE USER / ROLE, GRANT, DENY, …)
//! are performed through the SQL query endpoint as the super-user; there is no
//! write endpoint here.

use std::sync::Arc;

use ::axum::{extract::State, http::StatusCode, Extension, Json};
use beacon_core::api::{AuthRoleView, AuthRuleView, AuthUserView};
use beacon_core::AuthIdentity;
use crate::datalake::{
    sql::{query_rows, str_field},
    DataLake,
};

use super::bad_request;

/// Rules come back as a JSON array shaped like [`AuthRuleView`]; anything
/// unparseable yields an empty list rather than failing the whole listing.
fn rules(row: &serde_json::Value, column: &str) -> Vec<AuthRuleView> {
    serde_json::from_str(str_field(row, column)).unwrap_or_default()
}

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
    Extension(identity): Extension<AuthIdentity>,
) -> Result<Json<Vec<AuthUserView>>, (StatusCode, String)> {
    let rows = query_rows(
        &state,
        "SELECT username, roles FROM beacon.system.users ORDER BY username",
        identity,
    )
    .await
    .map_err(bad_request)?;

    let super_user = &state.config().admin.username;
    let anonymous = crate::datalake::ANONYMOUS_USERNAME;

    let mut users: Vec<AuthUserView> = rows
        .iter()
        .map(|row| {
            let username = str_field(row, "username").to_string();
            AuthUserView {
                is_super_user: username == *super_user,
                is_anonymous: username == anonymous,
                roles: serde_json::from_str(str_field(row, "roles")).unwrap_or_default(),
                username,
            }
        })
        .collect();

    // The super-user is config-defined, not a directory entry, so it never shows
    // up in `beacon.system.users`. Surface it here so the listing reflects every
    // principal (and the `is_super_user` flag is actually reachable).
    if !users.iter().any(|u| u.username == *super_user) {
        users.insert(
            0,
            AuthUserView {
                username: super_user.clone(),
                is_super_user: true,
                is_anonymous: false,
                roles: Vec::new(),
            },
        );
    }

    Ok(Json(users))
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
pub(crate) async fn list_roles(
    State(state): State<Arc<DataLake>>,
    Extension(identity): Extension<AuthIdentity>,
) -> Json<Vec<AuthRoleView>> {
    let rows = query_rows(
        &state,
        "SELECT role_name, grants, denies FROM beacon.system.roles ORDER BY role_name",
        identity,
    )
    .await
    .unwrap_or_default();

    Json(
        rows.iter()
            .map(|row| AuthRoleView {
                name: str_field(row, "role_name").to_string(),
                grants: rules(row, "grants"),
                denies: rules(row, "denies"),
            })
            .collect(),
    )
}
