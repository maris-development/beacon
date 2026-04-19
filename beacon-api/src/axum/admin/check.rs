//! Simple administrative health endpoints.

use ::axum::Json;

/// Returns whether the caller has reached the authenticated admin surface.
#[tracing::instrument(level = "info")]
#[utoipa::path(
    tag = "admin",
    get,
    path = "/api/admin/check",
    responses((status = 200, description = "Admin API is reachable")),
    security(
        ("basic-auth" = []),
        ("bearer" = [])
    ))
]
pub async fn check() -> Json<CheckResponse> {
    let check = CheckResponse { is_admin: true };
    Json(check)
}

/// Minimal response payload for the admin connectivity check.
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct CheckResponse {
    is_admin: bool,
}
