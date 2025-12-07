use axum::Json;

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
pub async fn check() -> Json<CheckReponse> {
    let check = CheckReponse { is_admin: true };
    Json(check)
}

#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct CheckReponse {
    is_admin: bool,
}
