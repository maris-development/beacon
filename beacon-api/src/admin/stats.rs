use axum::response::IntoResponse;

#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/stats",
    responses(
        (status=200, description="Response containing the Beacon Node Stats"),
    ),
    security(
        ("basic-auth" = [])
    )
)]
pub(crate) async fn stats() -> impl IntoResponse {
    "Hello, world!"
}
