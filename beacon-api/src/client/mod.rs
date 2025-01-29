use std::sync::Arc;

use axum::{routing::post, Router};
use beacon_core::runtime::Runtime;
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

mod query;

#[derive(utoipa::OpenApi)]
#[openapi()]
pub struct ClientApiDoc;

pub(crate) fn setup_client_router() -> (Router<Arc<Runtime>>, utoipa::openapi::OpenApi) {
    let (client_router, client_api) = OpenApiRouter::with_openapi(ClientApiDoc::openapi())
        .routes(routes!(query::query))
        .split_for_parts();

    (client_router, client_api)
}
