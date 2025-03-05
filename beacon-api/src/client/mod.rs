use std::sync::Arc;

use axum::{routing::post, Router};
use beacon_core::runtime::Runtime;
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

mod datasets;
mod query;
mod tables;

#[derive(utoipa::OpenApi)]
#[openapi()]
pub struct ClientApiDoc;

pub(crate) fn setup_client_router() -> (Router<Arc<Runtime>>, utoipa::openapi::OpenApi) {
    let (client_router, client_api) = OpenApiRouter::with_openapi(ClientApiDoc::openapi())
        .routes(routes!(query::query))
        .routes(routes!(datasets::list_datasets))
        .routes(routes!(datasets::list_dataset_schema))
        .routes(routes!(tables::list_tables))
        .routes(routes!(tables::list_table_schema))
        .split_for_parts();

    (client_router, client_api)
}
