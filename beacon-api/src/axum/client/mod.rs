//! Public client-facing HTTP endpoints.

use std::sync::Arc;

use ::axum::Router;
use beacon_core::runtime::Runtime;
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

mod datasets;
mod functions;
mod info;
mod query;
mod tables;

/// OpenAPI document marker for the client surface.
#[derive(utoipa::OpenApi)]
#[openapi()]
pub struct ClientApiDoc;

/// Builds the client router and returns the generated OpenAPI document alongside it.
#[allow(deprecated)]
pub(crate) fn setup_client_router(
    beacon_runtime: Arc<Runtime>,
) -> (Router<Arc<Runtime>>, utoipa::openapi::OpenApi) {
    OpenApiRouter::with_openapi(ClientApiDoc::openapi())
        .routes(routes!(query::query))
        .routes(routes!(query::parse_query))
        .routes(routes!(query::query_metrics))
        .routes(routes!(query::explain_query))
        .routes(routes!(query::available_columns))
        .routes(routes!(datasets::datasets))
        .routes(routes!(datasets::list_datasets))
        .routes(routes!(datasets::list_dataset_schema))
        .routes(routes!(datasets::total_datasets))
        .routes(routes!(tables::list_tables))
        .routes(routes!(tables::list_tables_with_schema))
        .routes(routes!(tables::default_table))
        .routes(routes!(tables::list_table_schema))
        .routes(routes!(tables::list_table_config))
        .routes(routes!(tables::default_table_schema))
        .routes(routes!(functions::list_functions))
        .routes(routes!(functions::list_table_functions))
        .routes(routes!(info::system_info))
        .layer(::axum::middleware::from_fn_with_state(
            beacon_runtime,
            crate::axum::auth::resolve_identity,
        ))
        .split_for_parts()
}
