use std::sync::Arc;

use axum::{routing::post, Router};
use beacon_core::runtime::Runtime;
use datasets::total_datasets;
use query::{available_columns, query_metrics};
use tables::list_table_extensions;
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

mod datasets;
mod info;
mod query;
mod tables;

#[derive(utoipa::OpenApi)]
#[openapi()]
pub struct ClientApiDoc;

pub(crate) fn setup_client_router() -> (Router<Arc<Runtime>>, utoipa::openapi::OpenApi) {
    let (client_router, client_api) = OpenApiRouter::with_openapi(ClientApiDoc::openapi())
        .routes(routes!(query::query))
        .routes(routes!(query::query_metrics))
        .routes(routes!(query::explain_query))
        .routes(routes!(query::available_columns))
        .routes(routes!(query::list_functions))
        .routes(routes!(datasets::list_datasets))
        .routes(routes!(datasets::list_dataset_schema))
        .routes(routes!(datasets::total_datasets))
        .routes(routes!(tables::list_tables))
        .routes(routes!(tables::default_table))
        .routes(routes!(tables::list_table_schema))
        .routes(routes!(tables::list_table_extensions))
        .routes(routes!(tables::list_table_config))
        .routes(routes!(tables::default_table_schema))
        .routes(routes!(info::system_info))
        .split_for_parts();

    (client_router, client_api)
}
