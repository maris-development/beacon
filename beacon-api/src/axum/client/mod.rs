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
#[openapi(tags(
    (name = "query", description = "Execute, validate, explain, and inspect metrics of queries."),
    (name = "datasets", description = "Discover dataset files in the datasets store and inspect their schemas."),
    (name = "tables", description = "List registered tables and inspect their schemas and configuration."),
    (name = "functions", description = "Browse the scalar, aggregate, and table-valued functions available in queries."),
    (name = "system", description = "Beacon runtime version and host information.")
))]
pub struct ClientApiDoc;

/// Builds the client router and returns the generated OpenAPI document alongside it.
#[allow(deprecated)]
pub(crate) fn setup_client_router() -> (Router<Arc<Runtime>>, utoipa::openapi::OpenApi) {
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
        .split_for_parts()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The client OpenAPI document assembles without panicking and surfaces the
    /// structured query schema tree (which uses untagged enums and `flatten`).
    /// This guards the `request_body = Query` wiring and the un-hidden output
    /// formats against regressions.
    #[test]
    fn client_openapi_includes_query_schema() {
        #[allow(deprecated)]
        let (_router, openapi) = setup_client_router();
        let spec = openapi.to_json().expect("openapi serializes to JSON");

        // The structured query body and its nested types are present as components.
        for schema in ["QueryBody", "Filter", "Select", "OutputFormat"] {
            assert!(
                spec.contains(schema),
                "expected `{schema}` schema in client OpenAPI document"
            );
        }

        // The query endpoint references the real Query type, not an opaque object.
        assert!(
            spec.contains("#/components/schemas/Query"),
            "expected /api/query to reference the Query schema"
        );
    }
}
