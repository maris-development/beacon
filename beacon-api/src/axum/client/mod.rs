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
    (name = "tables", description = "List registered tables and inspect their schemas."),
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
        let spec: serde_json::Value =
            serde_json::to_value(&openapi).expect("openapi serializes to JSON");

        // The structured query body and its nested types are registered as
        // component schemas (not merely mentioned in a description or example).
        let schemas = spec
            .pointer("/components/schemas")
            .and_then(|s| s.as_object())
            .expect("components.schemas object");
        for schema in ["Query", "QueryBody", "Filter", "Select", "OutputFormat"] {
            assert!(
                schemas.contains_key(schema),
                "expected `{schema}` in components.schemas"
            );
        }

        // The /api/query request body references the real Query schema rather
        // than an opaque object.
        let request_ref = spec.pointer(
            "/paths/~1api~1query/post/requestBody/content/application~1json/schema/$ref",
        );
        assert_eq!(
            request_ref.and_then(|r| r.as_str()),
            Some("#/components/schemas/Query"),
            "expected /api/query requestBody to $ref the Query schema"
        );
    }
}
