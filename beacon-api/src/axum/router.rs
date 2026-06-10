//! HTTP router assembly for the Beacon API binary.

use std::{str::FromStr, sync::Arc, time::Duration};

use ::axum::{
    body::Bytes,
    extract::MatchedPath,
    http::{HeaderMap, HeaderName, HeaderValue, Method, Request},
    response::{Redirect, Response},
    routing::get,
    Router,
};
use anyhow::Context;
use beacon_core::runtime::Runtime;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::{classify::ServerErrorsFailureClass, trace::TraceLayer};
use tracing::{info_span, Span};
use utoipa::openapi::{OpenApi, Server};
use utoipa_scalar::{Scalar, Servable};
use utoipa_swagger_ui::{SwaggerUi, Url};

use crate::axum::{admin::setup_admin_router, client::setup_client_router};

const BEACON_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Builds the complete HTTP router, OpenAPI docs, CORS policy, and tracing layers.
///
/// Returns an error when CORS configuration in [`static@beacon_config::CONFIG`]
/// contains invalid origins, methods, or headers — validated once at startup
/// rather than on every request.
pub(crate) fn setup_router(beacon_runtime: Arc<Runtime>) -> anyhow::Result<Router> {
    let (client_router, mut api_docs_client) = setup_client_router();
    let (admin_router, api_docs_admin) = setup_admin_router();

    api_docs_client.merge(api_docs_admin);
    api_docs_client = set_api_docs_info(api_docs_client);

    let base_path = &beacon_config::CONFIG.server.base_path;
    let swagger_url = format!("{base_path}/swagger");
    let openapi_url = format!("{base_path}/openapi.json");

    let docs = if base_path.is_empty() {
        api_docs_client
    } else {
        OpenApi::default().nest(base_path, api_docs_client)
    };

    let router = client_router
        .merge(admin_router)
        .merge(Scalar::with_url("/scalar/", docs.clone()))
        .route("/scalar", get(|| async { Redirect::to("./scalar/") }))
        .route(
            "/api/health",
            get(|| async { Response::new("Ok".to_string()) }),
        )
        .route("/", get(|| async { Redirect::to("./swagger") }))
        .layer(build_cors_layer()?)
        .with_state::<_>(beacon_runtime);

    // SwaggerUi is merged outside the base_path nest to avoid doubling the prefix
    // in the openapi.json URL (/{base_path}/{base_path}/openapi.json).
    let openapi_url_static: &'static str = openapi_url.leak();
    let swagger_ui =
        SwaggerUi::new(swagger_url).urls(vec![(Url::new("Docs", openapi_url_static), docs)]);

    if base_path.is_empty() {
        Ok(Router::new()
            .merge(setup_tracing_router(router))
            .merge(swagger_ui))
    } else {
        Ok(Router::new()
            .nest(base_path, setup_tracing_router(router))
            .merge(swagger_ui))
    }
}

/// Fills in the top-level OpenAPI metadata exposed by the HTTP documentation endpoints.
///
/// Title, description, contact, and license are sourced from
/// [`static@beacon_config::CONFIG`] so deployments can brand the docs via
/// `BEACON_API_*` environment variables without recompiling. The version is
/// always the compiled crate version.
fn set_api_docs_info(mut openapi: utoipa::openapi::OpenApi) -> utoipa::openapi::OpenApi {
    use utoipa::openapi::{Contact, License};

    let cfg = &beacon_config::CONFIG.api_docs;

    openapi.info.title = cfg.title.clone();
    openapi.info.version = BEACON_VERSION.to_string();
    openapi.info.description = Some(cfg.description.clone());
    openapi.info.terms_of_service = cfg.terms_of_service.clone();

    // Only attach a contact object if at least one contact field is set.
    if cfg.contact_name.is_some() || cfg.contact_url.is_some() || cfg.contact_email.is_some() {
        let mut contact = Contact::new();
        contact.name = cfg.contact_name.clone();
        contact.url = cfg.contact_url.clone();
        contact.email = cfg.contact_email.clone();
        openapi.info.contact = Some(contact);
    }

    // The license object requires a name, so only attach it when one is given.
    if let Some(name) = &cfg.license_name {
        let mut license = License::new(name.clone());
        license.url = cfg.license_url.clone();
        license.identifier = cfg.license_identifier.clone();
        openapi.info.license = Some(license);
    }

    openapi
}

/// Adds request/response tracing to every HTTP route.
fn setup_tracing_router<T>(mut router: Router<T>) -> Router<T>
where
    T: Send + Sync + Clone + 'static,
{
    router = router.layer(
        TraceLayer::new_for_http()
            .make_span_with(|request: &Request<_>| {
                let matched_path = request
                    .extensions()
                    .get::<MatchedPath>()
                    .map(MatchedPath::as_str);

                info_span!(
                    "http_request",
                    matched_path,
                    some_other_field = tracing::field::Empty,
                )
            })
            .on_request(|request: &Request<_>, span: &Span| {
                let method = request.method();
                let path = request.uri();

                span.record("method", method.to_string());
                span.record("path", path.to_string());

                tracing::info!(
                    parent: span,
                    method = %method,
                    path = %path,
                    "Request received"
                );
            })
            .on_response(|response: &Response, latency: Duration, span: &Span| {
                let status = response.status();

                tracing::info!(
                    parent: span,
                    status = %status.as_u16(),
                    latency = ?latency,
                    "Response completed"
                );
            })
            .on_body_chunk(|_chunk: &Bytes, _latency: Duration, _span: &Span| {})
            .on_eos(|_trailers: Option<&HeaderMap>, _stream_duration: Duration, _span: &Span| {})
            .on_failure(
                |error: ServerErrorsFailureClass, latency: Duration, span: &Span| {
                    let method = span.metadata().and_then(|m| m.fields().field("method"));
                    let path = span.metadata().and_then(|m| m.fields().field("path"));

                    let status_code = match &error {
                        ServerErrorsFailureClass::StatusCode(status) => status.as_u16(),
                        _ => 0,
                    };

                    match (method, path) {
                        (Some(m), Some(p)) => {
                            tracing::error!(
                                parent: span,
                                method = %m,
                                path = %p,
                                status = status_code,
                                error = ?error,
                                latency = ?latency,
                                "Request failed"
                            );
                        }
                        _ => {
                            tracing::error!(
                                parent: span,
                                status = status_code,
                                error = ?error,
                                latency = ?latency,
                                "Request failed"
                            );
                        }
                    }
                },
            ),
    );

    router
}

/// Builds the CORS layer from Beacon configuration.
///
/// Configuration values are validated at startup so invalid origins, methods, or
/// header names surface as a contextual error rather than a runtime panic.
fn build_cors_layer() -> anyhow::Result<CorsLayer> {
    let cors = &beacon_config::CONFIG.cors;
    let mut layer = CorsLayer::new();

    if cors.allowed_origins.trim() == "*" {
        layer = layer.allow_origin(Any);
    } else {
        let origins = cors
            .allowed_origins
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                HeaderValue::from_str(s)
                    .with_context(|| format!("invalid CORS origin in config: {s}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        layer = layer.allow_origin(AllowOrigin::list(origins));
    }

    let methods = cors
        .allowed_methods
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| Method::from_str(s).with_context(|| format!("invalid CORS method in config: {s}")))
        .collect::<anyhow::Result<Vec<_>>>()?;
    layer = layer.allow_methods(methods);

    let headers = cors
        .allowed_headers
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            HeaderName::from_str(s).with_context(|| format!("invalid CORS header in config: {s}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    layer = layer.allow_headers(headers);

    if cors.allowed_credentials {
        layer = layer.allow_credentials(true);
    }

    Ok(layer.max_age(Duration::from_secs(cors.max_age)))
}
