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
use tower_http::services::{ServeDir, ServeFile};
use tower_http::{classify::ServerErrorsFailureClass, trace::TraceLayer};
use tracing::{info_span, Span};
use utoipa::openapi::OpenApi;
use utoipa_scalar::{Scalar, Servable};
use utoipa_swagger_ui::{SwaggerUi, Url};

use crate::axum::{admin::setup_admin_router, client::setup_client_router};

const BEACON_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Builds the complete HTTP router, OpenAPI docs, CORS policy, and tracing layers.
///
/// Returns an error when the CORS configuration in the passed-in `config`
/// contains invalid origins, methods, or headers — validated once at startup
/// rather than on every request.
pub(crate) fn setup_router(
    beacon_runtime: Arc<Runtime>,
    config: Arc<beacon_config::Config>,
) -> anyhow::Result<Router> {
    let (client_router, mut api_docs_client) = setup_client_router();
    let (admin_router, api_docs_admin) = setup_admin_router();

    // Resolve the caller's identity for every client route; gate admin routes on a super-user.
    // Both middlewares carry the runtime as their own state (independent of the router state set
    // below), so they are attached per-sub-router before the two are merged.
    let client_router = client_router.layer(::axum::middleware::from_fn_with_state(
        beacon_runtime.clone(),
        crate::axum::auth::resolve_identity,
    ));
    let admin_router = admin_router.layer(::axum::middleware::from_fn_with_state(
        beacon_runtime.clone(),
        crate::axum::auth::basic_auth,
    ));

    api_docs_client.merge(api_docs_admin);
    api_docs_client = set_api_docs_info(api_docs_client, &config.api_docs);

    let base_path = &config.server.base_path;
    let swagger_url = format!("{base_path}/swagger");
    let openapi_url = format!("{base_path}/openapi.json");

    // Redirect targets must be absolute and include the base path. Relative
    // targets (e.g. "./swagger") resolve against the incoming request URI, which
    // breaks once the router is nested under a non-empty base path.
    let swagger_redirect: &'static str = format!("{base_path}/swagger").leak();
    let scalar_redirect: &'static str = format!("{base_path}/scalar/").leak();

    let docs = if base_path.is_empty() {
        api_docs_client
    } else {
        OpenApi::default().nest(base_path, api_docs_client)
    };

    // Mount the bundled admin web UI when its build directory is present (it is in
    // the Docker image; usually absent for a bare `cargo run`). The bare root then
    // lands on the UI instead of the API docs.
    let web_ui = web_ui_router(&config.server.web_ui_dir);

    // MCP streamable-HTTP endpoint, gated by BEACON_MCP_ENABLED (default on). It
    // rides the same `resolve_identity` middleware as the client API, so MCP tool
    // calls execute under the caller's identity (or the anonymous principal when
    // enabled) and per-user RBAC applies at query time.
    let mcp_enabled = std::env::var("BEACON_MCP_ENABLED")
        .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "off"))
        .unwrap_or(true);
    let mut router = client_router.merge(admin_router);
    if mcp_enabled {
        let mcp_router = ::axum::Router::new()
            .route_service(
                "/mcp",
                beacon_mcp::streamable_http_service(beacon_runtime.clone()),
            )
            .layer(::axum::middleware::from_fn_with_state(
                beacon_runtime.clone(),
                crate::axum::auth::resolve_identity,
            ));
        router = router.merge(mcp_router);
    }
    let mut router = router
        .merge(Scalar::with_url("/scalar/", docs.clone()))
        .route(
            "/scalar",
            get(move || async move { Redirect::to(scalar_redirect) }),
        )
        .route(
            "/api/health",
            get(|| async { Response::new("Ok".to_string()) }),
        )
        .route(
            "/",
            get(move || async move { Redirect::to(swagger_redirect) }),
        );

    if let Some(web_ui) = web_ui {
        router = router.merge(web_ui);
    }

    let router = router
        .layer(build_cors_layer(&config.cors)?)
        // Publish the config so middleware/handlers can read transport settings.
        .layer(::axum::Extension(config.clone()))
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

/// Builds a router that serves the admin web UI (a Vite single-page app) at
/// `/admin`, or `None` when no build is present at `dir`.
///
/// The whole router is later nested under `base_path`, so the SPA is reachable at
/// `{base_path}/admin`. Missing files fall back to `index.html` with a `200` (via
/// `fallback`, not `not_found_service`, which would preserve the `404`) so deep
/// client-side routes (e.g. `/admin/tables`) resolve on a hard reload. A genuinely
/// missing asset thus also yields the SPA shell, which the build's hashed asset
/// names make a non-issue in practice.
///
/// Presence is detected by `index.html` rather than the directory itself so a
/// stray empty `web/` directory does not advertise a broken UI.
fn web_ui_router(dir: &str) -> Option<Router<Arc<Runtime>>> {
    let dir = std::path::Path::new(dir);
    let index = dir.join("index.html");
    if !index.is_file() {
        return None;
    }

    tracing::info!("serving admin web UI from {} at /admin", dir.display());
    let serve = ServeDir::new(dir).fallback(ServeFile::new(index));
    Some(Router::new().nest_service("/admin", serve))
}

/// Fills in the top-level OpenAPI metadata exposed by the HTTP documentation endpoints.
///
/// Title, description, contact, and license are sourced from the passed-in
/// `ApiDocsConfig` so deployments can brand the docs via `BEACON_API_*`
/// environment variables without recompiling. The version is always the
/// compiled crate version.
fn set_api_docs_info(
    mut openapi: utoipa::openapi::OpenApi,
    cfg: &beacon_config::ApiDocsConfig,
) -> utoipa::openapi::OpenApi {
    use utoipa::openapi::{Contact, License};

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
fn build_cors_layer(cors: &beacon_config::CorsConfig) -> anyhow::Result<CorsLayer> {
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

    let expose = cors
        .expose_headers
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            HeaderName::from_str(s)
                .with_context(|| format!("invalid CORS expose header in config: {s}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    if !expose.is_empty() {
        layer = layer.expose_headers(expose);
    }

    if cors.allowed_credentials {
        layer = layer.allow_credentials(true);
    }

    Ok(layer.max_age(Duration::from_secs(cors.max_age)))
}
