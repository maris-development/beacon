use std::{net::IpAddr, str::FromStr, sync::Arc, time::Duration};

use admin::setup_admin_router;
use axum::{
    body::Bytes,
    extract::MatchedPath,
    http::{HeaderMap, Request},
    response::{Redirect, Response},
    routing::get,
    Router,
};
use client::setup_client_router;
use tower_http::{classify::ServerErrorsFailureClass, trace::TraceLayer};
use tracing::{field::Empty, info_span, Level, Span};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, FmtSubscriber};
use utoipa_scalar::{Scalar, Servable};
use utoipa_swagger_ui::SwaggerUi;

mod admin;
mod client;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const BEACON_VERSION: &str = env!("CARGO_PKG_VERSION");

fn set_api_docs_info(mut openapi: utoipa::openapi::OpenApi) -> utoipa::openapi::OpenApi {
    openapi.info.title = "Beacon API".to_string();
    openapi.info.version = BEACON_VERSION.to_string();
    openapi.info.description = Some("API ".to_string());

    openapi
}

#[tokio::main(worker_threads = 8)]
async fn main() -> anyhow::Result<()> {
    setup_tracing();

    tracing::info!("Beacon API v{}", BEACON_VERSION);
    let beacon_runtime = Arc::new(beacon_core::runtime::Runtime::new()?);

    let (client_router, mut api_docs_client) = setup_client_router();
    let (admin_router, api_docs_admin) = setup_admin_router();
    //Merge the two openapi docs
    api_docs_client.merge(api_docs_admin);
    api_docs_client = set_api_docs_info(api_docs_client);

    let mut router: Router<_> = client_router
        .merge(admin_router)
        .merge(Scalar::with_url("/scalar/", api_docs_client.clone()))
        .route("/scalar", get(|| async { Redirect::to("/scalar/") }))
        .merge(SwaggerUi::new("/swagger").url("/api/openapi.json", api_docs_client.clone()))
        .with_state::<_>(beacon_runtime.clone());

    let addr = std::net::SocketAddr::new(
        IpAddr::from_str(&beacon_config::CONFIG.host)
            .map_err(|e| anyhow::anyhow!("Failed to parse IP address from config: {}", e))?,
        beacon_config::CONFIG.port,
    );

    router = setup_tracing_router(router);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind to address {}: {}", addr, e))?;

    tracing::info!("listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, router)
        .await
        .map_err(|e| anyhow::anyhow!("Server failed: {}", e))?;

    Ok(())
}

fn setup_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                // axum logs rejections from built-in extractors with the `axum::rejection`
                // target, at `TRACE` level. `axum::rejection=trace` enables showing those events
                format!(
                    "info,{}=debug,tower_http=debug,axum::rejection=trace,beacon_core=debug,beacon_arrow_odv=debug,beacon_arrow_netcdf=debug",
                    env!("CARGO_CRATE_NAME")
                )
                .into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

fn setup_tracing_router<T>(mut router: Router<T>) -> Router<T>
where
    T: Send + Sync + Clone + 'static,
{
    router = router.layer(
        TraceLayer::new_for_http()
            .make_span_with(|request: &Request<_>| {
                // Log the matched route's path (with placeholders not filled in).
                // Use request.uri() or OriginalUri if you want the real path.
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

                // Attach method & path to the tracing span
                span.record("method", &method.to_string());
                span.record("path", &path.to_string());

                tracing::info!(
                    parent: span,
                    method = %method,
                    path = %path,
                    "Request received"
                );
            })
            .on_response(|response: &Response, latency: Duration, span: &Span| {
                // ...
                let status = response.status();

                tracing::info!(
                    parent: span,
                    status = %status.as_u16(),
                    latency = ?latency,
                    "Response completed"
                );
            })
            .on_body_chunk(|_chunk: &Bytes, _latency: Duration, _span: &Span| {
                // ...
            })
            .on_eos(
                |_trailers: Option<&HeaderMap>, _stream_duration: Duration, _span: &Span| {
                    // ...
                },
            )
            .on_failure(
                |error: ServerErrorsFailureClass, latency: Duration, span: &Span| {
                    let method = span
                        .metadata()
                        .and_then(|m| m.fields().field("method"))
                        .unwrap();
                    let path = span
                        .metadata()
                        .and_then(|m| m.fields().field("path"))
                        .unwrap();

                    let status_code = match &error {
                        ServerErrorsFailureClass::StatusCode(status) => status.as_u16(),
                        _ => 0, // Unknown or internal failure
                    };

                    tracing::error!(
                        parent: span,
                        method = %method,
                        path = %path,
                        status = status_code,
                        error = ?error,
                        latency = ?latency,
                        "Request failed"
                    );
                },
            ),
    );

    router
}
