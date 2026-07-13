//! Beacon API binary entrypoint.
//!
//! The process hosts the Axum HTTP API and, when enabled, the Arrow Flight SQL
//! server on top of a shared Beacon runtime so both transports see the same
//! catalog state, execution environment, and authorization rules.

use std::{net::IpAddr, str::FromStr, sync::Arc};

use anyhow::Context;
use tokio::runtime::Builder;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::axum::setup_router;

mod auth;
mod axum;
mod flight_sql;

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const BEACON_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Builds the Tokio runtime and hands control to the async entrypoint.
fn main() -> anyhow::Result<()> {
    // Load and validate configuration up front so problems (e.g. a malformed
    // `BEACON_BASE_PATH`) surface as a clean error here. The config is owned and
    // passed explicitly into the runtime and the transports — it is not stored in
    // a process-global.
    let config = Arc::new(beacon_config::Config::load().context("failed to load configuration")?);

    let rt = Builder::new_multi_thread()
        .worker_threads(config.server.worker_threads)
        .enable_all()
        .build()
        .context("failed to build Tokio runtime")?;

    rt.block_on(async_main(config))
}

/// Initializes shared services and starts all configured API transports.
async fn async_main(config: Arc<beacon_config::Config>) -> anyhow::Result<()> {
    setup_tracing();
    install_panic_hook();

    tracing::info!("Beacon v{}", BEACON_VERSION);
    let beacon_runtime = Arc::new(beacon_core::runtime::Runtime::new(config.clone()).await?);
    // Keep both transports on the same runtime so metadata and access rules stay aligned.
    let router = setup_router(beacon_runtime.clone(), config.clone())?;

    let server = &config.server;
    let addr = std::net::SocketAddr::new(
        IpAddr::from_str(&server.host)
            .with_context(|| format!("invalid `host` in config: {}", server.host))?,
        server.port,
    );

    let http_server = serve_http(router, addr);

    if config.flight_sql.enable {
        tokio::try_join!(http_server, flight_sql::serve(beacon_runtime.clone()))?;
    } else {
        http_server.await?;
    }

    Ok(())
}

/// Serves the Axum HTTP API on the configured socket address.
async fn serve_http(router: ::axum::Router, addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind HTTP listener to {addr}"))?;

    let local_addr = listener
        .local_addr()
        .context("failed to read HTTP listener local address")?;
    tracing::info!("listening on {local_addr}");

    ::axum::serve(listener, router)
        .await
        .context("HTTP server failed")?;

    Ok(())
}

/// Routes panics through `tracing` (so they land in the rolling log file) while
/// preserving the default hook's stderr output.
fn install_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        tracing_panic::panic_hook(info); // ERROR event -> stdout + rolling log file
        default_hook(info); // preserve default stderr output
    }));
}

/// Configures stdout and rolling-file tracing subscribers for the API process.
fn setup_tracing() {
    let file_appender = tracing_appender::rolling::daily("logs", "beacon.log");
    let (file_writer, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(
            // Fallback filter is only used when `RUST_LOG` is unset. Axum logs rejections from
            // built-in extractors with the `axum::rejection` target at `TRACE` level.
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!(
                    "info,{}=debug,tower_http=debug,axum::rejection=trace,beacon_core=debug,beacon_arrow_odv=debug,beacon_arrow_netcdf=debug,beacon_data_lake=debug,beacon_api=debug,beacon_arrow_ipc=debug,beacon_arrow_csv=debug,beacon_arrow_parquet=debug,beacon_arrow_geoparquet=debug,beacon_arrow_bbf=debug,beacon_common=debug,beacon_functions=debug,beacon_nd_array=debug,beacon_arrow_atlas=debug",
                    env!("CARGO_CRATE_NAME")
                )
                .into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::fmt::layer().with_writer(file_writer).with_ansi(false))
        .init();

    // The non-blocking writer must outlive the subscriber, so keep the guard for the
    // lifetime of the process.
    std::mem::forget(_guard);
}
