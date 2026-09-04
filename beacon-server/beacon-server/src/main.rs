//! Beacon API binary entrypoint.
//!
//! The process hosts the Axum HTTP API and, when enabled, the Arrow Flight SQL
//! server on top of a shared [`Server`](crate::server::Server) so both
//! transports see the same files, catalog state, and authorization rules.

use std::{net::IpAddr, str::FromStr, sync::Arc};

use anyhow::Context;
use tokio::runtime::{Builder, Handle};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use beacon_server::{axum::setup_router, flight_sql, Server};

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const BEACON_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Builds the two Tokio runtimes and hands control to the async entrypoint.
///
/// The API runtime serves HTTP and Flight SQL. The query runtime plans and runs
/// queries, and hosts the crawler and file statistics timers. A partition decode
/// holds a worker until it yields, and Tokio cannot preempt it, so a query on
/// the API runtime would make every other request wait.
fn main() -> anyhow::Result<()> {
    // Load and validate configuration up front so problems (e.g. a malformed
    // `BEACON_BASE_PATH`) surface as a clean error here. The config is owned and
    // passed explicitly into the runtime and the transports — it is not stored in
    // a process-global.
    let config = Arc::new(beacon_server_config::Config::load().context("failed to load configuration")?);

    let api_runtime = Builder::new_multi_thread()
        .worker_threads(config.server.api_threads)
        .thread_name("beacon-api")
        .enable_all()
        .build()
        .context("failed to build the API Tokio runtime")?;
    let query_runtime = Builder::new_multi_thread()
        .worker_threads(config.server.worker_threads)
        .thread_name("beacon-query")
        .enable_all()
        .build()
        .context("failed to build the query Tokio runtime")?;

    // Both runtimes live until this returns, so a task on one can always reach the other.
    api_runtime.block_on(async_main(config, query_runtime.handle().clone()))
}

/// Initializes shared services and starts all configured API transports.
async fn async_main(
    config: Arc<beacon_server_config::Config>,
    query_runtime: Handle,
) -> anyhow::Result<()> {
    let log_filter = setup_tracing(&config);
    install_panic_hook();

    tracing::info!("Beacon v{}", BEACON_VERSION);
    // This line only prints when DEBUG is on, so it confirms the level took effect.
    tracing::debug!(filter = %log_filter, "debug logging is on");
    tracing::info!(
        api_threads = config.server.api_threads,
        query_threads = config.server.worker_threads,
        "runtime threads"
    );
    // The server owns the datasets store and hosts the runtime that queries it.
    // It opens on the query runtime: the timers it starts spawn onto the ambient
    // runtime, and they belong with the queries, not with the API.
    let server = {
        let config = config.clone();
        let handle = query_runtime.clone();
        query_runtime
            .spawn(async move { Server::open(config, handle).await })
            .await
            .context("the server did not finish opening")??
    };
    let server = Arc::new(server);
    // Keep both transports on the same server so metadata and access rules stay aligned.
    let router = setup_router(server.clone(), config.clone())?;

    let listen = &config.server;
    let addr = std::net::SocketAddr::new(
        IpAddr::from_str(&listen.host)
            .with_context(|| format!("invalid `host` in config: {}", listen.host))?,
        listen.port,
    );

    let http_server = serve_http(router, addr);

    if config.flight_sql.enable {
        tokio::try_join!(http_server, flight_sql::serve(server.clone()))?;
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

/// Third-party crates whose `DEBUG`/`TRACE` events bury Beacon's own logs. They
/// stay at `INFO` when `BEACON_LOG_LEVEL` asks for `debug` or `trace`. Set
/// `RUST_LOG` to see them.
const NOISY_DEPENDENCIES: &[&str] = &[
    "arrow",
    "aws_config",
    "aws_smithy_runtime",
    "datafusion",
    "deltalake",
    "h2",
    "hyper",
    "hyper_util",
    "iceberg",
    "lance",
    "mio",
    "object_store",
    "parquet",
    "reqwest",
    "rustls",
    "sqlparser",
    "tokio_util",
    "tonic",
    "want",
    "zarrs",
];

/// Builds the tracing filter for a validated `BEACON_LOG_LEVEL` value.
///
/// The level is the global directive, so it covers every Beacon crate, including
/// crates added later. The filter names no Beacon crate, which is what keeps it
/// from going stale.
fn log_filter(level: &str) -> String {
    match level {
        // Beacon logs at the requested level; the loud dependencies do not.
        "debug" | "trace" => {
            let mut filter = String::from(level);
            for dependency in NOISY_DEPENDENCIES {
                filter.push_str(&format!(",{dependency}=info"));
            }
            filter
        }
        // Axum logs rejections from built-in extractors on the `axum::rejection`
        // target at `TRACE`.
        "info" => String::from("info,tower_http=debug,axum::rejection=trace"),
        // `warn`, `error`, and `off` ask for less, so nothing is raised here.
        _ => String::from(level),
    }
}

/// Configures stdout and rolling-file tracing subscribers for the API process.
///
/// `RUST_LOG` takes precedence when it is set, for the full `EnvFilter` syntax.
/// `BEACON_LOG_LEVEL` (through `config`) sets the level otherwise. Returns the
/// filter it applied, for the startup log.
fn setup_tracing(config: &beacon_server_config::Config) -> String {
    let file_appender = tracing_appender::rolling::daily("logs", "beacon.log");
    let (file_writer, _guard) = tracing_appender::non_blocking(file_appender);

    let default_filter = log_filter(&config.server.log_level);
    let filter = match std::env::var("RUST_LOG") {
        Ok(rust_log) => tracing_subscriber::EnvFilter::try_new(&rust_log).unwrap_or_else(|err| {
            // Tracing is not up yet, so this warning goes straight to stderr.
            eprintln!("ignoring invalid RUST_LOG (`{rust_log}`): {err}");
            tracing_subscriber::EnvFilter::new(&default_filter)
        }),
        Err(_) => tracing_subscriber::EnvFilter::new(&default_filter),
    };

    let applied = filter.to_string();
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::fmt::layer().with_writer(file_writer).with_ansi(false))
        .init();

    // The non-blocking writer must outlive the subscriber, so keep the guard for the
    // lifetime of the process.
    std::mem::forget(_guard);

    applied
}

#[cfg(test)]
mod tests {
    use super::log_filter;

    #[test]
    fn debug_level_covers_every_beacon_crate() {
        let filter = log_filter("debug");
        // A global `debug` directive, so a new Beacon crate needs no filter change.
        assert!(filter.starts_with("debug,"));
        assert!(!filter.contains("beacon_"));
        assert!(filter.contains("datafusion=info"));
    }

    #[test]
    fn quiet_levels_raise_nothing() {
        assert_eq!(log_filter("warn"), "warn");
        assert_eq!(log_filter("off"), "off");
    }
}
