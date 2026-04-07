use std::{sync::OnceLock, time::Duration};

use anyhow::Context;
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, Meter},
    trace::TracerProvider as _,
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    trace::{Sampler, Tracer, TracerProvider},
    Resource,
};
use sysinfo::{ProcessesToUpdate, System};

pub(crate) struct OtelGuard {
    trace_provider: TracerProvider,
    meter_provider: SdkMeterProvider,
    process_metrics_task: tokio::task::JoinHandle<()>,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        self.process_metrics_task.abort();

        if let Err(error) = self.meter_provider.shutdown() {
            eprintln!("failed to shutdown opentelemetry meter provider cleanly: {error}");
        }

        if let Err(error) = self.trace_provider.shutdown() {
            eprintln!("failed to shutdown opentelemetry provider cleanly: {error}");
        }
    }
}

static API_METER: OnceLock<Meter> = OnceLock::new();
static API_QUERY_REQUESTS_TOTAL: OnceLock<Counter<u64>> = OnceLock::new();
static API_QUERY_ERRORS_TOTAL: OnceLock<Counter<u64>> = OnceLock::new();
static API_QUERY_DURATION_MS: OnceLock<Histogram<f64>> = OnceLock::new();
static WS_SQL_RESULT_ROWS: OnceLock<Histogram<u64>> = OnceLock::new();
static WS_SQL_CHUNKS: OnceLock<Histogram<u64>> = OnceLock::new();
static PROCESS_RSS_BYTES: OnceLock<Histogram<u64>> = OnceLock::new();
static PROCESS_CPU_PERCENT: OnceLock<Histogram<f64>> = OnceLock::new();

fn api_meter() -> &'static Meter {
    API_METER.get_or_init(|| global::meter("beacon.api"))
}

fn api_query_requests_total() -> &'static Counter<u64> {
    API_QUERY_REQUESTS_TOTAL.get_or_init(|| {
        api_meter()
            .u64_counter("beacon.query.requests.total")
            .with_description("Total number of query requests")
            .build()
    })
}

fn api_query_errors_total() -> &'static Counter<u64> {
    API_QUERY_ERRORS_TOTAL.get_or_init(|| {
        api_meter()
            .u64_counter("beacon.query.errors.total")
            .with_description("Total number of query request errors")
            .build()
    })
}

fn api_query_duration_ms() -> &'static Histogram<f64> {
    API_QUERY_DURATION_MS.get_or_init(|| {
        api_meter()
            .f64_histogram("beacon.query.duration.ms")
            .with_description("Query request duration in milliseconds")
            .build()
    })
}

fn ws_sql_result_rows() -> &'static Histogram<u64> {
    WS_SQL_RESULT_ROWS.get_or_init(|| {
        api_meter()
            .u64_histogram("beacon.ws_sql.result_rows")
            .with_description("Rows returned by ws_sql queries")
            .build()
    })
}

fn ws_sql_chunks() -> &'static Histogram<u64> {
    WS_SQL_CHUNKS.get_or_init(|| {
        api_meter()
            .u64_histogram("beacon.ws_sql.chunks")
            .with_description("Chunk count emitted by ws_sql queries")
            .build()
    })
}

fn process_rss_bytes() -> &'static Histogram<u64> {
    PROCESS_RSS_BYTES.get_or_init(|| {
        api_meter()
            .u64_histogram("beacon.process.memory.rss.bytes")
            .with_description("Process resident memory size in bytes")
            .build()
    })
}

fn process_cpu_percent() -> &'static Histogram<f64> {
    PROCESS_CPU_PERCENT.get_or_init(|| {
        api_meter()
            .f64_histogram("beacon.process.cpu.percent")
            .with_description("Process CPU utilization percentage")
            .build()
    })
}

pub(crate) fn build_tracer(service_version: &str) -> anyhow::Result<(Tracer, OtelGuard)> {
    let trace_sample_ratio = beacon_config::CONFIG.otel_trace_sample_ratio.clamp(0.0, 1.0);
    let metric_export_interval_secs =
        beacon_config::CONFIG.otel_metric_export_interval_secs.max(1);

    let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(beacon_config::CONFIG.otel_otlp_endpoint.clone())
        .build()
        .context("failed to build OpenTelemetry OTLP span exporter")?;

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(beacon_config::CONFIG.otel_otlp_endpoint.clone())
        .build()
        .context("failed to build OpenTelemetry OTLP metric exporter")?;

    let resource = Resource::new([
        KeyValue::new("service.name", beacon_config::CONFIG.otel_service_name.clone()),
        KeyValue::new(
            "service.namespace",
            beacon_config::CONFIG.otel_service_namespace.clone(),
        ),
        KeyValue::new("service.version", service_version.to_string()),
    ]);

    let provider = TracerProvider::builder()
        .with_batch_exporter(trace_exporter, opentelemetry_sdk::runtime::Tokio)
        .with_sampler(Sampler::TraceIdRatioBased(trace_sample_ratio))
        .with_resource(resource.clone())
        .build();

    let reader = PeriodicReader::builder(metric_exporter, opentelemetry_sdk::runtime::Tokio)
        .with_interval(Duration::from_secs(metric_export_interval_secs))
        .build();

    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(reader)
        .build();

    let tracer = provider.tracer(beacon_config::CONFIG.otel_service_name.as_str());
    global::set_tracer_provider(provider.clone());
    global::set_meter_provider(meter_provider.clone());

    let process_metrics_task = spawn_process_metrics_recorder();

    Ok((
        tracer,
        OtelGuard {
            trace_provider: provider,
            meter_provider,
            process_metrics_task,
        },
    ))
}

pub(crate) fn record_http_query_start() {
    api_query_requests_total().add(
        1,
        &[
            KeyValue::new("protocol", "http"),
            KeyValue::new("route", "/api/query"),
        ],
    );
}

pub(crate) fn record_http_query_error() {
    api_query_errors_total().add(
        1,
        &[
            KeyValue::new("protocol", "http"),
            KeyValue::new("route", "/api/query"),
        ],
    );
}

pub(crate) fn record_http_query_duration(output_type: &str, status: &str, duration: Duration) {
    api_query_duration_ms().record(
        duration.as_secs_f64() * 1000.0,
        &[
            KeyValue::new("protocol", "http"),
            KeyValue::new("route", "/api/query"),
            KeyValue::new("output_type", output_type.to_string()),
            KeyValue::new("status", status.to_string()),
        ],
    );
}

pub(crate) fn record_ws_sql_query_start() {
    api_query_requests_total().add(
        1,
        &[
            KeyValue::new("protocol", "ws_sql"),
            KeyValue::new("route", "/api/ws/sql"),
        ],
    );
}

pub(crate) fn record_ws_sql_query_error() {
    api_query_errors_total().add(
        1,
        &[
            KeyValue::new("protocol", "ws_sql"),
            KeyValue::new("route", "/api/ws/sql"),
        ],
    );
}

pub(crate) fn record_ws_sql_query_duration(status: &str, duration: Duration) {
    api_query_duration_ms().record(
        duration.as_secs_f64() * 1000.0,
        &[
            KeyValue::new("protocol", "ws_sql"),
            KeyValue::new("route", "/api/ws/sql"),
            KeyValue::new("output_type", "stream"),
            KeyValue::new("status", status.to_string()),
        ],
    );
}

pub(crate) fn record_ws_sql_query_result(rows: u64, chunks: usize) {
    ws_sql_result_rows().record(rows, &[]);
    ws_sql_chunks().record(chunks as u64, &[]);
}

fn spawn_process_metrics_recorder() -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval_secs = beacon_config::CONFIG.otel_metric_export_interval_secs.max(1);
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

        let pid = match sysinfo::get_current_pid() {
            Ok(pid) => pid,
            Err(error) => {
                tracing::warn!(?error, "failed to initialize process metrics pid");
                return;
            }
        };

        let mut system = System::new();

        loop {
            interval.tick().await;
            let _ = system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);

            if let Some(process) = system.process(pid) {
                process_rss_bytes().record(process.memory(), &[]);
                process_cpu_percent().record(process.cpu_usage() as f64, &[]);
            }
        }
    })
}
