use std::sync::OnceLock;

use beacon_planner::metrics::ConsolidatedMetrics;
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, Meter},
    KeyValue,
};

static CORE_METER: OnceLock<Meter> = OnceLock::new();
static QUERY_COMPLETED_TOTAL: OnceLock<Counter<u64>> = OnceLock::new();
static QUERY_FAILURES_TOTAL: OnceLock<Counter<u64>> = OnceLock::new();
static QUERY_RESULT_ROWS: OnceLock<Histogram<u64>> = OnceLock::new();
static QUERY_RESULT_SIZE_BYTES: OnceLock<Histogram<u64>> = OnceLock::new();
static QUERY_PEAK_OUTPUT_BATCH_BYTES: OnceLock<Histogram<u64>> = OnceLock::new();
static QUERY_PROCESS_MEMORY_DELTA_BYTES: OnceLock<Histogram<f64>> = OnceLock::new();

fn core_meter() -> &'static Meter {
    CORE_METER.get_or_init(|| global::meter("beacon.core"))
}

fn query_completed_total() -> &'static Counter<u64> {
    QUERY_COMPLETED_TOTAL.get_or_init(|| {
        core_meter()
            .u64_counter("beacon.query.completed.total")
            .with_description("Total number of successfully completed queries")
            .build()
    })
}

fn query_failures_total() -> &'static Counter<u64> {
    QUERY_FAILURES_TOTAL.get_or_init(|| {
        core_meter()
            .u64_counter("beacon.query.failures.total")
            .with_description("Total number of failed queries")
            .build()
    })
}

fn query_result_rows() -> &'static Histogram<u64> {
    QUERY_RESULT_ROWS.get_or_init(|| {
        core_meter()
            .u64_histogram("beacon.query.result_rows")
            .with_description("Result row count for completed queries")
            .build()
    })
}

fn query_result_size_bytes() -> &'static Histogram<u64> {
    QUERY_RESULT_SIZE_BYTES.get_or_init(|| {
        core_meter()
            .u64_histogram("beacon.query.result_size.bytes")
            .with_description("Result size in bytes for completed queries")
            .build()
    })
}

fn query_peak_output_batch_bytes() -> &'static Histogram<u64> {
    QUERY_PEAK_OUTPUT_BATCH_BYTES.get_or_init(|| {
        core_meter()
            .u64_histogram("beacon.query.peak_output_batch.bytes")
            .with_description("Peak output batch size in bytes for completed queries")
            .build()
    })
}

fn query_process_memory_delta_bytes() -> &'static Histogram<f64> {
    QUERY_PROCESS_MEMORY_DELTA_BYTES.get_or_init(|| {
        core_meter()
            .f64_histogram("beacon.query.process_memory_delta.bytes")
            .with_description("Process memory delta in bytes between query start and end")
            .build()
    })
}

pub fn record_query_completion(output_type: &'static str, consolidated: &ConsolidatedMetrics) {
    query_completed_total().add(
        1,
        &[
            KeyValue::new("status", "ok"),
            KeyValue::new("output_type", output_type),
        ],
    );

    query_result_rows().record(
        consolidated.result_num_rows,
        &[KeyValue::new("output_type", output_type)],
    );

    query_result_size_bytes().record(
        consolidated.result_size_in_bytes,
        &[KeyValue::new("output_type", output_type)],
    );

    query_peak_output_batch_bytes().record(
        consolidated.peak_output_batch_bytes,
        &[KeyValue::new("output_type", output_type)],
    );

    if let Some(delta_bytes) = consolidated.process_memory_delta_bytes {
        query_process_memory_delta_bytes().record(
            delta_bytes as f64,
            &[KeyValue::new("output_type", output_type)],
        );
    }
}

pub fn record_query_failure(output_type: &'static str, stage: &'static str) {
    query_failures_total().add(
        1,
        &[
            KeyValue::new("status", "error"),
            KeyValue::new("output_type", output_type),
            KeyValue::new("stage", stage),
        ],
    );
}
