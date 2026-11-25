/// Benchmarks for `NdRecordBatch::try_as_arrow_stream` and
/// `pipe_nd_record_batch_stream` — the end-to-end path from n-dimensional
/// record batches to flat Arrow `RecordBatch` streams.
use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use beacon_nd_arrow::{
    NdRecordBatch, NdToArrowPipeOptions, pipe_nd_record_batch_stream,
    array::{NdArrowArray, NdArrowArrayDispatch},
};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures::StreamExt;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn dstr(s: &str) -> String {
    s.to_string()
}

/// Wrap a typed dispatch into `Arc<dyn NdArrowArray>`.
fn arc_nd<T: beacon_nd_arrow::array::compat_typings::ArrowTypeConversion>(
    data: Vec<T>,
    shape: Vec<usize>,
    dims: Vec<String>,
    fill: Option<T>,
) -> Arc<dyn NdArrowArray> {
    Arc::new(
        NdArrowArrayDispatch::<T>::new_in_mem(data, shape, dims, fill)
            .expect("failed to create NdArrowArray"),
    )
}

// ---------------------------------------------------------------------------
// bench_flatten_1d
// ---------------------------------------------------------------------------

/// Single-dimension NdRecordBatch: time + temperature + scalar platform.
/// Tests the degenerate "no broadcasting needed" path.
fn bench_flatten_1d(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let n = 65_536usize;

    let time_data: Vec<i64> = (0..n as i64).collect();
    let temp_data: Vec<f64> = (0..n).map(|i| 5.0 + (i % 97) as f64 * 0.1).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("temperature", DataType::Float64, false),
        Field::new("platform", DataType::Utf8, true),
    ]));

    let arrays: Vec<Arc<dyn NdArrowArray>> = vec![
        arc_nd(time_data, vec![n], vec![dstr("time")], None),
        arc_nd(temp_data, vec![n], vec![dstr("time")], None),
        arc_nd(vec!["buoy-01".to_string()], vec![], vec![], None),
    ];

    let batch = NdRecordBatch::new("1d".to_string(), schema, arrays).unwrap();

    let total_elements = n as u64 * 3; // 3 columns × n rows after flatten
    let mut group = c.benchmark_group("flatten_1d");

    for chunk_size in [4_096usize, 65_536] {
        group.throughput(Throughput::Elements(total_elements));
        group.bench_with_input(
            BenchmarkId::new("try_as_arrow_stream", chunk_size),
            &chunk_size,
            |b, &cs| {
                b.iter(|| {
                    rt.block_on(async {
                        let stream = batch.try_as_arrow_stream(cs).await.unwrap();
                        let results: Vec<anyhow::Result<RecordBatch>> =
                            stream.collect().await;
                        black_box(results)
                    })
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// bench_flatten_3d
// ---------------------------------------------------------------------------

/// Mixed-rank NdRecordBatch with 3D target: time×lat×lon temperature +
/// 1D coordinate arrays + scalar platform.
fn bench_flatten_3d(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let n_time = 24usize;
    let n_lat = 64usize;
    let n_lon = 128usize;
    let n_2d = n_lat * n_lon;
    let n_3d = n_time * n_2d;

    let time_data: Vec<i64> = (0..n_time as i64).collect();
    let lat_data: Vec<f64> = (0..n_lat)
        .map(|i| -90.0 + i as f64 * 180.0 / (n_lat - 1) as f64)
        .collect();
    let lon_data: Vec<f64> = (0..n_lon)
        .map(|i| -180.0 + i as f64 * 360.0 / (n_lon - 1) as f64)
        .collect();
    let temp_data: Vec<f64> = (0..n_3d)
        .map(|i| {
            let t = i / n_2d;
            let lat = (i % n_2d) / n_lon;
            let lon = (i % n_2d) % n_lon;
            10.0 + t as f64 * 0.3 + lat as f64 * 0.02 + lon as f64 * 0.01
        })
        .collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("latitude", DataType::Float64, false),
        Field::new("longitude", DataType::Float64, false),
        Field::new("temperature", DataType::Float64, false),
        Field::new("platform", DataType::Utf8, true),
    ]));

    let arrays: Vec<Arc<dyn NdArrowArray>> = vec![
        arc_nd(time_data, vec![n_time], vec![dstr("time")], None),
        arc_nd(lat_data, vec![n_lat], vec![dstr("latitude")], None),
        arc_nd(lon_data, vec![n_lon], vec![dstr("longitude")], None),
        arc_nd(
            temp_data,
            vec![n_time, n_lat, n_lon],
            vec![dstr("time"), dstr("latitude"), dstr("longitude")],
            None,
        ),
        arc_nd(vec!["vessel-42".to_string()], vec![], vec![], None),
    ];

    let batch = NdRecordBatch::new("3d".to_string(), schema, arrays).unwrap();
    let total_elements = n_3d as u64 * 5; // 5 columns after full broadcast

    let mut group = c.benchmark_group("flatten_3d");

    for chunk_size in [65_536usize, 524_288] {
        group.throughput(Throughput::Elements(total_elements));
        group.bench_with_input(
            BenchmarkId::new("try_as_arrow_stream", chunk_size),
            &chunk_size,
            |b, &cs| {
                b.iter(|| {
                    rt.block_on(async {
                        let stream = batch.try_as_arrow_stream(cs).await.unwrap();
                        let results: Vec<anyhow::Result<RecordBatch>> =
                            stream.collect().await;
                        black_box(results)
                    })
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// bench_flatten_pipe_stream
// ---------------------------------------------------------------------------

/// Multiple NdRecordBatches piped through `pipe_nd_record_batch_stream`.
/// Exercises concurrent batch processing and buffered output merging.
fn bench_flatten_pipe_stream(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let n_batches = 8usize;
    let n_time = 12usize;
    let n_lat = 32usize;
    let n_lon = 64usize;
    let n_2d = n_lat * n_lon;
    let n_3d = n_time * n_2d;

    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("temperature", DataType::Float64, false),
        Field::new("platform", DataType::Utf8, true),
    ]));

    // Pre-build arrays for all batches (Arc, so cheap to clone).
    let batches: Vec<Arc<dyn NdArrowArray>> = {
        let time_data: Vec<i64> = (0..n_time as i64).collect();
        let temp_data: Vec<f64> = (0..n_3d).map(|i| i as f64 * 0.01).collect();
        vec![
            arc_nd(time_data, vec![n_time], vec![dstr("time")], None),
            arc_nd(
                temp_data,
                vec![n_time, n_lat, n_lon],
                vec![dstr("time"), dstr("latitude"), dstr("longitude")],
                None,
            ),
            arc_nd(
                vec!["ship-A".to_string()],
                vec![],
                vec![],
                None,
            ),
        ]
    };

    let total_elements = (n_3d * 3 * n_batches) as u64;

    let mut group = c.benchmark_group("flatten_pipe_stream");
    group.throughput(Throughput::Elements(total_elements));

    for parallelism in [1usize, 4] {
        group.bench_with_input(
            BenchmarkId::new("pipe_nd_record_batch_stream", parallelism),
            &parallelism,
            |b, &par| {
                let schema = schema.clone();
                let batches = batches.clone();
                let options = NdToArrowPipeOptions {
                    preferred_chunk_size: 65_536,
                    batch_parallelism: par,
                    output_buffer: par.max(1),
                };

                b.iter(|| {
                    rt.block_on(async {
                        let input_batches: Vec<anyhow::Result<NdRecordBatch>> = (0..n_batches)
                            .map(|i| {
                                NdRecordBatch::new(
                                    format!("batch_{i}"),
                                    schema.clone(),
                                    batches.clone(),
                                )
                            })
                            .collect();
                        let input_stream = futures::stream::iter(input_batches);
                        let output = pipe_nd_record_batch_stream(input_stream, options);
                        let results: Vec<anyhow::Result<RecordBatch>> =
                            output.collect().await;
                        black_box(results)
                    })
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// criterion wiring
// ---------------------------------------------------------------------------

fn criterion_benchmark(c: &mut Criterion) {
    bench_flatten_1d(c);
    bench_flatten_3d(c);
    bench_flatten_pipe_stream(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
