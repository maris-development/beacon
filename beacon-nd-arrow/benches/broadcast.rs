/// Benchmarks for `NdArrowArray::broadcast` — the axis-alignment and
/// ndarray permute/insert/broadcast materialisation path.
///
/// Each case starts from one or more source arrays and measures how long it
/// takes to broadcast them to a common N-dimensional target and then convert
/// each result to an Arrow `ArrayRef` (i.e. the per-chunk work that
/// `try_as_arrow_stream` performs on every iteration).
use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use beacon_nd_arrow::array::{NdArrowArray, NdArrowArrayDispatch};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn dstr(s: &str) -> String {
    s.to_string()
}

type DynArray = Arc<dyn NdArrowArray>;

fn make_f64(data: Vec<f64>, shape: Vec<usize>, dims: Vec<String>) -> DynArray {
    Arc::new(
        NdArrowArrayDispatch::<f64>::new_in_mem(data, shape, dims, None)
            .expect("failed to create f64 NdArrowArray"),
    )
}

fn make_i64(data: Vec<i64>, shape: Vec<usize>, dims: Vec<String>) -> DynArray {
    Arc::new(
        NdArrowArrayDispatch::<i64>::new_in_mem(data, shape, dims, None)
            .expect("failed to create i64 NdArrowArray"),
    )
}

fn make_string(data: Vec<String>, shape: Vec<usize>, dims: Vec<String>) -> DynArray {
    Arc::new(
        NdArrowArrayDispatch::<String>::new_in_mem(data, shape, dims, None)
            .expect("failed to create String NdArrowArray"),
    )
}

/// Broadcast `arrays` to `(target_shape, target_dims)`, call
/// `as_arrow_array_ref()` on each, then assemble a `RecordBatch`.
async fn broadcast_and_build_batch(
    arrays: &[(DynArray, Vec<usize>, Vec<String>)],
    schema: Arc<Schema>,
) -> RecordBatch {
    let mut arrow_cols = Vec::with_capacity(arrays.len());
    for (array, target_shape, target_dims) in arrays {
        let broadcasted = array
            .broadcast(target_shape, target_dims)
            .await
            .expect("broadcast failed");
        let col = broadcasted
            .as_arrow_array_ref()
            .await
            .expect("as_arrow_array_ref failed");
        arrow_cols.push(col);
    }
    RecordBatch::try_new(schema, arrow_cols).expect("RecordBatch::try_new failed")
}

// ---------------------------------------------------------------------------
// bench_broadcast_scalar_to_nd
// ---------------------------------------------------------------------------

/// A scalar (single-element) array broadcast to 1-D, 2-D, and 3-D targets.
/// Measures the overhead of the degenerate case where the source has no named
/// dimensions.
fn bench_broadcast_scalar_to_nd(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let scalar = make_string(vec!["sensor-01".to_string()], vec![], vec![]);

    let cases: &[(&str, Vec<usize>, Vec<String>)] = &[
        ("1d_8k", vec![8_192], vec![dstr("time")]),
        ("2d_128x256", vec![128, 256], vec![dstr("lat"), dstr("lon")]),
        (
            "3d_24x64x128",
            vec![24, 64, 128],
            vec![dstr("time"), dstr("lat"), dstr("lon")],
        ),
    ];

    let mut group = c.benchmark_group("broadcast_scalar_to_nd");

    for (case_name, target_shape, target_dims) in cases {
        let n_out: usize = target_shape.iter().product();
        group.throughput(Throughput::Elements(n_out as u64));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "platform",
            DataType::Utf8,
            true,
        )]));
        let arrays = vec![(scalar.clone(), target_shape.clone(), target_dims.clone())];

        group.bench_function(BenchmarkId::new("scalar_broadcast", case_name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    black_box(broadcast_and_build_batch(&arrays, schema.clone()).await)
                })
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// bench_broadcast_1d_to_nd
// ---------------------------------------------------------------------------

/// A single 1-D array broadcast to 2-D and 3-D targets.
/// Exercises axis insertion and the ndarray `broadcast` call.
fn bench_broadcast_1d_to_nd(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let n_time = 256usize;
    let n_lat = 64usize;
    let n_lon = 128usize;

    let time_array = make_i64(
        (0..n_time as i64).collect(),
        vec![n_time],
        vec![dstr("time")],
    );

    let cases: &[(&str, Vec<usize>, Vec<String>, usize)] = &[
        (
            "1d_to_2d",
            vec![n_time, n_lat],
            vec![dstr("time"), dstr("lat")],
            n_time * n_lat,
        ),
        (
            "1d_to_3d",
            vec![n_time, n_lat, n_lon],
            vec![dstr("time"), dstr("lat"), dstr("lon")],
            n_time * n_lat * n_lon,
        ),
    ];

    let mut group = c.benchmark_group("broadcast_1d_to_nd");

    for (case_name, target_shape, target_dims, n_out) in cases {
        group.throughput(Throughput::Elements(*n_out as u64));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Int64,
            false,
        )]));
        let arrays = vec![(
            time_array.clone(),
            target_shape.clone(),
            target_dims.clone(),
        )];

        group.bench_function(BenchmarkId::new("1d_broadcast", case_name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    black_box(broadcast_and_build_batch(&arrays, schema.clone()).await)
                })
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// bench_broadcast_2d_to_3d
// ---------------------------------------------------------------------------

/// A 2-D lat/lon grid broadcast to a 3-D time/lat/lon target.
/// Exercises axis permutation + insertion.
fn bench_broadcast_2d_to_3d(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let cases: &[(&str, usize, usize, usize)] = &[
        ("small_8x16x32", 8, 16, 32),
        ("medium_24x64x128", 24, 64, 128),
        ("large_48x128x256", 48, 128, 256),
    ];

    let mut group = c.benchmark_group("broadcast_2d_to_3d");

    for (case_name, n_time, n_lat, n_lon) in cases {
        let n_2d = n_lat * n_lon;
        let n_out = n_time * n_2d;

        let lat_data: Vec<f64> = (0..n_2d)
            .map(|i| -90.0 + (i / n_lon) as f64 * 180.0 / (n_lat - 1) as f64)
            .collect();
        let lon_data: Vec<f64> = (0..n_2d)
            .map(|i| -180.0 + (i % n_lon) as f64 * 360.0 / (n_lon - 1) as f64)
            .collect();

        let lat_array = make_f64(
            lat_data,
            vec![*n_lat, *n_lon],
            vec![dstr("lat"), dstr("lon")],
        );
        let lon_array = make_f64(
            lon_data,
            vec![*n_lat, *n_lon],
            vec![dstr("lat"), dstr("lon")],
        );

        let target_shape = vec![*n_time, *n_lat, *n_lon];
        let target_dims = vec![dstr("time"), dstr("lat"), dstr("lon")];

        let schema = Arc::new(Schema::new(vec![
            Field::new("latitude", DataType::Float64, false),
            Field::new("longitude", DataType::Float64, false),
        ]));
        let arrays = vec![
            (lat_array, target_shape.clone(), target_dims.clone()),
            (lon_array, target_shape.clone(), target_dims.clone()),
        ];

        group.throughput(Throughput::Elements(n_out as u64 * 2));
        group.bench_function(BenchmarkId::new("2d_to_3d", case_name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    black_box(broadcast_and_build_batch(&arrays, schema.clone()).await)
                })
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// bench_broadcast_mixed
// ---------------------------------------------------------------------------

/// All four rank combinations broadcast to a 3-D target simultaneously,
/// matching what `try_as_arrow_stream` does per chunk for a mixed-rank batch.
fn bench_broadcast_mixed(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let cases: &[(&str, usize, usize, usize)] = &[
        ("small_8x16x32", 8, 16, 32),
        ("medium_24x64x128", 24, 64, 128),
    ];

    let mut group = c.benchmark_group("broadcast_mixed_rank");

    for (case_name, n_time, n_lat, n_lon) in cases {
        let n_2d = n_lat * n_lon;
        let n_3d = n_time * n_2d;

        // 1-D time
        let time_array = make_i64(
            (0..*n_time as i64).collect(),
            vec![*n_time],
            vec![dstr("time")],
        );
        // 2-D lat/lon grid
        let lat_data: Vec<f64> = (0..n_2d).map(|i| -90.0 + (i / n_lon) as f64).collect();
        let lat_array = make_f64(
            lat_data,
            vec![*n_lat, *n_lon],
            vec![dstr("lat"), dstr("lon")],
        );
        // 3-D temperature
        let temp_data: Vec<f64> = (0..n_3d).map(|i| i as f64 * 0.01).collect();
        let temp_array = make_f64(
            temp_data,
            vec![*n_time, *n_lat, *n_lon],
            vec![dstr("time"), dstr("lat"), dstr("lon")],
        );
        // scalar
        let platform_array = make_string(vec!["platform-X".to_string()], vec![], vec![]);

        let target_shape = vec![*n_time, *n_lat, *n_lon];
        let target_dims = vec![dstr("time"), dstr("lat"), dstr("lon")];

        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("latitude", DataType::Float64, false),
            Field::new("temperature", DataType::Float64, false),
            Field::new("platform", DataType::Utf8, true),
        ]));

        let arrays = vec![
            (time_array, target_shape.clone(), target_dims.clone()),
            (lat_array, target_shape.clone(), target_dims.clone()),
            (temp_array, target_shape.clone(), target_dims.clone()),
            (platform_array, target_shape.clone(), target_dims.clone()),
        ];

        group.throughput(Throughput::Elements(n_3d as u64 * 4));
        group.bench_function(BenchmarkId::new("mixed_1d_2d_3d_scalar", case_name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    black_box(broadcast_and_build_batch(&arrays, schema.clone()).await)
                })
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// criterion wiring
// ---------------------------------------------------------------------------

fn criterion_benchmark(c: &mut Criterion) {
    bench_broadcast_scalar_to_nd(c);
    bench_broadcast_1d_to_nd(c);
    bench_broadcast_2d_to_3d(c);
    bench_broadcast_mixed(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
