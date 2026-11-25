/// Benchmarks for `NdArrowArray::subset` / `ArrayBackend::read_subset` —
/// the ndarray slice-and-copy path used by every broadcast and chunk read.
///
/// Three access patterns are exercised for each array dimensionality:
///
/// - **full**  — read the whole array in one subset (best case; C-contiguous
///               copy of the entire buffer).
/// - **window_contiguous** — inner sub-region whose last axis spans the full
///               width, so each 2-D "row" is a contiguous slice.
/// - **window_partial_last** — inner sub-region that does NOT span the full
///               last axis, forcing ndarray to copy each row in smaller pieces.
use std::sync::Arc;

use beacon_nd_arrow::array::{NdArrowArray, NdArrowArrayDispatch, subset::ArraySubset};
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

/// Construct an `ArraySubset` from owned `start` and `shape` vecs.
fn sub(start: Vec<usize>, shape: Vec<usize>) -> ArraySubset {
    ArraySubset { start, shape }
}

// ---------------------------------------------------------------------------
// bench_subset_1d
// ---------------------------------------------------------------------------

/// 1-D array reads: full read vs. contiguous windows of various sizes.
fn bench_subset_1d(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let n = 1_048_576usize;
    let data: Vec<f64> = (0..n).map(|i| i as f64).collect();
    let array = make_f64(data, vec![n], vec![dstr("x")]);

    // (name, start, shape)
    let cases: &[(&str, Vec<usize>, Vec<usize>)] = &[
        ("full", vec![0], vec![n]),
        ("window_256k", vec![128_000], vec![262_144]),
        ("window_64k", vec![100_000], vec![65_536]),
    ];

    let mut group = c.benchmark_group("subset_1d");

    for (name, start, shape) in cases {
        let n_out: usize = shape.iter().product();
        group.throughput(Throughput::Elements(n_out as u64));

        let start = start.clone();
        let shape = shape.clone();

        group.bench_function(BenchmarkId::new("read_subset", name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let s = sub(start.clone(), shape.clone());
                    black_box(array.subset(s).await.unwrap())
                })
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// bench_subset_2d
// ---------------------------------------------------------------------------

/// 2-D array reads.
///
/// - **full**: whole [n_lat × n_lon].
/// - **window_contiguous**: inner rows, full column width → contiguous rows.
/// - **window_partial_last**: inner rows AND inner columns → non-contiguous.
fn bench_subset_2d(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let n_lat = 1024usize;
    let n_lon = 2048usize;
    let n = n_lat * n_lon;
    let data: Vec<f64> = (0..n).map(|i| i as f64).collect();
    let array = make_f64(
        data,
        vec![n_lat, n_lon],
        vec![dstr("lat"), dstr("lon")],
    );

    // (name, start, shape)
    let cases: &[(&str, Vec<usize>, Vec<usize>)] = &[
        ("full", vec![0, 0], vec![n_lat, n_lon]),
        (
            "window_contiguous_half_rows",
            vec![256, 0],
            vec![512, n_lon], // full last axis → contiguous rows
        ),
        (
            "window_partial_last",
            vec![256, 512],
            vec![512, 1024], // partial last axis → non-contiguous
        ),
        (
            "small_inner_window",
            vec![100, 100],
            vec![64, 128],
        ),
    ];

    let mut group = c.benchmark_group("subset_2d");

    for (name, start, shape) in cases {
        let n_out: usize = shape.iter().product();
        group.throughput(Throughput::Elements(n_out as u64));

        let start = start.clone();
        let shape = shape.clone();

        group.bench_function(BenchmarkId::new("read_subset", name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let s = sub(start.clone(), shape.clone());
                    black_box(array.subset(s).await.unwrap())
                })
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// bench_subset_3d
// ---------------------------------------------------------------------------

/// 3-D array reads exercising the full spectrum of access patterns.
fn bench_subset_3d(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let n_time = 48usize;
    let n_lat = 256usize;
    let n_lon = 512usize;
    let n = n_time * n_lat * n_lon;
    let data: Vec<f64> = (0..n).map(|i| i as f64 * 0.001).collect();
    let array = make_f64(
        data,
        vec![n_time, n_lat, n_lon],
        vec![dstr("time"), dstr("lat"), dstr("lon")],
    );

    let chunk_lon = (65_536 / n_lat).min(n_lon);

    // (name, start, shape)
    let cases: &[(&str, Vec<usize>, Vec<usize>)] = &[
        ("full", vec![0, 0, 0], vec![n_time, n_lat, n_lon]),
        (
            "single_time_full_spatial",
            vec![10, 0, 0],
            vec![1, n_lat, n_lon], // contiguous 2-D slice
        ),
        (
            "single_time_partial_spatial",
            vec![10, 64, 128],
            vec![1, 128, 256], // inner spatial window
        ),
        (
            "multi_time_partial_spatial",
            vec![5, 64, 128],
            vec![12, 128, 256], // multiple time steps, inner spatial
        ),
        (
            "chunk_like_65k",
            vec![0, 0, 0],
            vec![1, n_lat, chunk_lon.max(1)],
        ),
    ];

    let mut group = c.benchmark_group("subset_3d");

    for (name, start, shape) in cases {
        let n_out: usize = shape.iter().product();
        if n_out == 0 {
            continue;
        }
        group.throughput(Throughput::Elements(n_out as u64));

        let start = start.clone();
        let shape = shape.clone();

        group.bench_function(BenchmarkId::new("read_subset", name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let s = sub(start.clone(), shape.clone());
                    black_box(array.subset(s).await.unwrap())
                })
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// bench_subset_to_arrow_with_fill
// ---------------------------------------------------------------------------

/// Compares subset → Arrow materialisation with and without fill-value null
/// encoding to verify the null-bitmap path doesn't regress subset reads.
fn bench_subset_to_arrow_with_fill(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let n_lat = 256usize;
    let n_lon = 256usize;
    let n = n_lat * n_lon;
    let fill = -9999.0f64;

    // ~10% fill values
    let data: Vec<f64> = (0..n)
        .map(|i| if i % 10 == 0 { fill } else { i as f64 * 0.01 })
        .collect();

    let array_with_fill = Arc::new(
        NdArrowArrayDispatch::<f64>::new_in_mem(
            data.clone(),
            vec![n_lat, n_lon],
            vec![dstr("lat"), dstr("lon")],
            Some(fill),
        )
        .unwrap(),
    );
    let array_no_fill = make_f64(
        data,
        vec![n_lat, n_lon],
        vec![dstr("lat"), dstr("lon")],
    );

    let start = vec![32, 32];
    let shape = vec![128, 128];

    let mut group = c.benchmark_group("subset_to_arrow_fill");
    group.throughput(Throughput::Elements((128 * 128) as u64));

    group.bench_function("no_fill_value", |b| {
        b.iter(|| {
            rt.block_on(async {
                let s = sub(start.clone(), shape.clone());
                let sub_array = array_no_fill.subset(s).await.unwrap();
                black_box(sub_array.as_arrow_array_ref().await.unwrap())
            })
        });
    });

    group.bench_function("with_fill_value_10pct_nulls", |b| {
        b.iter(|| {
            rt.block_on(async {
                let s = sub(start.clone(), shape.clone());
                let sub_array = array_with_fill.subset(s).await.unwrap();
                black_box(sub_array.as_arrow_array_ref().await.unwrap())
            })
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// criterion wiring
// ---------------------------------------------------------------------------

fn criterion_benchmark(c: &mut Criterion) {
    bench_subset_1d(c);
    bench_subset_2d(c);
    bench_subset_3d(c);
    bench_subset_to_arrow_with_fill(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
