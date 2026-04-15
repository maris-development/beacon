/// Benchmarks for `ArrowTypeConversion` — the innermost hot-path that
/// converts a Rust slice into an Arrow `ArrayRef`, optionally encoding
/// fill-value elements as nulls via a compact validity bitmap.
///
/// Cases covered:
/// - `arrow_from_array_view` (no nulls)           — various types and sizes
/// - `arrow_from_array_view_with_fill` (2% nulls) — sparse nullability
/// - `arrow_from_array_view_with_fill` (50% nulls)— dense nullability
/// - `arrow_from_array_view_with_fill` (0% nulls) — short-circuit path
use beacon_nd_arrow::array::compat_typings::ArrowTypeConversion;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

const SIZES: &[usize] = &[1_024, 65_536, 1_048_576];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a Vec<f64> of `n` elements with `null_fraction` of them set to
/// `fill_value`.
fn make_f64_data(n: usize, fill_value: f64, null_fraction: f64) -> Vec<f64> {
    (0..n)
        .map(|i| {
            if (i as f64 / n as f64) < null_fraction {
                fill_value
            } else {
                i as f64 * 0.001
            }
        })
        .collect()
}

fn make_f32_data(n: usize, fill_value: f32, null_fraction: f32) -> Vec<f32> {
    (0..n)
        .map(|i| {
            if (i as f32 / n as f32) < null_fraction {
                fill_value
            } else {
                i as f32 * 0.001
            }
        })
        .collect()
}

fn make_i32_data(n: usize, fill_value: i32, null_fraction: f64) -> Vec<i32> {
    (0..n)
        .map(|i| {
            if (i as f64 / n as f64) < null_fraction {
                fill_value
            } else {
                i as i32
            }
        })
        .collect()
}

fn make_i64_data(n: usize, fill_value: i64, null_fraction: f64) -> Vec<i64> {
    (0..n)
        .map(|i| {
            if (i as f64 / n as f64) < null_fraction {
                fill_value
            } else {
                i as i64
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// bench_convert_no_fill
// ---------------------------------------------------------------------------

/// Baseline conversion without any fill-value / null handling.
fn bench_convert_no_fill(c: &mut Criterion) {
    let mut group = c.benchmark_group("convert_no_fill");

    for &n in SIZES {
        group.throughput(Throughput::Elements(n as u64));

        let f64_data: Vec<f64> = (0..n).map(|i| i as f64 * 0.001).collect();
        let f32_data: Vec<f32> = (0..n).map(|i| i as f32 * 0.001).collect();
        let i32_data: Vec<i32> = (0..n).map(|i| i as i32).collect();
        let i64_data: Vec<i64> = (0..n).map(|i| i as i64).collect();

        group.bench_with_input(BenchmarkId::new("f64", n), &n, |b, _| {
            b.iter(|| {
                black_box(<f64 as ArrowTypeConversion>::arrow_from_array_view(&f64_data).unwrap())
            });
        });

        group.bench_with_input(BenchmarkId::new("f32", n), &n, |b, _| {
            b.iter(|| {
                black_box(<f32 as ArrowTypeConversion>::arrow_from_array_view(&f32_data).unwrap())
            });
        });

        group.bench_with_input(BenchmarkId::new("i32", n), &n, |b, _| {
            b.iter(|| {
                black_box(<i32 as ArrowTypeConversion>::arrow_from_array_view(&i32_data).unwrap())
            });
        });

        group.bench_with_input(BenchmarkId::new("i64", n), &n, |b, _| {
            b.iter(|| {
                black_box(<i64 as ArrowTypeConversion>::arrow_from_array_view(&i64_data).unwrap())
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// bench_convert_with_fill_sparse
// ---------------------------------------------------------------------------

/// ~2% of elements equal the fill value — bitmap is built but null_count is
/// small. Represents typical sparse "missing sensor value" data.
fn bench_convert_with_fill_sparse(c: &mut Criterion) {
    let mut group = c.benchmark_group("convert_with_fill_sparse_2pct");

    for &n in SIZES {
        group.throughput(Throughput::Elements(n as u64));

        let fill_f64 = -9999.0f64;
        let fill_f32 = -9999.0f32;
        let fill_i32 = i32::MIN;
        let fill_i64 = i64::MIN;

        let f64_data = make_f64_data(n, fill_f64, 0.02);
        let f32_data = make_f32_data(n, fill_f32, 0.02);
        let i32_data = make_i32_data(n, fill_i32, 0.02);
        let i64_data = make_i64_data(n, fill_i64, 0.02);

        group.bench_with_input(BenchmarkId::new("f64", n), &n, |b, _| {
            b.iter(|| {
                black_box(
                    <f64 as ArrowTypeConversion>::arrow_from_array_view_with_fill(
                        &f64_data, &fill_f64,
                    )
                    .unwrap(),
                )
            });
        });

        group.bench_with_input(BenchmarkId::new("f32", n), &n, |b, _| {
            b.iter(|| {
                black_box(
                    <f32 as ArrowTypeConversion>::arrow_from_array_view_with_fill(
                        &f32_data, &fill_f32,
                    )
                    .unwrap(),
                )
            });
        });

        group.bench_with_input(BenchmarkId::new("i32", n), &n, |b, _| {
            b.iter(|| {
                black_box(
                    <i32 as ArrowTypeConversion>::arrow_from_array_view_with_fill(
                        &i32_data, &fill_i32,
                    )
                    .unwrap(),
                )
            });
        });

        group.bench_with_input(BenchmarkId::new("i64", n), &n, |b, _| {
            b.iter(|| {
                black_box(
                    <i64 as ArrowTypeConversion>::arrow_from_array_view_with_fill(
                        &i64_data, &fill_i64,
                    )
                    .unwrap(),
                )
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// bench_convert_with_fill_dense
// ---------------------------------------------------------------------------

/// ~50% of elements are null — stresses the bitmap build and null_count scan
/// path as null density grows.
fn bench_convert_with_fill_dense(c: &mut Criterion) {
    let mut group = c.benchmark_group("convert_with_fill_dense_50pct");

    for &n in SIZES {
        group.throughput(Throughput::Elements(n as u64));

        let fill_f64 = -9999.0f64;
        let fill_i64 = i64::MIN;

        let f64_data = make_f64_data(n, fill_f64, 0.50);
        let i64_data = make_i64_data(n, fill_i64, 0.50);

        group.bench_with_input(BenchmarkId::new("f64", n), &n, |b, _| {
            b.iter(|| {
                black_box(
                    <f64 as ArrowTypeConversion>::arrow_from_array_view_with_fill(
                        &f64_data, &fill_f64,
                    )
                    .unwrap(),
                )
            });
        });

        group.bench_with_input(BenchmarkId::new("i64", n), &n, |b, _| {
            b.iter(|| {
                black_box(
                    <i64 as ArrowTypeConversion>::arrow_from_array_view_with_fill(
                        &i64_data, &fill_i64,
                    )
                    .unwrap(),
                )
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// bench_convert_fill_shortcircuit
// ---------------------------------------------------------------------------

/// No element matches the fill value — exercises the early-return path where
/// the bitmap is built but `null_count == 0` so the base array is returned
/// immediately without attaching a null buffer.
fn bench_convert_fill_shortcircuit(c: &mut Criterion) {
    let mut group = c.benchmark_group("convert_with_fill_shortcircuit_0pct");

    for &n in SIZES {
        group.throughput(Throughput::Elements(n as u64));

        // Data has no elements equal to the fill value.
        let fill_f64 = -9999.0f64;
        let fill_i64 = i64::MIN;

        let f64_data: Vec<f64> = (0..n).map(|i| i as f64 * 0.001).collect();
        let i64_data: Vec<i64> = (0..n).map(|i| i as i64).collect();

        group.bench_with_input(BenchmarkId::new("f64", n), &n, |b, _| {
            b.iter(|| {
                black_box(
                    <f64 as ArrowTypeConversion>::arrow_from_array_view_with_fill(
                        &f64_data, &fill_f64,
                    )
                    .unwrap(),
                )
            });
        });

        group.bench_with_input(BenchmarkId::new("i64", n), &n, |b, _| {
            b.iter(|| {
                black_box(
                    <i64 as ArrowTypeConversion>::arrow_from_array_view_with_fill(
                        &i64_data, &fill_i64,
                    )
                    .unwrap(),
                )
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// criterion wiring
// ---------------------------------------------------------------------------

fn criterion_benchmark(c: &mut Criterion) {
    bench_convert_no_fill(c);
    bench_convert_with_fill_sparse(c);
    bench_convert_with_fill_dense(c);
    bench_convert_fill_shortcircuit(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
