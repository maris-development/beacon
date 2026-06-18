/// Benchmarks for `build_broadcast_array` — comparing flat materialization of a
/// broadcast column against the run-end-encoded and dictionary-encoded constructions.
///
/// The win is structural: an outer broadcast dim builds REE in `O(num_runs)` instead of
/// `O(rows)`, so the gap widens with the broadcast factor.
use arrow::array::{Float64Array, Int64Array};
use beacon_nd_arrow::{ColumnEncoding, build_broadcast_array};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

fn dstr(s: &str) -> String {
    s.to_string()
}

/// 3D target time×lat×lon. `time` is the outer coordinate (REE-friendly), `lon` the
/// inner coordinate (dictionary-friendly).
fn bench_broadcast_encode(c: &mut Criterion) {
    let n_time = 24usize;
    let n_lat = 64usize;
    let n_lon = 128usize;
    let total = (n_time * n_lat * n_lon) as u64; // 196_608 rows

    let target = vec![dstr("time"), dstr("latitude"), dstr("longitude")];
    let shape = [n_time, n_lat, n_lon];

    let time_vals = Int64Array::from((0..n_time as i64).collect::<Vec<_>>());
    let lon_vals = Float64Array::from((0..n_lon).map(|i| i as f64).collect::<Vec<_>>());

    // Outer coordinate: flat vs run-end-encoded.
    let mut outer = c.benchmark_group("broadcast_encode_outer_time");
    outer.throughput(Throughput::Elements(total));
    for (label, encoding) in [
        ("flat", ColumnEncoding::Flat),
        ("ree", ColumnEncoding::RunEndEncoded),
    ] {
        outer.bench_with_input(BenchmarkId::from_parameter(label), &encoding, |b, &enc| {
            b.iter(|| {
                let out = build_broadcast_array(
                    &time_vals,
                    &[dstr("time")],
                    &target,
                    &shape,
                    enc,
                )
                .unwrap();
                black_box(out)
            });
        });
    }
    outer.finish();

    // Inner coordinate: flat vs dictionary.
    let mut inner = c.benchmark_group("broadcast_encode_inner_lon");
    inner.throughput(Throughput::Elements(total));
    for (label, encoding) in [
        ("flat", ColumnEncoding::Flat),
        ("dict", ColumnEncoding::Dictionary),
    ] {
        inner.bench_with_input(BenchmarkId::from_parameter(label), &encoding, |b, &enc| {
            b.iter(|| {
                let out = build_broadcast_array(
                    &lon_vals,
                    &[dstr("longitude")],
                    &target,
                    &shape,
                    enc,
                )
                .unwrap();
                black_box(out)
            });
        });
    }
    inner.finish();
}

criterion_group!(benches, bench_broadcast_encode);
criterion_main!(benches);
