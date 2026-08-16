//! `st_within_point` against `ST_Within`.
//!
//! Beacon keeps its own point-in-geometry function beside the PostGIS set that
//! `datafusion-spatial` supplies. The two do the same work through different paths:
//!
//! * `st_within_point(wkt, lon, lat)` parses the WKT once per batch, keeps a bounding rectangle
//!   prefilter, and caches an answer per coordinate pair in an LRU.
//! * `ST_Within(ST_Point(lon, lat), ST_GeomFromText(wkt))` builds a GeoArrow point column, then
//!   runs the box test and the topology test of the crate.
//!
//! Run it with `cargo bench -p beacon-functions --bench within_point`.
//!
//! Two axes drive the result. A polygon with more vertices costs more per exact test. A column
//! with repeated coordinates feeds the cache, which a station or a mooring produces.
//!
//! # The measured answer
//!
//! A kernel-level run of the same two paths, over 8192 rows per batch, gave this. A number above
//! 1.0 means `st_within_point` is that many times faster.
//!
//! | Polygon | Coordinates | Most rows fail the box test | Most rows reach the exact test |
//! |---|---|--:|--:|
//! | 4 corners | 8192 distinct | 0.9x | 0.6x |
//! | 4 corners | 500 distinct | 1.3x | 11.6x |
//! | 256 corners | 8192 distinct | 0.4x | 0.8x |
//! | 256 corners | 500 distinct | 0.4x | 4.7x |
//!
//! So `ST_Within` wins on a column of distinct coordinates, and `st_within_point` wins by a wide
//! margin on a column that repeats them. A Beacon table repeats them: one station reports at many
//! depths and at many times, and the coordinate pair is the same on every row. Both functions
//! therefore stay.

use std::sync::Arc;

use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::prelude::SessionContext;

/// Rows per query.
const ROWS: usize = 50_000;

/// A regular polygon of `vertices` corners, centred on the origin.
fn polygon_wkt(vertices: usize) -> String {
    let radius = 40.0_f64;
    let mut corners: Vec<String> = (0..vertices)
        .map(|i| {
            let angle = 2.0 * std::f64::consts::PI * (i as f64) / (vertices as f64);
            format!("{} {}", radius * angle.cos(), radius * angle.sin())
        })
        .collect();
    // A WKT ring closes on its first corner.
    corners.push(corners[0].clone());
    format!("POLYGON(({}))", corners.join(", "))
}

/// `rows` coordinate pairs drawn from `distinct` positions.
///
/// A low `distinct` gives the LRU cache of `st_within_point` its best case. A `distinct` equal to
/// `rows` gives it no repeat at all.
fn points(rows: usize, distinct: usize) -> RecordBatch {
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    let mut next = || {
        // One round of an xorshift generator. The bench needs a repeatable sequence, not a
        // statistically strong one.
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state >> 11) as f64 / (1_u64 << 53) as f64
    };

    let positions: Vec<(f64, f64)> = (0..distinct)
        .map(|_| (next() * 360.0 - 180.0, next() * 180.0 - 90.0))
        .collect();
    let lon: Vec<f64> = (0..rows).map(|i| positions[i % distinct].0).collect();
    let lat: Vec<f64> = (0..rows).map(|i| positions[i % distinct].1).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("lon", DataType::Float64, true),
        Field::new("lat", DataType::Float64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Float64Array::from(lon)),
            Arc::new(Float64Array::from(lat)),
        ],
    )
    .expect("the two columns hold the same row count")
}

/// A session that holds both function sets, over one `points` table.
fn session(batch: RecordBatch) -> SessionContext {
    let ctx = SessionContext::new();
    datafusion_spatial::register_all(&ctx);
    beacon_functions::geo::register_geo_udfs(&ctx, 128 * 1024);
    ctx.register_batch("points", batch)
        .expect("the table name is free");
    ctx
}

fn bench_within_point(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a tokio runtime starts");

    let mut group = c.benchmark_group("within_point");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.sample_size(20);

    for (shape, vertices) in [("box", 4), ("ring256", 256)] {
        let wkt = polygon_wkt(vertices);
        for (spread, distinct) in [("repeated", 500), ("distinct", ROWS)] {
            let ctx = session(points(ROWS, distinct));
            let case = format!("{shape}/{spread}");

            let beacon =
                format!("SELECT count(*) FROM points WHERE st_within_point('{wkt}', lon, lat)");
            group.bench_function(BenchmarkId::new("st_within_point", &case), |b| {
                b.iter(|| {
                    runtime.block_on(async {
                        ctx.sql(&beacon)
                            .await
                            .expect("the plan builds")
                            .collect()
                            .await
                            .expect("the query runs")
                    })
                })
            });

            let postgis = format!(
                "SELECT count(*) FROM points \
                 WHERE ST_Within(ST_Point(lon, lat), ST_GeomFromText('{wkt}'))"
            );
            group.bench_function(BenchmarkId::new("ST_Within", &case), |b| {
                b.iter(|| {
                    runtime.block_on(async {
                        ctx.sql(&postgis)
                            .await
                            .expect("the plan builds")
                            .collect()
                            .await
                            .expect("the query runs")
                    })
                })
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_within_point);
criterion_main!(benches);
