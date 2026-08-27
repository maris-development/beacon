//! Projections and filters over an nd scan, end to end.
//!
//! A NetCDF/Zarr/TIFF/HDF5 scan puts `NdSourceExec` and `NdBroadcastExec`
//! between the file scan and the rest of the plan. Two things ride on that:
//!
//! * The nodes refuse a projection, so an alias or a computed column never
//!   reaches the file source — the scan below only ever gets a plain column
//!   list. That is why [#382](https://github.com/maris-development/beacon/issues/382)
//!   left the nd formats alone, and the first test here pins it.
//! * With the nd pipeline enabled, `NdFilterPushdown` sinks the element-wise
//!   conjuncts of a `WHERE` below the broadcast into `NdFilterExec`, which
//!   evaluates each conjunct on the sub-grid its inputs span and records the
//!   surviving cells as a grid selection. A fully-sunk predicate leaves *no*
//!   `FilterExec` in the plan, so a conjunct applied wrongly would silently
//!   change the answer.
//!
//! The second group therefore runs each query twice — once with the nd pipeline
//! on, once with it off — and requires the same rows both ways. The pipeline-off
//! run is the oracle: there the predicate is a plain `FilterExec` over fully
//! broadcast columns.

mod common;

use arrow::record_batch::RecordBatch;
use common::{runtime_with, TestRuntime};

/// The WOD CTD fixture shipped with the NetCDF reader: 418 rows, every column
/// full-rank on one axis. Good for checking *what* the pipeline answers; it
/// cannot show footprint reduction, because every footprint is the whole grid.
/// [`gridded_fixture`] is the one with real coordinate axes.
fn netcdf_fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("beacon-file-formats/beacon-arrow-netcdf/test_files/wod_ctd_1964.nc")
}

/// A real gridded SST file: `lat` (1208) × `lon` (1920) × `time` (1) = 2,319,360
/// cells, with `lat` and `lon` on their own axes. A predicate over one axis has a
/// footprint of that axis alone; one over both spans the whole plane.
fn gridded_fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("beacon-file-formats/beacon-arrow-netcdf/test_files/gridded-example.nc")
}

/// A box over the north-east Atlantic — it holds part of the fixture, not all of
/// it, so a predicate that was dropped or inverted shows up as a row count.
const BOX_WKT: &str = "POLYGON((-13 40, 32 40, 32 60, -13 60, -13 40))";

async fn nd_runtime(tag: &str, nd_pipeline: bool) -> TestRuntime {
    let rt = runtime_with(tag, |b| if nd_pipeline { b.with_nd_pipeline() } else { b }).await;
    std::fs::copy(netcdf_fixture(), rt.datasets_dir().join("nd.nc")).expect("copy fixture");
    rt.sql("CREATE EXTERNAL TABLE nd STORED AS NC LOCATION 'nd.nc'")
        .await;
    rt
}

/// A runtime over [`gridded_fixture`], table `g`, nd pipeline on.
async fn gridded_runtime(tag: &str) -> TestRuntime {
    let rt = runtime_with(tag, |b| b.with_nd_pipeline()).await;
    std::fs::copy(gridded_fixture(), rt.datasets_dir().join("g.nc")).expect("copy fixture");
    rt.sql("CREATE EXTERNAL TABLE g STORED AS NC LOCATION 'g.nc'")
        .await;
    rt
}

/// The result rendered as text, so two runtimes' answers compare directly.
fn rendered(batches: &[RecordBatch]) -> String {
    arrow::util::pretty::pretty_format_batches(batches)
        .expect("format")
        .to_string()
}

/// The physical plan of `sql` as one string.
async fn plan(rt: &TestRuntime, sql: &str) -> String {
    rendered(&rt.sql(&format!("EXPLAIN {sql}")).await)
}

/// The plan of `sql` annotated with the metrics the run collected.
async fn analyzed(rt: &TestRuntime, sql: &str) -> String {
    rendered(&rt.sql(&format!("EXPLAIN ANALYZE {sql}")).await)
}

/// The value of `metric` on the plan line naming `node`, as printed — so `"0"`,
/// `"2.42 K"`, `"2.32 M"`. Compared as text, because the exact humanized figure
/// is what the reader of a plan sees.
fn metric(plan: &str, node: &str, metric: &str) -> String {
    let line = plan
        .lines()
        .find(|l| l.contains(node))
        .unwrap_or_else(|| panic!("no {node} in plan:\n{plan}"));
    let rest = line
        .split_once(&format!("{metric}="))
        .unwrap_or_else(|| panic!("no {metric} on the {node} line:\n{line}"))
        .1;
    rest.split(',')
        .next()
        .expect("a metric is comma-terminated")
        .trim()
        .to_string()
}

// ── the nd nodes refuse a projection ────────────────────────────────────────

/// An alias and a computed column stay in a `ProjectionExec` above the
/// broadcast; the file source below is only ever handed a plain column list.
///
/// This is the exemption #382 relies on. If a future change let a projection
/// through to `NetCdfSource`, the alias would reach a scan that resolves file
/// columns by name and the query would break — so assert on the plan, not just
/// the values.
#[tokio::test(flavor = "multi_thread")]
async fn an_alias_never_reaches_the_file_source() {
    let rt = nd_runtime("nd-alias-plan", false).await;

    let plan = plan(&rt, "SELECT lat AS easting, lat * 2 AS doubled FROM nd").await;
    assert!(
        plan.contains("ProjectionExec: expr=[lat@0 as easting"),
        "the projection must stay above the broadcast:\n{plan}"
    );
    assert!(
        plan.contains("projection=[lat], file_type=netcdf"),
        "the scan must get a plain column list:\n{plan}"
    );

    // And it answers: the alias names the column, the values are `lat`'s.
    let aliased = rt.sql("SELECT lat AS easting FROM nd LIMIT 3").await;
    let plain = rt.sql("SELECT lat FROM nd LIMIT 3").await;
    assert_eq!(aliased[0].schema().field(0).name(), "easting");
    assert_eq!(aliased[0].column(0), plain[0].column(0));
}

// ── a sunk filter answers what the plain filter answers ─────────────────────

/// Every query that the nd filter pushdown rewrites must answer exactly what it
/// answers without the rewrite.
#[tokio::test(flavor = "multi_thread")]
async fn a_sunk_filter_answers_what_the_plain_filter_answers() {
    let with_nd = nd_runtime("nd-filter-on", true).await;
    let without = nd_runtime("nd-filter-off", false).await;

    // Each query is ordered, so the two runtimes' rows line up positionally.
    let queries = [
        // A spatial predicate — a non-volatile scalar function over two columns.
        format!(
            "SELECT lat, lon FROM nd \
             WHERE st_within_point('{BOX_WKT}', CAST(lon AS DOUBLE), CAST(lat AS DOUBLE)) \
             ORDER BY lat, lon"
        ),
        // The same predicate conjoined with a plain one: two conjuncts whose
        // masks have to intersect on the target grid rather than replace it.
        format!(
            "SELECT lat, lon, z FROM nd \
             WHERE st_within_point('{BOX_WKT}', CAST(lon AS DOUBLE), CAST(lat AS DOUBLE)) \
               AND z > 100 \
             ORDER BY lat, lon, z"
        ),
        // A predicate over a computed value.
        "SELECT lat, z FROM nd WHERE lat * 2 > 90.0 ORDER BY lat, z".to_string(),
        // A disjunction inside one conjunct — it is not split, so the nd filter
        // has to union the branches rather than intersect them.
        "SELECT lat, lon FROM nd WHERE lat > 60 OR lon < -10 ORDER BY lat, lon".to_string(),
        // A projection with an alias on top of a sunk filter.
        "SELECT lat AS easting, z AS depth FROM nd WHERE lat > 0 AND z < 50 ORDER BY 1, 2"
            .to_string(),
        // A predicate combining two columns arithmetically.
        "SELECT lat, z FROM nd WHERE lat + z > 200 ORDER BY lat, z".to_string(),
    ];

    // The fixture's full row count, so each predicate can be shown to select a
    // strict subset — a filter that matched everything, or nothing, would agree
    // across the two runtimes while proving nothing.
    const TOTAL_ROWS: usize = 418;

    for sql in &queries {
        let with = with_nd.sql(sql).await;
        let plain = without.sql(sql).await;
        assert_eq!(
            rendered(&with),
            rendered(&plain),
            "the nd pipeline changed the answer to:\n{sql}"
        );

        let rows: usize = with.iter().map(|b| b.num_rows()).sum();
        assert!(
            rows > 0 && rows < TOTAL_ROWS,
            "this query should select a strict subset, got {rows} of {TOTAL_ROWS}:\n{sql}"
        );
    }
}

/// The comparison above is only worth something if the rewrite actually fires.
#[tokio::test(flavor = "multi_thread")]
async fn the_spatial_predicate_is_sunk_below_the_broadcast() {
    let rt = nd_runtime("nd-filter-fires", true).await;

    let sql = format!(
        "SELECT lat, lon FROM nd \
         WHERE st_within_point('{BOX_WKT}', CAST(lon AS DOUBLE), CAST(lat AS DOUBLE))"
    );
    let plan = plan(&rt, &sql).await;
    assert!(
        plan.contains("NdFilterExec: predicate=[st_within_point("),
        "the spatial predicate must sink below the broadcast:\n{plan}"
    );
    assert!(
        !plan.contains("FilterExec: st_within_point"),
        "a fully-sunk predicate leaves no residual filter:\n{plan}"
    );
}

/// A volatile function is not element-wise under broadcast — evaluating it on a
/// sub-grid would repeat one draw across a whole slice — so it must stay in a
/// `FilterExec` above the broadcast.
#[tokio::test(flavor = "multi_thread")]
async fn a_volatile_predicate_stays_above_the_broadcast() {
    let rt = nd_runtime("nd-filter-volatile", true).await;

    let plan = plan(&rt, "SELECT lat FROM nd WHERE random() < 0.5 AND lat > 0").await;
    assert!(
        plan.contains("NdFilterExec: predicate=[lat@0 > 0]"),
        "the deterministic conjunct still sinks:\n{plan}"
    );
    assert!(
        plan.contains("FilterExec: random()"),
        "the volatile conjunct must stay above the broadcast:\n{plan}"
    );
}

// ── what the footprint actually buys ────────────────────────────────────────

/// A predicate over one coordinate axis is evaluated over *that axis*, not the
/// grid it selects from.
///
/// This is the whole point of `NdFilterExec`: `lat > 40` is 2.42 K comparisons
/// on the lat axis, and the resulting mask is lifted to the 2.32 M-cell grid.
#[tokio::test(flavor = "multi_thread")]
async fn a_single_axis_predicate_is_evaluated_over_that_axis() {
    let rt = gridded_runtime("nd-footprint-axis").await;

    let plan = analyzed(&rt, "SELECT lat, analysed_sst FROM g WHERE lat > 40").await;
    assert_eq!(metric(&plan, "NdFilterExec", "input_rows"), "2.32 M");
    assert_eq!(
        metric(&plan, "NdFilterExec", "elements_evaluated"),
        "2.42 K",
        "the predicate runs on the lat axis, not on the grid:\n{plan}"
    );
    assert_eq!(metric(&plan, "NdFilterExec", "elements_saved"), "2.32 M");
}

/// A predicate over *two* axes has both in its footprint, so it is evaluated
/// once per cell of their plane — there is no reduction to a smaller axis.
///
/// This is the shape every spatial predicate takes: `lon` is on the lon axis and
/// `lat` on the lat axis, so `ST_Within(ST_Point(lon, lat), …)` spans lat × lon,
/// which on this single-time-step file *is* the whole grid. The saving on a file
/// with further axes is that the mask is computed once for the plane and reused
/// across them — not that the function runs fewer times per cell of the plane.
#[tokio::test(flavor = "multi_thread")]
async fn a_lon_lat_predicate_spans_the_whole_plane() {
    let rt = gridded_runtime("nd-footprint-plane").await;

    let plan = analyzed(
        &rt,
        "SELECT lat, lon FROM g \
         WHERE ST_Within(ST_Point(CAST(lon AS DOUBLE), CAST(lat AS DOUBLE)), \
                         ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'))",
    )
    .await;

    assert!(
        plan.contains("NdFilterExec: predicate=[st_within("),
        "the spatial predicate is evaluated before the broadcast:\n{plan}"
    );
    assert_eq!(
        metric(&plan, "NdFilterExec", "elements_evaluated"),
        "2.32 M",
        "lat ∪ lon is the whole plane here:\n{plan}"
    );
    assert_eq!(metric(&plan, "NdFilterExec", "elements_saved"), "0");
}

/// The geometry itself is built before the broadcast too: `ST_Point` becomes an
/// `NdProjectionExec` output column on the un-broadcast nd columns.
#[tokio::test(flavor = "multi_thread")]
async fn st_point_is_constructed_below_the_broadcast() {
    let rt = gridded_runtime("nd-footprint-point").await;

    let plan = plan(
        &rt,
        "SELECT ST_Point(CAST(lon AS DOUBLE), CAST(lat AS DOUBLE)) AS geom FROM g WHERE lat > 40",
    )
    .await;

    assert!(
        plan.contains("NdProjectionExec: exprs=[geom]"),
        "the point is constructed below the broadcast:\n{plan}"
    );
    assert!(
        !plan.contains("ProjectionExec: expr=[st_point("),
        "…and not left above it:\n{plan}"
    );
}

/// Whether a predicate sinks depends on the *select list*, not the predicate.
///
/// Narrowing the output to fewer columns than the predicate reads makes
/// DataFusion fold the narrowing into the filter (`FilterExec: …,
/// projection=[…]`), and `NdFilterPushdown` declines any filter carrying one. So
/// the most natural spatial query — project a couple of columns, filter on
/// lon/lat — is exactly the one that still filters *after* the broadcast.
#[tokio::test(flavor = "multi_thread")]
async fn a_narrowing_select_list_keeps_the_filter_above_the_broadcast() {
    let rt = gridded_runtime("nd-footprint-narrow").await;

    const WITHIN: &str = "ST_Within(ST_Point(CAST(lon AS DOUBLE), CAST(lat AS DOUBLE)), \
                          ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'))";

    // Both predicate columns projected: the filter sinks.
    let both = plan(&rt, &format!("SELECT lat, lon FROM g WHERE {WITHIN}")).await;
    assert!(
        both.contains("NdFilterExec: predicate=[st_within("),
        "{both}"
    );

    // One of them projected: the filter carries the narrowing and stays put.
    let one = plan(&rt, &format!("SELECT lat FROM g WHERE {WITHIN}")).await;
    assert!(
        one.contains("FilterExec: st_within(") && one.contains("projection=[lat@0]"),
        "the narrowing is folded into the filter:\n{one}"
    );
    assert!(
        !one.contains("NdFilterExec"),
        "so nothing sinks below the broadcast:\n{one}"
    );
}
