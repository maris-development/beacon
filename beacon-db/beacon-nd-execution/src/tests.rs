use std::sync::Arc;

use arrow::array::{AsArray, Float64Array, Int32Array};
use arrow::compute::concat_batches;
use arrow::datatypes::{Float64Type, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;

use beacon_nd_encoding::*;
use beacon_nd_encoding::encoding::*;

use crate::exec::{NdBroadcastExec, NdSourceExec};
use crate::optimizer::*;

fn dims(spec: &[(&str, usize)]) -> Dimensions {
    Dimensions::try_new(
        spec.iter()
            .map(|(name, size)| Dimension::new(*name, *size))
            .collect(),
    )
    .unwrap()
}

/// A (time=4, lat=3, lon=2) grid with coordinate variables and one data
/// variable, encoded into two nd-encoded `RecordBatch`es (split along
/// `time`) and served from an in-memory `DataSourceExec` — the shape a file
/// opener produces.
fn test_source() -> Arc<NdSourceExec> {
    let encoded = encoded_chunks();
    let encoded_schema = encoded[0].schema();
    // One partition, each nd chunk its own single-row encoded batch.
    let source = MemorySourceConfig::try_new_exec(&[encoded], encoded_schema, None).unwrap();
    Arc::new(NdSourceExec::try_new(source).unwrap())
}

/// The grid of [`test_source`] with each chunk in its own partition.
fn split_source() -> Arc<NdSourceExec> {
    let encoded = encoded_chunks();
    let encoded_schema = encoded[0].schema();
    let partitions: Vec<_> = encoded.into_iter().map(|chunk| vec![chunk]).collect();
    let source = MemorySourceConfig::try_new_exec(&partitions, encoded_schema, None).unwrap();
    Arc::new(NdSourceExec::try_new(source).unwrap())
}

/// The two encoded chunks of the test grid, split along `time`.
fn encoded_chunks() -> Vec<RecordBatch> {
    let logical = Arc::new(Schema::new(vec![
        arrow::datatypes::Field::new("time", arrow::datatypes::DataType::Int32, true),
        arrow::datatypes::Field::new("lat", arrow::datatypes::DataType::Int32, true),
        arrow::datatypes::Field::new("lon", arrow::datatypes::DataType::Int32, true),
        arrow::datatypes::Field::new("sst", arrow::datatypes::DataType::Float64, true),
    ]));

    let lat = || {
        NdArrowArray::try_new(
            Arc::new(Int32Array::from(vec![-30, 0, 30])),
            dims(&[("lat", 3)]),
        )
        .unwrap()
    };
    let lon = || {
        NdArrowArray::try_new(Arc::new(Int32Array::from(vec![5, 15])), dims(&[("lon", 2)]))
            .unwrap()
    };

    let mut encoded = Vec::new();
    for chunk in 0..2u32 {
        let t0 = chunk * 2;
        let time = NdArrowArray::try_new(
            Arc::new(Int32Array::from(vec![100 + t0 as i32, 101 + t0 as i32])),
            dims(&[("time", 2)]),
        )
        .unwrap();
        let sst_values: Vec<f64> = (0..12).map(|i| (t0 * 6 + i) as f64).collect();
        let sst = NdArrowArray::try_new(
            Arc::new(Float64Array::from(sst_values)),
            dims(&[("time", 2), ("lat", 3), ("lon", 2)]),
        )
        .unwrap();
        let nd = NdRecordBatch::try_new(
            logical.clone(),
            vec![time, lat(), lon(), sst],
            dims(&[("time", 2), ("lat", 3), ("lon", 2)]),
        )
        .unwrap();
        encoded.push(encode_nd_record_batch(&nd).unwrap());
    }
    encoded
}

async fn run(plan: Arc<dyn ExecutionPlan>) -> Result<RecordBatch> {
    let schema = plan.schema();
    let batches: Vec<_> = plan
        .execute(0, Arc::new(TaskContext::default()))?
        .try_collect()
        .await?;
    Ok(concat_batches(&schema, &batches)?)
}

#[tokio::test]
async fn source_alone_materializes_full_grid() {
    // NdSourceExec's own `execute` materializes (no broadcast node needed).
    let batch = run(test_source()).await.unwrap();
    assert_eq!(batch.num_rows(), 24);
    assert_eq!(batch.column(0).as_primitive::<Int32Type>().value(0), 100);
    assert_eq!(batch.column(1).as_primitive::<Int32Type>().value(0), -30);
    assert_eq!(batch.column(2).as_primitive::<Int32Type>().value(0), 5);
}

#[tokio::test]
async fn source_then_broadcast_materializes_full_grid() {
    let plan = Arc::new(NdBroadcastExec::try_new(test_source()).unwrap());
    let batch = run(plan).await.unwrap();

    // 4 time x 3 lat x 2 lon = 24 rows, C-order (time outer, lon inner).
    assert_eq!(batch.num_rows(), 24);

    // time repeats over the 6 (lat,lon) cells of each step.
    assert_eq!(
        batch.column(0).as_primitive::<Int32Type>().values(),
        &[
            100, 100, 100, 100, 100, 100, 101, 101, 101, 101, 101, 101, 102, 102, 102, 102,
            102, 102, 103, 103, 103, 103, 103, 103
        ]
    );
    // lat repeats over lon, tiles over time.
    assert_eq!(
        &batch.column(1).as_primitive::<Int32Type>().values()[..6],
        &[-30, -30, 0, 0, 30, 30]
    );
    // lon tiles over everything.
    assert_eq!(
        &batch.column(2).as_primitive::<Int32Type>().values()[..6],
        &[5, 15, 5, 15, 5, 15]
    );
    // sst is full-rank: passes through unbroadcast, values 0..24.
    assert_eq!(
        batch.column(3).as_primitive::<Float64Type>().values(),
        &(0..24).map(|v| v as f64).collect::<Vec<_>>()[..]
    );
}

#[tokio::test]
async fn nodes_report_metrics() {
    let source = test_source();
    let broadcast = Arc::new(NdBroadcastExec::try_new(source.clone()).unwrap());

    // Drain the plan so the streams run to completion and finalize metrics.
    let out = run(broadcast.clone() as Arc<dyn ExecutionPlan>).await.unwrap();
    assert_eq!(out.num_rows(), 24);

    // Broadcast reports the flattened output rows.
    let broadcast_metrics = broadcast.metrics().unwrap();
    assert_eq!(broadcast_metrics.output_rows(), Some(24));

    // Each chunk broadcasts time/lat/lon (3) and passes sst through (1);
    // two chunks → 6 implicit broadcasts, 2 pass-throughs.
    assert_eq!(
        broadcast_metrics
            .sum_by_name("implicit_broadcasts")
            .map(|v| v.as_usize()),
        Some(6)
    );
    assert_eq!(
        broadcast_metrics
            .sum_by_name("passthrough_columns")
            .map(|v| v.as_usize()),
        Some(2)
    );

    // Source reports the grid rows it decoded (12 + 12) and the number of
    // nd batches.
    let source_metrics = source.metrics().unwrap();
    assert_eq!(source_metrics.output_rows(), Some(24));
    assert_eq!(
        source_metrics
            .sum_by_name("nd_batches")
            .map(|v| v.as_usize()),
        Some(2)
    );
}

#[tokio::test]
async fn broadcast_requires_nd_input() {
    // A non-nd child is rejected at construction.
    let broadcast = Arc::new(NdBroadcastExec::try_new(test_source()).unwrap());
    // Wrapping a broadcast (which is not nd-aware) must fail.
    assert!(NdBroadcastExec::try_new(broadcast).is_err());
}

// ── projection pushdown ──────────────────────────────────────────────

use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{binary, col, lit};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};

use crate::exec::NdProjectionExec;

/// A mix of projection expressions with different footprints:
/// `lat*2` (footprint {lat}), `lon+1` ({lon}), `sst` passthrough
/// ({time,lat,lon}), and a constant ({}).
fn projection_exprs(schema: &arrow::datatypes::SchemaRef) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
    vec![
        (
            binary(col("lat", schema).unwrap(), Operator::Multiply, lit(2i32), schema).unwrap(),
            "lat2".to_string(),
        ),
        (
            binary(col("lon", schema).unwrap(), Operator::Plus, lit(1i32), schema).unwrap(),
            "lon1".to_string(),
        ),
        (col("sst", schema).unwrap(), "sst".to_string()),
        (lit(7i32), "seven".to_string()),
    ]
}

/// Evaluating a projection *before* broadcast (on footprint sub-grids) yields
/// byte-identical output to evaluating it *after* broadcasting the full grid.
#[tokio::test]
async fn projection_before_broadcast_matches_after() {
    let schema = test_source().schema();
    let exprs = projection_exprs(&schema);

    // Reference: broadcast the full grid, then project (plain ProjectionExec).
    let reference = Arc::new(
        ProjectionExec::try_new(
            exprs
                .iter()
                .cloned()
                .map(|(expr, alias)| ProjectionExpr { expr, alias }),
            Arc::new(NdBroadcastExec::try_new(test_source()).unwrap()),
        )
        .unwrap(),
    );
    let expected = run(reference).await.unwrap();

    // Optimized: project on footprints first, then broadcast.
    let nd_proj = Arc::new(NdProjectionExec::try_new(test_source(), exprs).unwrap());
    let optimized = Arc::new(NdBroadcastExec::try_new(nd_proj).unwrap());
    let actual = run(optimized).await.unwrap();

    assert_eq!(actual.num_rows(), 24);
    assert_eq!(actual, expected);
}

/// An expression combining two columns on *different* axes (`lat + lon`,
/// dims {lat} and {lon}) co-broadcasts both onto their union footprint
/// {lat, lon} before evaluating — matching a post-broadcast projection.
#[tokio::test]
async fn projection_combines_columns_of_different_dims() {
    let schema = test_source().schema();
    // lat{lat} + lon{lon}  → footprint {lat, lon}, evaluated over 3·2 = 6
    // cells, then broadcast across time to the full 24-row grid.
    let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![(
        binary(
            col("lat", &schema).unwrap(),
            Operator::Plus,
            col("lon", &schema).unwrap(),
            &schema,
        )
        .unwrap(),
        "lat_plus_lon".to_string(),
    )];

    let reference = Arc::new(
        ProjectionExec::try_new(
            exprs
                .iter()
                .cloned()
                .map(|(expr, alias)| ProjectionExpr { expr, alias }),
            Arc::new(NdBroadcastExec::try_new(test_source()).unwrap()),
        )
        .unwrap(),
    );
    let expected = run(reference).await.unwrap();

    let nd_proj = Arc::new(NdProjectionExec::try_new(test_source(), exprs).unwrap());
    let optimized = Arc::new(NdBroadcastExec::try_new(nd_proj).unwrap());
    let actual = run(optimized).await.unwrap();

    assert_eq!(actual.num_rows(), 24);
    assert_eq!(actual, expected);
    // Spot-check the outer-product semantics: first (lat,lon) cell is
    // lat[-30] + lon[5] = -25.
    assert_eq!(actual.column(0).as_primitive::<Int32Type>().value(0), -25);
}

/// Single-column expressions take the fast path: the projection does no
/// co-broadcast (footprint == the column's own dims), leaving the single
/// gather to the terminal broadcast. `lat * 2` (single column) and `sst`
/// (bare passthrough) both report zero implicit broadcasts, with unchanged
/// results.
#[tokio::test]
async fn single_column_projection_skips_broadcast() {
    let schema = test_source().schema();
    let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
        (
            binary(col("lat", &schema).unwrap(), Operator::Multiply, lit(2i32), &schema)
                .unwrap(),
            "lat2".to_string(),
        ),
        (col("sst", &schema).unwrap(), "sst".to_string()),
    ];

    let reference = Arc::new(
        ProjectionExec::try_new(
            exprs
                .iter()
                .cloned()
                .map(|(expr, alias)| ProjectionExpr { expr, alias }),
            Arc::new(NdBroadcastExec::try_new(test_source()).unwrap()),
        )
        .unwrap(),
    );
    let expected = run(reference).await.unwrap();

    let projection = Arc::new(NdProjectionExec::try_new(test_source(), exprs).unwrap());
    let broadcast = Arc::new(NdBroadcastExec::try_new(projection.clone()).unwrap());
    let actual = run(broadcast).await.unwrap();

    assert_eq!(actual, expected);
    // The projection performed no gather; only the terminal broadcast did.
    assert_eq!(
        projection
            .metrics()
            .unwrap()
            .sum_by_name("implicit_broadcasts")
            .map(|v| v.as_usize()),
        Some(0)
    );
}

/// NdProjectionExec reports the work it did and saved: elements evaluated on
/// footprints, elements avoided versus the full grid, and implicit
/// co-broadcasts.
#[tokio::test]
async fn projection_reports_metrics() {
    let schema = test_source().schema();
    // lat{lat} + lon{lon} → footprint {lat, lon}; both inputs co-broadcast.
    let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![(
        binary(
            col("lat", &schema).unwrap(),
            Operator::Plus,
            col("lon", &schema).unwrap(),
            &schema,
        )
        .unwrap(),
        "lat_plus_lon".to_string(),
    )];

    let projection = Arc::new(NdProjectionExec::try_new(test_source(), exprs).unwrap());
    let broadcast = Arc::new(NdBroadcastExec::try_new(projection.clone()).unwrap());
    let out = run(broadcast).await.unwrap();
    assert_eq!(out.num_rows(), 24);

    let m = projection.metrics().unwrap();
    let count = |name: &str| m.sum_by_name(name).map(|v| v.as_usize());
    // Per chunk: full grid = 2·3·2 = 12, footprint {lat=3, lon=2} = 6; two
    // chunks. Evaluated 6·2 = 12, saved (12−6)·2 = 12.
    assert_eq!(count("elements_evaluated"), Some(12));
    assert_eq!(count("elements_saved"), Some(12));
    // Both referenced columns gather onto the footprint: 2 per chunk · 2 = 4.
    assert_eq!(count("implicit_broadcasts"), Some(4));
}

/// The rule rewrites `ProjectionExec → NdBroadcastExec` into
/// `NdBroadcastExec → NdProjectionExec`, preserving schema and results.
#[tokio::test]
async fn pushdown_rule_sinks_projection_below_broadcast() {
    use datafusion::common::config::ConfigOptions;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::displayable;

    let schema = test_source().schema();
    let exprs = projection_exprs(&schema);

    let original: Arc<dyn ExecutionPlan> = Arc::new(
        ProjectionExec::try_new(
            exprs
                .iter()
                .cloned()
                .map(|(expr, alias)| ProjectionExpr { expr, alias }),
            Arc::new(NdBroadcastExec::try_new(test_source()).unwrap()),
        )
        .unwrap(),
    );
    let original_schema = original.schema();
    let expected = run(original.clone()).await.unwrap();

    let optimized = NdProjectionPushdown::new()
        .optimize(original, &ConfigOptions::default())
        .unwrap();

    // Schema is preserved (the rule reports schema_check = true).
    assert_eq!(optimized.schema(), original_schema);

    // The projection now sits *below* the broadcast.
    let rendered = displayable(optimized.as_ref()).indent(true).to_string();
    let broadcast = rendered.find("NdBroadcastExec");
    let projection = rendered.find("NdProjectionExec");
    let source = rendered.find("NdSourceExec");
    assert!(
        broadcast < projection && projection < source,
        "expected NdBroadcastExec → NdProjectionExec → NdSourceExec:\n{rendered}"
    );

    // Results are unchanged by the rewrite.
    let actual = run(optimized).await.unwrap();
    assert_eq!(actual, expected);
}

/// The rule leaves a projection in place when any expression is not
/// element-wise (here a volatile scalar function): no `NdProjectionExec`.
#[tokio::test]
async fn pushdown_rule_skips_non_elementwise() {
    use datafusion::common::config::ConfigOptions;
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
    use datafusion::physical_expr::ScalarFunctionExpr;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::displayable;
    use datafusion::scalar::ScalarValue;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct VolatileUdf {
        signature: Signature,
    }
    impl ScalarUDFImpl for VolatileUdf {
        fn name(&self) -> &str {
            "test_volatile"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[arrow::datatypes::DataType]) -> Result<arrow::datatypes::DataType> {
            Ok(arrow::datatypes::DataType::Float64)
        }
        fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))))
        }
    }

    let schema = test_source().schema();
    let udf = Arc::new(ScalarUDF::new_from_impl(VolatileUdf {
        signature: Signature::exact(vec![], Volatility::Volatile),
    }));
    let volatile: Arc<dyn PhysicalExpr> = Arc::new(
        ScalarFunctionExpr::try_new(udf, vec![], &schema, Arc::new(ConfigOptions::default()))
            .unwrap(),
    );

    let original: Arc<dyn ExecutionPlan> = Arc::new(
        ProjectionExec::try_new(
            [ProjectionExpr {
                expr: volatile,
                alias: "r".to_string(),
            }],
            Arc::new(NdBroadcastExec::try_new(test_source()).unwrap()),
        )
        .unwrap(),
    );

    let optimized = NdProjectionPushdown::new()
        .optimize(original, &ConfigOptions::default())
        .unwrap();
    let rendered = displayable(optimized.as_ref()).indent(true).to_string();
    assert!(
        !rendered.contains("NdProjectionExec"),
        "volatile projection must not be pushed below the broadcast:\n{rendered}"
    );
}

// ── filter pushdown ──────────────────────────────────────────────────

use datafusion::physical_plan::filter::FilterExec;

use crate::exec::NdFilterExec;

/// Filtering *before* broadcast (as a grid selection applied by the broadcast)
/// yields byte-identical output to filtering *after* broadcasting the full
/// grid — for coordinate-axis, cross-axis, and data-variable predicates.
#[tokio::test]
async fn filter_before_broadcast_matches_after() {
    let schema = test_source().schema();
    let predicates: Vec<Arc<dyn PhysicalExpr>> = vec![
        // Coordinate axis: keeps lat ∈ {0, 30}.
        binary(col("lat", &schema).unwrap(), Operator::Gt, lit(-1i32), &schema).unwrap(),
        // Single-axis range as one ANDed conjunct: time ∈ {101, 102}.
        binary(
            binary(col("time", &schema).unwrap(), Operator::GtEq, lit(101i32), &schema)
                .unwrap(),
            Operator::And,
            binary(col("time", &schema).unwrap(), Operator::LtEq, lit(102i32), &schema)
                .unwrap(),
            &schema,
        )
        .unwrap(),
        // Inequality on a coordinate axis: drops the time=101 slices.
        binary(col("time", &schema).unwrap(), Operator::NotEq, lit(101i32), &schema).unwrap(),
        // Cross-axis: footprint {lat, lon}.
        binary(
            binary(
                col("lat", &schema).unwrap(),
                Operator::Plus,
                col("lon", &schema).unwrap(),
                &schema,
            )
            .unwrap(),
            Operator::Gt,
            lit(10i32),
            &schema,
        )
        .unwrap(),
        // Disjunction across two axes: footprint {lat, lon}.
        binary(
            binary(col("lat", &schema).unwrap(), Operator::Lt, lit(0i32), &schema).unwrap(),
            Operator::Or,
            binary(col("lon", &schema).unwrap(), Operator::Eq, lit(15i32), &schema).unwrap(),
            &schema,
        )
        .unwrap(),
        // Full-rank data variable: footprint {time, lat, lon}.
        binary(col("sst", &schema).unwrap(), Operator::Gt, lit(5.0f64), &schema).unwrap(),
        // Coordinate axis combined with a data variable in one conjunct.
        binary(
            binary(col("lat", &schema).unwrap(), Operator::Gt, lit(-30i32), &schema).unwrap(),
            Operator::And,
            binary(col("sst", &schema).unwrap(), Operator::Lt, lit(20.0f64), &schema).unwrap(),
            &schema,
        )
        .unwrap(),
    ];

    for predicate in predicates {
        // Reference: broadcast the full grid, then filter (plain FilterExec).
        let reference = Arc::new(
            FilterExec::try_new(
                predicate.clone(),
                Arc::new(NdBroadcastExec::try_new(test_source()).unwrap()),
            )
            .unwrap(),
        );
        let expected = run(reference).await.unwrap();

        // Optimized: filter on footprints first (as a selection), then broadcast.
        let nd_filter =
            Arc::new(NdFilterExec::try_new(test_source(), vec![predicate.clone()]).unwrap());
        let optimized = Arc::new(NdBroadcastExec::try_new(nd_filter).unwrap());
        let actual = run(optimized).await.unwrap();

        assert_eq!(actual, expected, "mismatch for predicate {predicate}");
    }
}

/// Several *separate* conjuncts across different axes handed to one
/// `NdFilterExec` intersect correctly, matching a single `FilterExec` whose
/// predicate is their `AND`.
#[tokio::test]
async fn multiple_conjuncts_across_axes_match_reference() {
    let schema = test_source().schema();
    let c1 = binary(col("time", &schema).unwrap(), Operator::Gt, lit(100i32), &schema).unwrap();
    let c2 = binary(col("lat", &schema).unwrap(), Operator::GtEq, lit(0i32), &schema).unwrap();
    let c3 = binary(col("lon", &schema).unwrap(), Operator::Eq, lit(5i32), &schema).unwrap();

    // Reference: one FilterExec over `c1 AND c2 AND c3`.
    let combined: Arc<dyn PhysicalExpr> = binary(
        binary(c1.clone(), Operator::And, c2.clone(), &schema).unwrap(),
        Operator::And,
        c3.clone(),
        &schema,
    )
    .unwrap();
    let reference = Arc::new(
        FilterExec::try_new(
            combined,
            Arc::new(NdBroadcastExec::try_new(test_source()).unwrap()),
        )
        .unwrap(),
    );
    let expected = run(reference).await.unwrap();

    // Optimized: the three conjuncts as separate selections.
    let nd_filter =
        Arc::new(NdFilterExec::try_new(test_source(), vec![c1, c2, c3]).unwrap());
    let optimized = Arc::new(NdBroadcastExec::try_new(nd_filter).unwrap());
    let actual = run(optimized).await.unwrap();

    assert_eq!(actual, expected);
}

/// A predicate no cell satisfies yields an empty result — the broadcast emits
/// nothing, matching a `FilterExec` that drops every row.
#[tokio::test]
async fn filter_selecting_no_rows_is_empty() {
    let schema = test_source().schema();
    let predicate: Arc<dyn PhysicalExpr> =
        binary(col("time", &schema).unwrap(), Operator::Gt, lit(1000i32), &schema).unwrap();

    let reference = Arc::new(
        FilterExec::try_new(
            predicate.clone(),
            Arc::new(NdBroadcastExec::try_new(test_source()).unwrap()),
        )
        .unwrap(),
    );
    let expected = run(reference).await.unwrap();

    let nd_filter = Arc::new(NdFilterExec::try_new(test_source(), vec![predicate]).unwrap());
    let optimized = Arc::new(NdBroadcastExec::try_new(nd_filter).unwrap());
    let actual = run(optimized).await.unwrap();

    assert_eq!(actual.num_rows(), 0);
    assert_eq!(actual, expected);
}

/// A predicate every cell satisfies retains the whole grid, byte-identical to
/// the unfiltered broadcast.
#[tokio::test]
async fn filter_selecting_all_rows_matches_unfiltered() {
    let schema = test_source().schema();
    // time is always ≥ 100, so `time > 0` keeps every cell.
    let predicate: Arc<dyn PhysicalExpr> =
        binary(col("time", &schema).unwrap(), Operator::Gt, lit(0i32), &schema).unwrap();

    let unfiltered =
        run(Arc::new(NdBroadcastExec::try_new(test_source()).unwrap())).await.unwrap();

    let nd_filter = Arc::new(NdFilterExec::try_new(test_source(), vec![predicate]).unwrap());
    let optimized = Arc::new(NdBroadcastExec::try_new(nd_filter).unwrap());
    let actual = run(optimized).await.unwrap();

    assert_eq!(actual.num_rows(), 24);
    assert_eq!(actual, unfiltered);
}

/// A filter below a projection: the selection accumulated by the nd filter is
/// carried through the nd projection and applied by the terminal broadcast,
/// matching `Projection(Filter(full grid))`.
#[tokio::test]
async fn filter_then_projection_matches_reference() {
    let schema = test_source().schema();
    let predicate: Arc<dyn PhysicalExpr> =
        binary(col("lat", &schema).unwrap(), Operator::Gt, lit(-1i32), &schema).unwrap();
    let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
        (
            binary(col("lat", &schema).unwrap(), Operator::Multiply, lit(2i32), &schema)
                .unwrap(),
            "lat2".to_string(),
        ),
        (col("sst", &schema).unwrap(), "sst".to_string()),
    ];

    // Reference: project over filter over the broadcast full grid.
    let reference = Arc::new(
        ProjectionExec::try_new(
            exprs
                .iter()
                .cloned()
                .map(|(expr, alias)| ProjectionExpr { expr, alias }),
            Arc::new(
                FilterExec::try_new(
                    predicate.clone(),
                    Arc::new(NdBroadcastExec::try_new(test_source()).unwrap()),
                )
                .unwrap(),
            ),
        )
        .unwrap(),
    );
    let expected = run(reference).await.unwrap();

    // Optimized: nd filter → nd projection → broadcast.
    let nd_filter = Arc::new(NdFilterExec::try_new(test_source(), vec![predicate]).unwrap());
    let nd_proj = Arc::new(NdProjectionExec::try_new(nd_filter, exprs).unwrap());
    let optimized = Arc::new(NdBroadcastExec::try_new(nd_proj).unwrap());
    let actual = run(optimized).await.unwrap();

    assert_eq!(actual, expected);
}

/// The rule rewrites `FilterExec → NdBroadcastExec` into
/// `NdBroadcastExec → NdFilterExec`, dropping the filter when every conjunct
/// is element-wise, and preserving schema and results.
#[tokio::test]
async fn pushdown_rule_sinks_filter_below_broadcast() {
    use datafusion::common::config::ConfigOptions;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::displayable;

    let schema = test_source().schema();
    // `lat > -1 AND lon = 5`: two element-wise conjuncts.
    let predicate: Arc<dyn PhysicalExpr> = binary(
        binary(col("lat", &schema).unwrap(), Operator::Gt, lit(-1i32), &schema).unwrap(),
        Operator::And,
        binary(col("lon", &schema).unwrap(), Operator::Eq, lit(5i32), &schema).unwrap(),
        &schema,
    )
    .unwrap();

    let original: Arc<dyn ExecutionPlan> = Arc::new(
        FilterExec::try_new(
            predicate,
            Arc::new(NdBroadcastExec::try_new(test_source()).unwrap()),
        )
        .unwrap(),
    );
    let original_schema = original.schema();
    let expected = run(original.clone()).await.unwrap();

    let optimized = NdFilterPushdown::new()
        .optimize(original, &ConfigOptions::default())
        .unwrap();

    assert_eq!(optimized.schema(), original_schema);

    // Every conjunct is pushable, so no FilterExec remains and the nd filter
    // sits below the broadcast, above the source.
    let rendered = displayable(optimized.as_ref()).indent(true).to_string();
    assert!(
        !rendered
            .lines()
            .any(|l| l.trim_start().starts_with("FilterExec:")),
        "all-pushable filter should leave no residual FilterExec:\n{rendered}"
    );
    let broadcast = rendered.find("NdBroadcastExec");
    let filter = rendered.find("NdFilterExec");
    let source = rendered.find("NdSourceExec");
    assert!(
        broadcast < filter && filter < source,
        "expected NdBroadcastExec → NdFilterExec → NdSourceExec:\n{rendered}"
    );

    let actual = run(optimized).await.unwrap();
    assert_eq!(actual, expected);
}

/// A non-element-wise conjunct (a volatile function) stays in a residual
/// `FilterExec` above the broadcast, while the element-wise conjunct sinks
/// into an `NdFilterExec` below it.
#[tokio::test]
async fn pushdown_rule_splits_mixed_predicate() {
    use datafusion::common::config::ConfigOptions;
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
    use datafusion::physical_expr::ScalarFunctionExpr;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::displayable;
    use datafusion::scalar::ScalarValue;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct VolatilePred {
        signature: Signature,
    }
    impl ScalarUDFImpl for VolatilePred {
        fn name(&self) -> &str {
            "test_volatile_pred"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[arrow::datatypes::DataType]) -> Result<arrow::datatypes::DataType> {
            Ok(arrow::datatypes::DataType::Boolean)
        }
        fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
        }
    }

    let schema = test_source().schema();
    let udf = Arc::new(ScalarUDF::new_from_impl(VolatilePred {
        signature: Signature::exact(vec![], Volatility::Volatile),
    }));
    let volatile: Arc<dyn PhysicalExpr> = Arc::new(
        ScalarFunctionExpr::try_new(udf, vec![], &schema, Arc::new(ConfigOptions::default()))
            .unwrap(),
    );
    // `lat > -1 AND volatile()`: one pushable, one not.
    let predicate: Arc<dyn PhysicalExpr> = binary(
        binary(col("lat", &schema).unwrap(), Operator::Gt, lit(-1i32), &schema).unwrap(),
        Operator::And,
        volatile,
        &schema,
    )
    .unwrap();

    let original: Arc<dyn ExecutionPlan> = Arc::new(
        FilterExec::try_new(
            predicate,
            Arc::new(NdBroadcastExec::try_new(test_source()).unwrap()),
        )
        .unwrap(),
    );

    let optimized = NdFilterPushdown::new()
        .optimize(original, &ConfigOptions::default())
        .unwrap();
    let rendered = displayable(optimized.as_ref()).indent(true).to_string();

    // Residual FilterExec on top, nd filter below the broadcast. Match the
    // residual by a line starting with `FilterExec:` (not `NdFilterExec:`).
    let line_of = |needle: &str| {
        rendered
            .lines()
            .position(|l| l.trim_start().starts_with(needle))
    };
    let residual = line_of("FilterExec:");
    let broadcast = line_of("NdBroadcastExec");
    let nd_filter = line_of("NdFilterExec:");
    assert!(
        residual.is_some() && residual < broadcast && broadcast < nd_filter,
        "expected FilterExec → NdBroadcastExec → NdFilterExec:\n{rendered}"
    );
}

/// Apply the structural rule and render the plan.
fn sink_structural(plan: Arc<dyn ExecutionPlan>) -> (Arc<dyn ExecutionPlan>, String) {
    use datafusion::common::config::ConfigOptions;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::displayable;

    let schema = plan.schema();
    let optimized = NdStructuralPushdown::new()
        .optimize(plan, &ConfigOptions::default())
        .unwrap();
    assert_eq!(optimized.schema(), schema);
    let rendered = displayable(optimized.as_ref()).indent(true).to_string();
    (optimized, rendered)
}

/// All rows of all partitions, as sorted `(time, lat, lon, sst)` tuples.
async fn sorted_rows(plan: Arc<dyn ExecutionPlan>) -> Vec<(i32, i32, i32, i64)> {
    use datafusion::physical_plan::ExecutionPlanProperties;

    let mut rows = Vec::new();
    for partition in 0..plan.output_partitioning().partition_count() {
        let batches: Vec<RecordBatch> = plan
            .execute(partition, Arc::new(TaskContext::default()))
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        for batch in batches {
            let time = batch.column(0).as_primitive::<Int32Type>();
            let lat = batch.column(1).as_primitive::<Int32Type>();
            let lon = batch.column(2).as_primitive::<Int32Type>();
            let sst = batch.column(3).as_primitive::<Float64Type>();
            for row in 0..batch.num_rows() {
                rows.push((
                    time.value(row),
                    lat.value(row),
                    lon.value(row),
                    sst.value(row) as i64,
                ));
            }
        }
    }
    rows.sort();
    rows
}

fn broadcast(source: Arc<NdSourceExec>) -> Arc<dyn ExecutionPlan> {
    Arc::new(NdBroadcastExec::try_new(source).unwrap())
}

#[tokio::test]
async fn structural_rule_sinks_local_limit() {
    use datafusion::physical_plan::limit::LocalLimitExec;

    let original: Arc<dyn ExecutionPlan> =
        Arc::new(LocalLimitExec::new(broadcast(test_source()), 5));
    let expected = run(original.clone()).await.unwrap();

    let (optimized, rendered) = sink_structural(original);
    assert!(!rendered.contains("LocalLimitExec"), "{rendered}");
    assert!(
        rendered.find("NdBroadcastExec") < rendered.find("NdLimitExec: skip=0, fetch=5"),
        "{rendered}"
    );
    assert_eq!(run(optimized).await.unwrap(), expected);
}

/// The cut crosses the border between the two chunks.
#[tokio::test]
async fn structural_rule_sinks_global_limit_with_skip() {
    use datafusion::physical_plan::limit::GlobalLimitExec;

    let original: Arc<dyn ExecutionPlan> =
        Arc::new(GlobalLimitExec::new(broadcast(test_source()), 7, Some(8)));
    let expected = run(original.clone()).await.unwrap();
    assert_eq!(expected.num_rows(), 8);

    let (optimized, rendered) = sink_structural(original);
    assert!(!rendered.contains("GlobalLimitExec"), "{rendered}");
    assert!(rendered.contains("NdLimitExec: skip=7, fetch=8"), "{rendered}");
    assert_eq!(run(optimized).await.unwrap(), expected);
}

/// A global limit counts over all rows, so it stays over two partitions.
#[tokio::test]
async fn structural_rule_keeps_global_limit_over_partitions() {
    use datafusion::physical_plan::limit::GlobalLimitExec;

    let original: Arc<dyn ExecutionPlan> =
        Arc::new(GlobalLimitExec::new(broadcast(split_source()), 0, Some(3)));
    let (_, rendered) = sink_structural(original);
    assert!(rendered.contains("GlobalLimitExec"), "{rendered}");
    assert!(!rendered.contains("NdLimitExec"), "{rendered}");
}

/// A merge without a fetch stays flat, so each partition keeps its own
/// broadcast task.
#[tokio::test]
async fn structural_rule_keeps_coalesce_without_fetch() {
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;

    let original: Arc<dyn ExecutionPlan> =
        Arc::new(CoalescePartitionsExec::new(broadcast(split_source())));
    let (_, rendered) = sink_structural(original);
    assert!(rendered.starts_with("CoalescePartitionsExec"), "{rendered}");
    assert!(!rendered.contains("NdCoalescePartitionsExec"), "{rendered}");
}

/// The nd merge gives the rows of all partitions.
#[tokio::test]
async fn nd_coalesce_matches_flat_coalesce() {
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;

    use crate::exec::NdCoalescePartitionsExec;

    let flat: Arc<dyn ExecutionPlan> =
        Arc::new(CoalescePartitionsExec::new(broadcast(split_source())));
    let expected = sorted_rows(flat).await;
    assert_eq!(expected.len(), 24);

    let merged = Arc::new(NdCoalescePartitionsExec::try_new(split_source()).unwrap());
    let nd: Arc<dyn ExecutionPlan> = Arc::new(NdBroadcastExec::try_new(merged).unwrap());
    assert_eq!(sorted_rows(nd).await, expected);
}

/// The fetch of the merge becomes a limit above the nd merge.
#[tokio::test]
async fn structural_rule_sinks_coalesce_fetch_as_limit() {
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;

    let original: Arc<dyn ExecutionPlan> = Arc::new(
        CoalescePartitionsExec::new(broadcast(split_source())).with_fetch(Some(5)),
    );

    let (optimized, rendered) = sink_structural(original);
    let limit = rendered.find("NdLimitExec: skip=0, fetch=5");
    let merge = rendered.find("NdCoalescePartitionsExec");
    assert!(limit.is_some() && limit < merge, "{rendered}");
    assert_eq!(run(optimized).await.unwrap().num_rows(), 5);
}

#[tokio::test]
async fn structural_rule_sinks_union() {
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::physical_plan::union::UnionExec;

    let original =
        UnionExec::try_new(vec![broadcast(test_source()), broadcast(split_source())]).unwrap();
    let expected = sorted_rows(original.clone()).await;
    assert_eq!(expected.len(), 48);

    let (optimized, rendered) = sink_structural(original);
    assert!(
        rendered.find("NdBroadcastExec") < rendered.find("NdUnionExec"),
        "{rendered}"
    );
    assert_eq!(rendered.matches("NdBroadcastExec").count(), 1, "{rendered}");
    assert_eq!(optimized.output_partitioning().partition_count(), 3);
    assert_eq!(sorted_rows(optimized).await, expected);
}

/// One flat input keeps the union above the broadcasts.
#[tokio::test]
async fn structural_rule_keeps_union_with_flat_input() {
    use datafusion::physical_plan::limit::GlobalLimitExec;
    use datafusion::physical_plan::union::UnionExec;

    // A limit over two partitions does not sink, so this input stays flat.
    let flat: Arc<dyn ExecutionPlan> =
        Arc::new(GlobalLimitExec::new(broadcast(split_source()), 0, Some(3)));
    let original = UnionExec::try_new(vec![broadcast(test_source()), flat]).unwrap();
    let (_, rendered) = sink_structural(original);
    assert!(rendered.contains("UnionExec"), "{rendered}");
    assert!(!rendered.contains("NdUnionExec"), "{rendered}");
}

/// A limit above a filter follows the filter below the broadcast.
#[tokio::test]
async fn structural_rule_sinks_limit_above_sunk_filter() {
    use datafusion::common::config::ConfigOptions;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::limit::GlobalLimitExec;

    let schema = test_source().schema();
    let predicate =
        binary(col("lat", &schema).unwrap(), Operator::Gt, lit(-1i32), &schema).unwrap();
    let filter = Arc::new(FilterExec::try_new(predicate, broadcast(test_source())).unwrap());
    let original: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(filter, 3, Some(9)));
    let expected = run(original.clone()).await.unwrap();
    assert_eq!(expected.num_rows(), 9);

    let filtered = NdFilterPushdown::new()
        .optimize(original, &ConfigOptions::default())
        .unwrap();
    let (optimized, rendered) = sink_structural(filtered);
    let limit = rendered.find("NdLimitExec");
    let nd_filter = rendered.find("NdFilterExec");
    assert!(
        rendered.starts_with("NdBroadcastExec") && limit.is_some() && limit < nd_filter,
        "{rendered}"
    );
    assert_eq!(run(optimized).await.unwrap(), expected);
}

/// The plan shapes that the SQL planner gives, with all nd rules in a session.
mod sql {
    use arrow::datatypes::SchemaRef;
    use datafusion::catalog::{Session, TableProvider};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::{Expr, TableType};
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::*;

    /// A table over the two-partition test grid, planned as an nd scan.
    #[derive(Debug)]
    struct GridTable;

    #[async_trait::async_trait]
    impl TableProvider for GridTable {
        fn schema(&self) -> SchemaRef {
            split_source().schema()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let scan = broadcast(split_source());
            match projection {
                Some(indices) => Ok(Arc::new(ProjectionExec::try_new(
                    indices.iter().map(|&index| {
                        let name = scan.schema().field(index).name().clone();
                        ProjectionExpr {
                            expr: Arc::new(datafusion::physical_expr::expressions::Column::new(
                                &name, index,
                            )),
                            alias: name,
                        }
                    }),
                    scan.clone(),
                )?)),
                None => Ok(scan),
            }
        }
    }

    fn context() -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(2))
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(NdFilterPushdown::new()))
            .with_physical_optimizer_rule(Arc::new(NdProjectionPushdown::new()))
            .with_physical_optimizer_rule(Arc::new(NdStructuralPushdown::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("t", Arc::new(GridTable)).unwrap();
        ctx
    }

    /// The physical plan of `sql` as text, and its row count.
    async fn plan_and_count(sql: &str) -> (String, usize) {
        use datafusion::physical_plan::displayable;

        let frame = context().sql(sql).await.unwrap();
        let plan = frame.clone().create_physical_plan().await.unwrap();
        let rendered = displayable(plan.as_ref()).indent(true).to_string();
        let rows = frame.collect().await.unwrap().iter().map(|b| b.num_rows()).sum();
        (rendered, rows)
    }

    #[tokio::test]
    async fn sql_limit_runs_below_the_broadcast() {
        let (plan, rows) = plan_and_count("SELECT time, lat, lon, sst FROM t LIMIT 5").await;
        assert_eq!(rows, 5);
        assert!(plan.contains("NdLimitExec"), "{plan}");
        assert!(!plan.contains("LocalLimitExec"), "{plan}");
    }

    #[tokio::test]
    async fn sql_limit_above_a_filter_runs_below_the_broadcast() {
        let (plan, rows) =
            plan_and_count("SELECT time, lat, lon, sst FROM t WHERE lat > -1 LIMIT 5").await;
        assert_eq!(rows, 5);
        let limit = plan.find("NdLimitExec");
        assert!(limit.is_some() && limit < plan.find("NdFilterExec"), "{plan}");
    }

    #[tokio::test]
    async fn sql_union_all_runs_below_the_broadcast() {
        let (plan, rows) = plan_and_count(
            "SELECT time, lat, lon, sst FROM t UNION ALL SELECT time, lat, lon, sst FROM t",
        )
        .await;
        assert_eq!(rows, 48);
        assert!(plan.contains("NdUnionExec"), "{plan}");
    }
}
