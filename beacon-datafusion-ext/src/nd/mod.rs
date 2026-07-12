//! N-dimensional Arrow arrays and dimension-aware physical operators.
//!
//! Beacon's nd formats (zarr, netcdf) are logically grids of variables over
//! shared named dimensions. Flattening a grid to a table broadcasts every
//! variable onto the full dimension cross-product. This module keeps columns
//! *un-broadcast* through the physical plan until a terminal node materializes
//! them:
//!
//! - [`NdArrowArray`]: a flat Arrow array plus named C-order [`Dimensions`].
//! - [`BroadcastMap`]: a broadcast expressed as stride arithmetic; no data
//!   moves.
//! - [`NdRecordBatch`]: columns (each on a subset of the dimensions) over a
//!   shared target grid.
//! - [`exec::NdSourceExec`]: streams nd batches from an [`exec::NdBatchProvider`].
//! - [`exec::NdBroadcastExec`]: the terminal node that materializes nd batches
//!   into flat `RecordBatch`es.
//!
//! Filtering and projection operators are layered on top of this spine later.

pub mod array;
pub mod batch;
pub mod broadcast;
pub mod dimensions;
pub mod encoding;
pub mod exec;
pub mod optimizer;

pub use array::NdArrowArray;
pub use batch::NdRecordBatch;
pub use broadcast::BroadcastMap;
pub use dimensions::{Dimension, Dimensions};
pub use encoding::{
    decode_nd_record_batch, encode_flat_batch_as_nd, encode_nd_record_batch, encoded_schema,
    is_nd_encoded, logical_schema, nd_encoded_field, nd_encoded_type,
};
pub use optimizer::{NdProjectionPushdown, is_pushable_expr};

#[cfg(test)]
mod tests {
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

    use super::encoding::encode_nd_record_batch;
    use super::exec::{NdBroadcastExec, NdSourceExec};
    use super::*;

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

        let encoded_schema = encoded[0].schema();
        // One partition, each nd chunk its own single-row encoded batch.
        let source = MemorySourceConfig::try_new_exec(
            &[encoded.into_iter().map(|b| vec![b]).flatten().collect()],
            encoded_schema,
            None,
        )
        .unwrap();
        Arc::new(NdSourceExec::try_new(source).unwrap())
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

    use super::exec::NdProjectionExec;

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
        use std::any::Any;

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
            fn as_any(&self) -> &dyn Any {
                self
            }
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
}
