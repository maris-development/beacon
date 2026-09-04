//! Rebuild geometry constants so a federated plan can be rendered back to SQL.
//!
//! DataFusion evaluates a constant call at plan time, so `ST_GeomFromGeoJSON('...')` leaves the
//! plan and a geometry value takes its place. That value is an Arrow union for a mixed geometry
//! and an Arrow struct for a point or a box. The DataFusion unparser has no SQL syntax for
//! either, so `plan_to_sql` stops with `Unsupported scalar`.
//!
//! [`geometry_literals_to_calls`] puts a constructor call back where each geometry constant sits.
//! A `Union 3:[[{x: 0.0, y: 0.0}, ...]]` constant becomes `ST_GeomFromText('POLYGON((0 0,...))')`.
//!
//! Call it immediately before the unparse step, and nowhere else. A local query needs no repair,
//! and the constant form is the faster one: a predicate prepares a constant argument once per
//! batch.

use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::Field;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Result, ScalarValue, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Expr, LogicalPlan, ScalarUDF, lit};
use datafusion_spatial::kernels::{crs, io};
use datafusion_spatial::udf;
use geoarrow_array::array::from_arrow_array;
use geoarrow_schema::GeoArrowType;

/// The Arrow key that names an extension type.
const EXTENSION_NAME: &str = "ARROW:extension:name";

/// The prefix every GeoArrow extension name carries.
const GEOARROW_PREFIX: &str = "geoarrow.";

/// Replace every geometry constant in a plan with the call that rebuilds it.
///
/// The rewrite keeps each node's declared schema, so federation's schema guard passes. The
/// constructor returns a mixed geometry, so a projected point constant widens in value while the
/// schema still reports the narrow type. The plan is unparsed and dropped straight after, so the
/// difference never reaches an executor.
///
/// # Errors
///
/// Reports a constant whose coordinate reference system has no SRID.
pub fn geometry_literals_to_calls(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform(|node| node.map_expressions(rewrite_expr))
        .map(|result| result.data)
}

/// Replace every geometry constant in one expression.
pub fn geometry_literals_in_expr(expr: Expr) -> Result<Expr> {
    rewrite_expr(expr).map(|result| result.data)
}

fn rewrite_expr(expr: Expr) -> Result<Transformed<Expr>> {
    expr.transform(|node| match &node {
        Expr::Literal(value, Some(metadata)) if is_geometry(metadata) => {
            Ok(Transformed::yes(literal_to_call(value, metadata)?))
        }
        _ => Ok(Transformed::no(node)),
    })
}

/// Does this literal hold a GeoArrow value?
///
/// The test reads the extension name, not the shape of the scalar. That is why one rule covers a
/// mixed geometry, a point and a box alike.
fn is_geometry(metadata: &FieldMetadata) -> bool {
    metadata
        .inner()
        .get(EXTENSION_NAME)
        .is_some_and(|name| name.starts_with(GEOARROW_PREFIX))
}

fn literal_to_call(value: &ScalarValue, metadata: &FieldMetadata) -> Result<Expr> {
    let field = metadata.add_to_field(Field::new("geometry", value.data_type(), true));
    let geo_type = GeoArrowType::from_arrow_field(&field).map_err(external)?;

    // A reference system that no number can express must not be dropped in silence.
    let srid = crs::srid_of(&geo_type);
    if srid == 0 && geo_type.metadata().crs().crs_value().is_some() {
        return plan_err!(
            "cannot rebuild a geometry constant whose coordinate reference system has no SRID. \
             ST_SetSRID takes a number, so a full WKT2 or PROJJSON description cannot be \
             restored. Stamp the constant with ST_SetSRID, or drop the reference system."
        );
    }

    let call = from_text(wkt_of(value, &field)?);
    if srid == 0 {
        return Ok(call);
    }
    Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::new_from_impl(udf::transform::StSetSrid::new())),
        vec![call, lit(srid)],
    )))
}

/// The Well-Known Text of one constant, or `None` when the constant is null.
///
/// Text keeps every coordinate digit, and it carries the z ordinate that GeoJSON drops.
fn wkt_of(value: &ScalarValue, field: &Field) -> Result<Option<String>> {
    if value.is_null() {
        return Ok(None);
    }
    let array = value.to_array_of_size(1)?;
    let geometry = from_arrow_array(array.as_ref(), field).map_err(external)?;
    let text = io::st_as_text(geometry.as_ref()).map_err(external)?;
    if text.is_null(0) {
        return Ok(None);
    }
    Ok(Some(text.value(0).to_string()))
}

/// `ST_GeomFromText('...')`, or `ST_GeomFromText(NULL)` for a null constant.
fn from_text(text: Option<String>) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(udf::st_geomfromtext()),
        vec![lit(ScalarValue::Utf8(text))],
    ))
}

fn external(err: geoarrow_schema::error::GeoArrowError) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use arrow::array::{Float64Array, RecordBatch};
    use arrow::datatypes::{DataType, Schema};
    use datafusion::prelude::SessionContext;
    use datafusion::sql::unparser::Unparser;

    /// The query from the bug report, optimized so its polygon constant has folded to an Arrow
    /// union. Shared with the tests that drive the same rewrite through a [`SQLTable`] hook.
    ///
    /// [`SQLTable`]: datafusion_federation::sql::SQLTable
    pub(crate) async fn folded_geometry_plan() -> LogicalPlan {
        session()
            .sql(
                "SELECT * FROM t WHERE ST_Within(ST_MakePoint(x, y),                  ST_GeomFromGeoJSON('{\"type\":\"Polygon\",\"coordinates\":                 [[[-7.0,48.5],[-7.0,49.5],[-6.0,49.5],[-6.0,48.5],[-7.0,48.5]]]}'))",
            )
            .await
            .expect("the query should plan")
            .into_optimized_plan()
            .expect("the plan should optimize")
    }

    /// A session holding the spatial functions and a two-column table to filter.
    fn session() -> SessionContext {
        let ctx = SessionContext::new();
        datafusion_spatial::register_all(&ctx);

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(vec![0.0, 1.0])),
                Arc::new(Float64Array::from(vec![45.0, 46.0])),
            ],
        )
        .expect("the test batch should build");
        ctx.register_batch("t", batch)
            .expect("the test table should register");
        ctx
    }

    /// Optimize `sql` so its geometry constants fold, then rewrite and unparse.
    ///
    /// Also asserts the two properties every case shares: the rewrite keeps the plan schema, and
    /// the SQL it emits plans again.
    async fn round_trip(sql: &str) -> String {
        let ctx = session();
        let plan = ctx
            .sql(sql)
            .await
            .expect("the query should plan")
            .into_optimized_plan()
            .expect("the plan should optimize");

        let before = plan.schema().clone();
        let rewritten = geometry_literals_to_calls(plan).expect("the rewrite should succeed");
        assert_eq!(
            &before,
            rewritten.schema(),
            "the rewrite must not alter the plan schema"
        );

        let text = Unparser::default()
            .plan_to_sql(&rewritten)
            .expect("the rewritten plan should unparse")
            .to_string();
        ctx.sql(&text)
            .await
            .unwrap_or_else(|err| panic!("the emitted SQL should re-plan: {text}\n{err}"));
        text
    }

    #[tokio::test]
    /// The reported query. A mixed geometry folds to an Arrow union, which the unparser rejects
    /// outright; after the rewrite it renders as the call that rebuilds it.
    async fn union_constant_becomes_a_constructor_call() {
        let sql = "SELECT * FROM t WHERE ST_Within(ST_MakePoint(x, y), \
                   ST_GeomFromGeoJSON('{\"type\":\"Polygon\",\"coordinates\":\
                   [[[-7.0,48.5],[-7.0,49.5],[-6.0,49.5],[-6.0,48.5],[-7.0,48.5]]]}'))";

        // Without the rewrite this is exactly the reported failure.
        let ctx = session();
        let plan = ctx
            .sql(sql)
            .await
            .expect("the query should plan")
            .into_optimized_plan()
            .expect("the plan should optimize");
        let err = Unparser::default()
            .plan_to_sql(&plan)
            .expect_err("a union constant has no SQL syntax");
        assert!(
            err.to_string().contains("Unsupported scalar: Union"),
            "unexpected error: {err}"
        );

        let text = round_trip(sql).await;
        assert!(
            text.contains("st_geomfromtext('POLYGON((-7 48.5,-7 49.5,-6 49.5,-6 48.5,-7 48.5))')"),
            "unexpected SQL: {text}"
        );
    }

    #[tokio::test]
    /// A typed point folds to an Arrow struct, not a union. The same rule covers it, because the
    /// test reads the GeoArrow extension name rather than the shape of the scalar.
    async fn struct_constant_becomes_a_constructor_call() {
        let text = round_trip(
            "SELECT * FROM t WHERE ST_Intersects(ST_MakePoint(x, y), ST_MakePoint(0, 45))",
        )
        .await;
        assert!(
            text.contains("st_geomfromtext('POINT(0 45)')"),
            "unexpected SQL: {text}"
        );
    }

    #[tokio::test]
    /// A box constant folds to an Arrow struct of its bounds. It rebuilds as the ring.
    async fn box_constant_becomes_a_constructor_call() {
        let text = round_trip(
            "SELECT * FROM t WHERE ST_Intersects(ST_MakePoint(x, y), \
             ST_Envelope(ST_GeomFromText('LINESTRING(-1 -1, 1 1)')))",
        )
        .await;
        assert!(
            text.contains("st_geomfromtext('POLYGON((-1 -1,-1 1,1 1,1 -1,-1 -1))')"),
            "unexpected SQL: {text}"
        );
    }

    #[tokio::test]
    /// A coordinate reference system lives in the field metadata, so the constructor call alone
    /// would drop it. `ST_SetSRID` puts it back.
    async fn srid_constant_keeps_its_reference_system() {
        let text = round_trip(
            "SELECT * FROM t WHERE ST_Intersects(ST_SetSRID(ST_MakePoint(x, y), 4326), \
             ST_SetSRID(ST_GeomFromText('POLYGON((-1 -1,-1 1,1 1,1 -1,-1 -1))'), 4326))",
        )
        .await;
        assert!(
            text.contains(
                "st_setsrid(st_geomfromtext('POLYGON((-1 -1,-1 1,1 1,1 -1,-1 -1))'), 4326)"
            ),
            "unexpected SQL: {text}"
        );
    }

    #[tokio::test]
    /// Well-Known Text carries the z ordinate. GeoJSON drops it, which is why the rewrite writes
    /// text and not GeoJSON.
    async fn z_ordinate_survives_the_round_trip() {
        let text = round_trip(
            "SELECT * FROM t WHERE ST_Intersects(ST_MakePoint(x, y), ST_MakePoint(1.5, 2.5, 3.5))",
        )
        .await;
        assert!(
            text.contains("st_geomfromtext('POINT Z(1.5 2.5 3.5)')"),
            "unexpected SQL: {text}"
        );
    }

    #[tokio::test]
    /// A null geometry constant still carries the extension metadata, so it rebuilds as a call
    /// over a null string rather than becoming an untyped null.
    async fn null_constant_becomes_a_null_argument() {
        let text = round_trip(
            "SELECT * FROM t WHERE ST_Intersects(ST_MakePoint(x, y), \
             ST_GeomFromText(CAST(NULL AS VARCHAR)))",
        )
        .await;
        assert!(
            text.contains("st_geomfromtext(NULL)"),
            "unexpected SQL: {text}"
        );
    }

    #[tokio::test]
    /// A plan with no geometry constant comes back untouched.
    async fn plan_without_geometry_is_unchanged() {
        let ctx = session();
        let plan = ctx
            .sql("SELECT * FROM t WHERE x > 0")
            .await
            .expect("the query should plan")
            .into_optimized_plan()
            .expect("the plan should optimize");

        let rewritten =
            geometry_literals_to_calls(plan.clone()).expect("the rewrite should succeed");
        assert_eq!(plan, rewritten);
    }
}
