//! Turning a spatial predicate into a query bounding box.
//!
//! A GeoParquet file states a bounding box per row group, either in its
//! `covering` metadata or in the native coordinate columns the reader infers one
//! from. So a scan can drop a whole row group before it reads one byte of data,
//! provided the query states a box of its own.
//!
//! This module reads that box out of the predicate. It answers `None` for
//! anything it does not recognise, which costs a full scan and never a wrong
//! row.

use std::sync::Arc;

use arrow::array::{Array, RecordBatchOptions};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::utils::{collect_columns, split_conjunction};
use datafusion::physical_plan::PhysicalExpr;
use datafusion_spatial_kernels::envelope::{Bound, bound};
use geo::{Rect, coord};

/// The predicates a bounding box can decide a row group on.
///
/// Each one holds only for a row whose own box meets the query box. So a row
/// group whose box misses the query box holds no matching row, and the scan can
/// skip it. The test is necessary and never sufficient, so the predicate itself
/// still runs above the scan.
///
/// `ST_Contains` and `ST_Within` belong here whichever way round their two
/// arguments read: one geometry inside another has its box inside the other's,
/// and two boxes where one holds the other do meet.
///
/// `ST_Disjoint` is absent, and cannot join: the row it keeps is exactly the row
/// that lies outside the query box.
const BOX_PREDICATES: [&str; 4] = [
    "st_intersects",
    "st_within",
    "st_contains",
    "st_bboxintersects",
];

/// `ST_DWithin(geometry, constant, radius)`. The box is the constant's own, grown
/// by the radius on every side.
const DISTANCE_PREDICATE: &str = "st_dwithin";

/// A box a scan may drop row groups against, and the geometry column it reads.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QueryBox {
    /// The geometry column the predicate names, by its table schema name.
    pub column: String,
    /// The box every matching row's own box must meet.
    pub rect: Rect<f64>,
}

impl QueryBox {
    /// This box narrowed by a second one the same scan states.
    ///
    /// Two boxes over one column compose as a conjunction, so their overlap is
    /// the answer. Two boxes over different columns, or two that miss each
    /// other, leave this one alone: a wider box costs a read and never a wrong
    /// row, and the predicates themselves still run above the scan.
    pub fn narrowed_by(self, other: QueryBox) -> QueryBox {
        if self.column != other.column {
            return self;
        }
        match intersection(self.rect, other.rect) {
            Some(rect) => QueryBox { rect, ..self },
            None => self,
        }
    }
}

/// The box `predicate` states over one geometry column, if it states one.
///
/// A conjunction narrows the box: a row has to satisfy every part of it, so the
/// parts intersect. A predicate this module does not recognise is dropped, which
/// only widens the box. Parts naming a second geometry column are dropped too —
/// the reader prunes on one column.
pub(crate) fn query_box(
    predicate: &Arc<dyn PhysicalExpr>,
    table_schema: &SchemaRef,
) -> Option<QueryBox> {
    let mut found: Option<QueryBox> = None;

    for conjunct in split_conjunction(predicate) {
        let Some(next) = conjunct_box(conjunct, table_schema) else {
            continue;
        };
        found = Some(match found {
            None => next,
            Some(current) if current.column == next.column => QueryBox {
                rect: intersection(current.rect, next.rect)?,
                column: current.column,
            },
            Some(current) => current,
        });
    }

    found
}

/// The box one predicate states, or `None` if it is not one of the five.
fn conjunct_box(expr: &Arc<dyn PhysicalExpr>, table_schema: &SchemaRef) -> Option<QueryBox> {
    let call = expr.as_any().downcast_ref::<ScalarFunctionExpr>()?;
    let args = call.args();

    if BOX_PREDICATES.contains(&call.name()) && args.len() == 2 {
        let (column, constant) = geometry_and_constant(&args[0], &args[1])?;
        return Some(QueryBox {
            column,
            rect: constant_rect(constant, table_schema)?,
        });
    }

    if call.name() == DISTANCE_PREDICATE && args.len() == 3 {
        let (column, constant) = geometry_and_constant(&args[0], &args[1])?;
        let radius = constant_f64(&args[2])?;
        // A negative radius keeps nothing, which is not a box. Leave it to the
        // predicate itself rather than inventing an empty one.
        if !radius.is_finite() || radius < 0.0 {
            return None;
        }
        let rect = constant_rect(constant, table_schema)?;
        return Some(QueryBox {
            column,
            rect: Rect::new(
                coord! { x: rect.min().x - radius, y: rect.min().y - radius },
                coord! { x: rect.max().x + radius, y: rect.max().y + radius },
            ),
        });
    }

    None
}

/// Split a two-argument predicate into the column it reads and the constant it
/// compares against, whichever order the query wrote them in.
fn geometry_and_constant<'a>(
    left: &'a Arc<dyn PhysicalExpr>,
    right: &'a Arc<dyn PhysicalExpr>,
) -> Option<(String, &'a Arc<dyn PhysicalExpr>)> {
    if let Some(column) = left.as_any().downcast_ref::<Column>()
        && collect_columns(right).is_empty()
    {
        return Some((column.name().to_string(), right));
    }
    if let Some(column) = right.as_any().downcast_ref::<Column>()
        && collect_columns(left).is_empty()
    {
        return Some((column.name().to_string(), left));
    }
    None
}

/// The bounding box of a constant geometry expression.
///
/// The expression reads no column, so one row of nothing is input enough. Its
/// field carries the GeoArrow type, which is what turns the plain Arrow array
/// back into a geometry the box walker can read.
fn constant_rect(expr: &Arc<dyn PhysicalExpr>, table_schema: &SchemaRef) -> Option<Rect<f64>> {
    let field = expr.return_field(table_schema).ok()?;
    let array = expr.evaluate(&one_empty_row()?).ok()?.to_array(1).ok()?;
    let geometry = geoarrow_array::array::from_arrow_array(array.as_ref(), &field).ok()?;

    let read = |which| -> Option<f64> {
        let values = bound(geometry.as_ref(), which).ok()?;
        (!values.is_empty() && values.is_valid(0)).then(|| values.value(0))
    };

    let (xmin, ymin, xmax, ymax) = (
        read(Bound::XMin)?,
        read(Bound::YMin)?,
        read(Bound::XMax)?,
        read(Bound::YMax)?,
    );
    // An empty geometry states no box, and NaN would compare false against every
    // row group, so it would drop the whole file.
    if [xmin, ymin, xmax, ymax].iter().any(|v| !v.is_finite()) {
        return None;
    }

    Some(Rect::new(
        coord! { x: xmin, y: ymin },
        coord! { x: xmax, y: ymax },
    ))
}

/// The value of a constant numeric expression, as `f64`.
fn constant_f64(expr: &Arc<dyn PhysicalExpr>) -> Option<f64> {
    if !collect_columns(expr).is_empty() {
        return None;
    }
    let value = expr.evaluate(&one_empty_row()?).ok()?;
    match value {
        datafusion::logical_expr::ColumnarValue::Scalar(scalar) => scalar
            .cast_to(&arrow::datatypes::DataType::Float64)
            .ok()
            .and_then(|scalar| match scalar {
                datafusion::common::ScalarValue::Float64(value) => value,
                _ => None,
            }),
        datafusion::logical_expr::ColumnarValue::Array(_) => None,
    }
}

/// One row of no columns: enough to evaluate an expression that reads none.
fn one_empty_row() -> Option<RecordBatch> {
    RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .ok()
}

/// The overlap of two boxes, or `None` when they miss each other.
fn intersection(left: Rect<f64>, right: Rect<f64>) -> Option<Rect<f64>> {
    let min = coord! {
        x: left.min().x.max(right.min().x),
        y: left.min().y.max(right.min().y),
    };
    let max = coord! {
        x: left.max().x.min(right.max().x),
        y: left.max().y.min(right.max().y),
    };
    (min.x <= max.x && min.y <= max.y).then(|| Rect::new(min, max))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::physical_expr::expressions::lit;
    use datafusion_spatial::udf::{envelopes, io_functions, predicates};
    use geoarrow_schema::{CoordType, Dimension, Metadata, PointType};

    /// A table with a geometry column at a position other than the first, so a
    /// wrong column index shows up as a wrong answer.
    fn schema() -> SchemaRef {
        let point = PointType::new(Dimension::XY, Arc::new(Metadata::default()))
            .with_coord_type(CoordType::Separated);
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("temperature", DataType::Float64, true),
            point.to_field("geometry", true),
        ]))
    }

    fn udf(name: &str) -> ScalarUDF {
        predicates()
            .into_iter()
            .chain(envelopes())
            .chain(io_functions())
            .find(|f| f.name() == name)
            .unwrap_or_else(|| panic!("{name} is registered"))
    }

    /// `ST_GeomFromText('<wkt>')` as a constant expression.
    fn wkt(text: &str) -> Arc<dyn PhysicalExpr> {
        call("st_geomfromtext", vec![lit(text)])
    }

    fn call(name: &str, args: Vec<Arc<dyn PhysicalExpr>>) -> Arc<dyn PhysicalExpr> {
        Arc::new(
            ScalarFunctionExpr::try_new(
                Arc::new(udf(name)),
                args,
                &schema(),
                Arc::new(datafusion::config::ConfigOptions::default()),
            )
            .expect("the call type checks"),
        )
    }

    fn geometry_column() -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new("geometry", 2))
    }

    #[test]
    fn intersects_against_a_constant_states_its_box() {
        let expr = call(
            "st_intersects",
            vec![geometry_column(), wkt("POLYGON((0 0, 4 0, 4 2, 0 2, 0 0))")],
        );
        let found = query_box(&expr, &schema()).expect("a box");
        assert_eq!(found.column, "geometry");
        assert_eq!(
            found.rect,
            Rect::new(coord! { x: 0.0, y: 0.0 }, coord! { x: 4.0, y: 2.0 })
        );
    }

    /// The constant may be written first. PostGIS argument order decides what the
    /// predicate means, but the box it states is the same either way.
    #[test]
    fn the_constant_may_come_first() {
        let expr = call(
            "st_contains",
            vec![wkt("POLYGON((0 0, 4 0, 4 2, 0 2, 0 0))"), geometry_column()],
        );
        let found = query_box(&expr, &schema()).expect("a box");
        assert_eq!(found.column, "geometry");
        assert_eq!(
            found.rect,
            Rect::new(coord! { x: 0.0, y: 0.0 }, coord! { x: 4.0, y: 2.0 })
        );
    }

    /// `ST_DWithin` grows the constant's box by the radius on every side.
    #[test]
    fn dwithin_grows_the_box_by_the_radius() {
        let expr = call(
            "st_dwithin",
            vec![geometry_column(), wkt("POINT(10 20)"), lit(2.5_f64)],
        );
        let found = query_box(&expr, &schema()).expect("a box");
        assert_eq!(
            found.rect,
            Rect::new(coord! { x: 7.5, y: 17.5 }, coord! { x: 12.5, y: 22.5 })
        );
    }

    /// Two predicates over one column narrow the box to their overlap.
    #[test]
    fn a_conjunction_narrows_the_box() {
        let left = call(
            "st_intersects",
            vec![
                geometry_column(),
                wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"),
            ],
        );
        let right = call(
            "st_intersects",
            vec![
                geometry_column(),
                wkt("POLYGON((4 4, 20 4, 20 20, 4 20, 4 4))"),
            ],
        );
        let expr = datafusion::physical_expr::conjunction([left, right]);

        let found = query_box(&expr, &schema()).expect("a box");
        assert_eq!(
            found.rect,
            Rect::new(coord! { x: 4.0, y: 4.0 }, coord! { x: 10.0, y: 10.0 })
        );
    }

    /// `ST_Disjoint` keeps the rows outside the box, so its box must not prune.
    #[test]
    fn disjoint_states_no_box() {
        let expr = call(
            "st_disjoint",
            vec![geometry_column(), wkt("POLYGON((0 0, 4 0, 4 2, 0 2, 0 0))")],
        );
        assert_eq!(query_box(&expr, &schema()), None);
    }

    /// Two columns compared against each other state no constant box.
    #[test]
    fn a_predicate_over_two_columns_states_no_box() {
        let expr = call("st_intersects", vec![geometry_column(), geometry_column()]);
        assert_eq!(query_box(&expr, &schema()), None);
    }

    /// A predicate this module does not know only widens the box, so a
    /// conjunction still prunes on the part it does know.
    #[test]
    fn an_unknown_predicate_is_dropped_from_the_conjunction() {
        let known = call(
            "st_intersects",
            vec![geometry_column(), wkt("POLYGON((0 0, 4 0, 4 2, 0 2, 0 0))")],
        );
        let unknown = call("st_touches", vec![geometry_column(), wkt("POINT(100 100)")]);
        let expr = datafusion::physical_expr::conjunction([known, unknown]);

        let found = query_box(&expr, &schema()).expect("a box");
        assert_eq!(
            found.rect,
            Rect::new(coord! { x: 0.0, y: 0.0 }, coord! { x: 4.0, y: 2.0 })
        );
    }
}
