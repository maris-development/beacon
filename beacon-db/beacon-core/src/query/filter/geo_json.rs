use std::sync::Arc;

use datafusion::{
    arrow::datatypes::Schema,
    execution::SessionState,
    logical_expr::ScalarUDF,
    prelude::{lit, Expr},
};
use utoipa::ToSchema;

use super::parse_column_name;

/// Builds a point from the longitude and the latitude column.
const ST_POINT: &str = "st_point";
/// Parses the GeoJSON of the request into a geometry.
const ST_GEOM_FROM_GEOJSON: &str = "st_geomfromgeojson";
/// Tests the point against the geometry.
const ST_WITHIN: &str = "st_within";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct GeoJsonFilter {
    #[serde(alias = "longitude_query_parameter")]
    longitude_column: String,
    #[serde(alias = "latitude_query_parameter")]
    latitude_column: String,
    #[schema(value_type = Object)]
    geometry: geojson::Geometry,
}

impl GeoJsonFilter {
    /// Renders the filter as `ST_Within(ST_Point(lon, lat), ST_GeomFromGeoJSON('<geometry>'))`.
    ///
    /// The three functions come from `datafusion-spatial`, so the JSON path and the SQL path
    /// state the same test under the same names. The GeoJSON geometry goes to the parser as a
    /// literal string; no WKT step sits between the two.
    pub fn parse(
        &self,
        session_state: &SessionState,
        _schema: &Schema,
    ) -> datafusion::error::Result<Expr> {
        let lon = parse_column_name(&self.longitude_column);
        let lat = parse_column_name(&self.latitude_column);

        let st_point = spatial_udf(session_state, ST_POINT)?;
        let st_geom_from_geojson = spatial_udf(session_state, ST_GEOM_FROM_GEOJSON)?;
        let st_within = spatial_udf(session_state, ST_WITHIN)?;

        let point = st_point.call(vec![lon, lat]);
        let geometry = st_geom_from_geojson.call(vec![lit(self.geometry.to_string())]);

        Ok(st_within.call(vec![point, geometry]))
    }
}

/// Reads one spatial function out of the session registry.
fn spatial_udf(
    session_state: &SessionState,
    name: &str,
) -> datafusion::error::Result<Arc<ScalarUDF>> {
    session_state
        .scalar_functions()
        .get(name)
        .cloned()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(format!(
                "Function {name} not found in the registry."
            ))
        })
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Float64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::prelude::SessionContext;

    use super::*;

    /// A session that holds the spatial function set.
    fn session() -> SessionContext {
        let ctx = SessionContext::new();
        datafusion_spatial::register_all(&ctx);
        ctx
    }

    fn filter(json: &str) -> GeoJsonFilter {
        serde_json::from_str(json).expect("the filter deserializes")
    }

    /// A request over a square between 0 and 10 degrees east and 50 and 60 degrees north.
    const POLYGON: &str = r#"{"longitude_column": "lon", "latitude_column": "lat",
        "geometry": {"type": "Polygon",
                     "coordinates": [[[0.0, 50.0], [10.0, 50.0], [10.0, 60.0],
                                      [0.0, 60.0], [0.0, 50.0]]]}}"#;

    /// The expression the filter builds. A client sends the same request as before, so the shape
    /// of this expression is the contract of the GeoJSON filter.
    #[test]
    fn the_filter_builds_st_within_over_a_point_and_the_geojson_geometry() {
        let session = session();
        let expr = filter(POLYGON)
            .parse(&session.state(), &Schema::empty())
            .expect("the spatial functions are registered");

        assert_eq!(
            expr.to_string(),
            "st_within(st_point(lon, lat), \
             st_geomfromgeojson(Utf8(\"{\"type\":\"Polygon\",\"coordinates\":\
             [[[0.0,50.0],[10.0,50.0],[10.0,60.0],[0.0,60.0],[0.0,50.0]]]}\")))"
        );
    }

    /// The point takes the longitude first and the latitude second, and the geometry is the
    /// second argument of the test. A swap of either pair reads the request wrongly.
    #[test]
    fn the_argument_order_follows_the_request() {
        let session = session();
        let expr = filter(POLYGON)
            .parse(&session.state(), &Schema::empty())
            .expect("the spatial functions are registered");

        let Expr::ScalarFunction(within) = &expr else {
            panic!("expected a scalar function, got {expr}");
        };
        assert_eq!(within.func.name(), ST_WITHIN);
        assert_eq!(within.args.len(), 2);

        let Expr::ScalarFunction(point) = &within.args[0] else {
            panic!("expected a point, got {}", within.args[0]);
        };
        assert_eq!(point.func.name(), ST_POINT);
        assert_eq!(
            point.args,
            vec![parse_column_name("lon"), parse_column_name("lat")]
        );

        let Expr::ScalarFunction(geometry) = &within.args[1] else {
            panic!("expected a geometry, got {}", within.args[1]);
        };
        assert_eq!(geometry.func.name(), ST_GEOM_FROM_GEOJSON);
        assert_eq!(geometry.args.len(), 1);
    }

    /// The expression runs, and it keeps the rows inside the square. A plan alone does not show
    /// that the geometry reaches the test: `ST_GeomFromGeoJSON` holds a constant, and a wrong
    /// argument order would still plan.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_filter_keeps_the_rows_inside_the_geometry() {
        let session = session();
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("lon", DataType::Float64, true),
            Field::new("lat", DataType::Float64, true),
        ]));
        // Two rows inside the square, then one east of it, one south of it, and one null.
        let batch = RecordBatch::try_new(
            table_schema,
            vec![
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    Some(9.0),
                    Some(20.0),
                    Some(5.0),
                    None,
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(51.0),
                    Some(59.0),
                    Some(55.0),
                    Some(10.0),
                    Some(55.0),
                ])),
            ],
        )
        .expect("the two columns hold the same row count");
        session
            .register_batch("points", batch)
            .expect("the table name is free");

        let expr = filter(POLYGON)
            .parse(&session.state(), &Schema::empty())
            .expect("the spatial functions are registered");
        let rows: usize = session
            .table("points")
            .await
            .expect("the table is registered")
            .filter(expr)
            .expect("the filter applies")
            .collect()
            .await
            .expect("the query runs")
            .iter()
            .map(|batch| batch.num_rows())
            .sum();

        assert_eq!(rows, 2);
    }

    /// A session without the spatial set names the function it misses.
    #[test]
    fn a_missing_spatial_function_names_itself() {
        let error = filter(POLYGON)
            .parse(&SessionContext::new().state(), &Schema::empty())
            .expect_err("a bare session holds no spatial function");
        assert!(
            error.to_string().contains(ST_POINT),
            "unexpected error: {error}"
        );
    }
}
