use datafusion::{
    arrow::datatypes::Schema,
    execution::SessionState,
    prelude::{lit, Expr},
};
use utoipa::ToSchema;

use super::parse_column_name;

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
    pub fn parse(
        &self,
        session_state: &SessionState,
        _schema: &Schema,
    ) -> datafusion::error::Result<Expr> {
        let lon = parse_column_name(&self.longitude_column);
        let lat = parse_column_name(&self.latitude_column);

        let st_geojson_as_wkt = session_state
            .scalar_functions()
            .get("st_geojson_as_wkt")
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "Function st_geojson_as_wkt not found in the registry.".to_string(),
                )
            })?
            .clone();

        let wkt_str = st_geojson_as_wkt.call(vec![lit(self.geometry.to_string())]);

        let st_within_point = session_state
            .scalar_functions()
            .get("st_within_point")
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "Function st_within_point not found in the registry.".to_string(),
                )
            })?
            .clone();

        let filter_expr = st_within_point.call(vec![wkt_str, lon, lat]);

        Ok(filter_expr)
    }
}
