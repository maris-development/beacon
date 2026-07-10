use std::{cell::RefCell, num::NonZero, str::FromStr, sync::Arc};

use arrow::{array::AsArray, datatypes::Float64Type};
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature},
    scalar::ScalarValue,
};
use geo::{BoundingRect, Contains, Geometry, Point, Rect};
use ordered_float::OrderedFloat;
use wkt::Wkt;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct WithinPointUdf {
    signature: Signature,
    /// LRU cache capacity for point-in-geometry results (per invocation).
    cache_size: usize,
}

impl WithinPointUdf {
    pub fn new(cache_size: usize) -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    arrow::datatypes::DataType::Utf8,
                    arrow::datatypes::DataType::Float64,
                    arrow::datatypes::DataType::Float64,
                ],
                datafusion::logical_expr::Volatility::Immutable,
            ),
            cache_size,
        }
    }
}

impl ScalarUDFImpl for WithinPointUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "st_within_point"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> datafusion::error::Result<arrow::datatypes::DataType> {
        Ok(arrow::datatypes::DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
        let resized_lon_array = args.args[1].to_array(args.number_rows)?;
        let mut lon_iter = resized_lon_array
            .as_primitive_opt::<Float64Type>()
            .map(|array| array.iter())
            .ok_or(datafusion::error::DataFusionError::Internal(
                "st_within_point expects a float64 array as its second argument".to_string(),
            ))?;

        let resized_lat_array = args.args[2].to_array(args.number_rows)?;
        let mut lat_iter = resized_lat_array
            .as_primitive_opt::<Float64Type>()
            .map(|array| array.iter())
            .ok_or(datafusion::error::DataFusionError::Internal(
                "st_within_point expects a float64 array as its third argument".to_string(),
            ))?;

        let mut geom_iter: Box<dyn Iterator<Item = Option<&str>>> =
            match &args.args[0] {
                datafusion::logical_expr::ColumnarValue::Array(array) => {
                    if let Some(array) = array.as_string_opt::<i32>() {
                        Box::new(array.iter())
                    } else {
                        return Err(datafusion::error::DataFusionError::Internal(
                            "st_within_point expects a string array as its first argument"
                                .to_string(),
                        ));
                    }
                }
                datafusion::logical_expr::ColumnarValue::Scalar(scalar_value) => {
                    if let ScalarValue::Utf8(wkt) = scalar_value {
                        if let Some(wkt) = wkt {
                            let wkt = Wkt::from_str(wkt).map_err(|e| anyhow::anyhow!(e)).map_err(
                                |e| datafusion::error::DataFusionError::Execution(e.to_string()),
                            )?;
                            let geometry: Geometry = wkt.try_into().map_err(|e| {
                                datafusion::error::DataFusionError::Execution(format!(
                                    "st_within_point: invalid WKT geometry: {e:?}"
                                ))
                            })?;
                            let result = st_within_point_fast(
                                geometry,
                                &mut lon_iter,
                                &mut lat_iter,
                                self.cache_size,
                            )
                            .map_err(|e| {
                                datafusion::error::DataFusionError::Execution(e.to_string())
                            })?;
                            return Ok(ColumnarValue::Array(Arc::new(
                                arrow::array::BooleanArray::from(result),
                            )));
                        }
                        // Fallback to repeating the WKT string
                        Box::new(std::iter::repeat_n(wkt.as_deref(), args.number_rows))
                    } else {
                        return Err(datafusion::error::DataFusionError::Internal(
                            "st_within_point expects a string as its first argument".to_string(),
                        ));
                    }
                }
            };

        let result = st_within_point(&mut geom_iter, &mut lon_iter, &mut lat_iter)
            .map_err(|e| datafusion::error::DataFusionError::Internal(e.to_string()))?;

        Ok(ColumnarValue::Array(Arc::new(
            arrow::array::BooleanArray::from(result),
        )))
    }
}

fn st_within_point<'a>(
    geom: &mut dyn Iterator<Item = Option<&'a str>>,
    lon: &mut dyn Iterator<Item = Option<f64>>,
    lat: &mut dyn Iterator<Item = Option<f64>>,
) -> anyhow::Result<Vec<bool>> {
    geom.zip(lon.zip(lat))
        .map(|(geom, (lon, lat))| st_within_point_impl(geom, lon, lat))
        .collect()
}

fn st_within_point_impl(
    geom: Option<&str>,
    lon: Option<f64>,
    lat: Option<f64>,
) -> anyhow::Result<bool> {
    match (geom, lon, lat) {
        (Some(geom), Some(lon), Some(lat)) => {
            // ST_WithinPoint implementation
            let wkt = Wkt::from_str(geom).map_err(|e| anyhow::anyhow!(e))?;
            let geometry: Geometry = wkt
                .try_into()
                .map_err(|e| anyhow::anyhow!("invalid WKT geometry: {e:?}"))?;

            let point = geo::Point::new(lon, lat);
            Ok(geometry.contains(&point))
        }
        _ => Ok(false),
    }
}

fn st_within_point_fast(
    geom: Geometry,
    lon: &mut dyn Iterator<Item = Option<f64>>,
    lat: &mut dyn Iterator<Item = Option<f64>>,
    cache_size: usize,
) -> anyhow::Result<Vec<bool>> {
    let mut cache: lru::LruCache<Point<OrderedFloat<f64>>, bool> =
        lru::LruCache::new(NonZero::new(cache_size).expect("Cache size must be non-zero"));
    let bounding_rect = geom.bounding_rect();
    lon.zip(lat)
        .map(|(lon, lat)| st_within_point_fast_impl(&geom, bounding_rect, &mut cache, lon, lat))
        .collect()
}

fn st_within_point_fast_impl(
    geometry: &Geometry,
    bounding_rect: Option<Rect>,
    cache: &mut lru::LruCache<Point<OrderedFloat<f64>>, bool>,
    lon: Option<f64>,
    lat: Option<f64>,
) -> anyhow::Result<bool> {
    match (geometry, lon, lat) {
        (geometry, Some(lon), Some(lat)) => {
            // ST_WithinPoint implementation
            let point = geo::Point::new(lon, lat);
            let ordered_point = geo::Point::new(OrderedFloat(lon), OrderedFloat(lat));

            // If the point is outside the bounding rectangle, it cannot be within the geometry
            if let Some(rect) = bounding_rect {
                if !rect.contains(&point) {
                    return Ok(false);
                }
            }

            // If the bounding rectangle is not available, we proceed with the full geometry check
            // First, check the cache
            if let Some(result) = cache.get(&ordered_point) {
                return Ok(*result);
            }

            // If not found in cache, perform the geometry check
            let result = geometry.contains(&point);

            // Store the result in the cache
            cache.put(ordered_point, result);

            Ok(result)
        }
        _ => Ok(false),
    }
}

impl Default for WithinPointUdf {
    fn default() -> Self {
        // Historical default cache size (formerly the env default).
        Self::new(10_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 10x10 square anchored at the origin.
    const SQUARE: &str = "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))";

    fn geometry(wkt: &str) -> Geometry {
        Wkt::from_str(wkt).unwrap().try_into().unwrap()
    }

    #[test]
    fn point_inside_polygon_is_within() {
        assert!(st_within_point_impl(Some(SQUARE), Some(5.0), Some(5.0)).unwrap());
    }

    #[test]
    fn point_outside_polygon_is_not_within() {
        assert!(!st_within_point_impl(Some(SQUARE), Some(20.0), Some(20.0)).unwrap());
    }

    #[test]
    fn missing_geometry_or_coordinate_yields_false() {
        assert!(!st_within_point_impl(None, Some(5.0), Some(5.0)).unwrap());
        assert!(!st_within_point_impl(Some(SQUARE), None, Some(5.0)).unwrap());
        assert!(!st_within_point_impl(Some(SQUARE), Some(5.0), None).unwrap());
    }

    #[test]
    fn invalid_wkt_is_an_error() {
        let err = st_within_point_impl(Some("NOT WKT"), Some(1.0), Some(1.0));
        assert!(err.is_err());
    }

    #[test]
    fn iterator_variant_maps_each_row() {
        let geoms = [Some(SQUARE), Some(SQUARE), None];
        let lons = [Some(5.0), Some(50.0), Some(5.0)];
        let lats = [Some(5.0), Some(50.0), Some(5.0)];

        let result = st_within_point(
            &mut geoms.into_iter(),
            &mut lons.into_iter(),
            &mut lats.into_iter(),
        )
        .unwrap();

        assert_eq!(result, vec![true, false, false]);
    }

    #[test]
    fn fast_variant_matches_the_simple_variant() {
        let geom = geometry(SQUARE);
        let lons = [Some(5.0), Some(20.0), None];
        let lats = [Some(5.0), Some(20.0), Some(5.0)];

        let result =
            st_within_point_fast(geom, &mut lons.into_iter(), &mut lats.into_iter(), 16).unwrap();

        assert_eq!(result, vec![true, false, false]);
    }

    #[test]
    fn fast_impl_rejects_points_outside_the_bounding_rect() {
        let geom = geometry(SQUARE);
        let bounding_rect = geom.bounding_rect();
        let mut cache = lru::LruCache::new(NonZero::new(4).unwrap());

        // Far outside the bounding rect: short-circuits to false.
        assert!(!st_within_point_fast_impl(
            &geom,
            bounding_rect,
            &mut cache,
            Some(100.0),
            Some(100.0)
        )
        .unwrap());
    }

    #[test]
    fn fast_impl_caches_repeated_lookups() {
        let geom = geometry(SQUARE);
        let bounding_rect = geom.bounding_rect();
        let mut cache = lru::LruCache::new(NonZero::new(4).unwrap());

        // First call computes and stores; second call must read from the cache
        // and return the same answer.
        let first =
            st_within_point_fast_impl(&geom, bounding_rect, &mut cache, Some(5.0), Some(5.0))
                .unwrap();
        assert!(first);
        assert_eq!(cache.len(), 1);

        let second =
            st_within_point_fast_impl(&geom, bounding_rect, &mut cache, Some(5.0), Some(5.0))
                .unwrap();
        assert_eq!(first, second);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn fast_impl_handles_missing_coordinates() {
        let geom = geometry(SQUARE);
        let bounding_rect = geom.bounding_rect();
        let mut cache = lru::LruCache::new(NonZero::new(4).unwrap());
        assert!(
            !st_within_point_fast_impl(&geom, bounding_rect, &mut cache, None, Some(5.0)).unwrap()
        );
    }
}
