use std::{cell::RefCell, num::NonZero, str::FromStr, sync::Arc};

use arrow::{array::AsArray, datatypes::Float64Type};
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature},
    scalar::ScalarValue,
};
use geo::{BoundingRect, Contains, Geometry, Point, Rect};
use ordered_float::OrderedFloat;
use wkt::Wkt;

thread_local! {
    static POINT_LOOKUP_CACHE: RefCell<lru::LruCache<Point<OrderedFloat<f64>>, bool>> = RefCell::new(lru::LruCache::new(NonZero::new(beacon_config::CONFIG.st_within_point_cache_size).expect("Cache size must be non-zero")));
}

#[derive(Debug)]
pub struct WithinPointUdf {
    signature: Signature,
}

impl WithinPointUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    arrow::datatypes::DataType::Utf8,
                    arrow::datatypes::DataType::Float64,
                    arrow::datatypes::DataType::Float64,
                ],
                datafusion::logical_expr::Volatility::Immutable,
            ),
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
        let resized_lon_array = args.args[1].to_array(args.number_rows).unwrap();
        let mut lon_iter = resized_lon_array
            .as_primitive_opt::<Float64Type>()
            .map(|array| array.iter())
            .ok_or(datafusion::error::DataFusionError::Internal(
                "st_within_point expects a float64 array as its second argument".to_string(),
            ))?;

        let resized_lat_array = args.args[2].to_array(args.number_rows).unwrap();
        let mut lat_iter = resized_lat_array
            .as_primitive_opt::<Float64Type>()
            .map(|array| array.iter())
            .ok_or(datafusion::error::DataFusionError::Internal(
                "st_within_point expects a float64 array as its third argument".to_string(),
            ))?;

        let mut geom_iter: Box<dyn Iterator<Item = Option<&str>>> = match &args.args[0] {
            datafusion::logical_expr::ColumnarValue::Array(array) => {
                if let Some(array) = array.as_string_opt::<i32>() {
                    Box::new(array.iter())
                } else {
                    return Err(datafusion::error::DataFusionError::Internal(
                        "st_within_point expects a string array as its first argument".to_string(),
                    ));
                }
            }
            datafusion::logical_expr::ColumnarValue::Scalar(scalar_value) => {
                if let ScalarValue::Utf8(wkt) = scalar_value {
                    if true {
                        if let Some(wkt) = wkt {
                            let wkt = Wkt::from_str(wkt).map_err(|e| anyhow::anyhow!(e)).map_err(
                                |e| datafusion::error::DataFusionError::Execution(e.to_string()),
                            )?;
                            let geometry: Geometry = wkt.try_into().unwrap();
                            let result =
                                st_within_point_fast(geometry, &mut lon_iter, &mut lat_iter)
                                    .map_err(|e| {
                                        datafusion::error::DataFusionError::Execution(e.to_string())
                                    })?;
                            return Ok(ColumnarValue::Array(Arc::new(
                                arrow::array::BooleanArray::from(result),
                            )));
                        }
                    }

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
            let geometry: Geometry = wkt.try_into().unwrap();

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
) -> anyhow::Result<Vec<bool>> {
    let bounding_rect = geom.bounding_rect();
    lon.zip(lat)
        .map(|(lon, lat)| st_within_point_fast_impl(&geom, bounding_rect, lon, lat))
        .collect()
}

fn st_within_point_fast_impl(
    geometry: &Geometry,
    bounding_rect: Option<Rect>,
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
            let result = POINT_LOOKUP_CACHE.with(|cache| {
                if let Some(result) = cache.borrow_mut().get(&ordered_point) {
                    return Some(*result);
                }
                None
            });

            if let Some(result) = result {
                return Ok(result);
            }

            // If not found in cache, perform the geometry check
            let result = geometry.contains(&point);

            // Store the result in the cache
            POINT_LOOKUP_CACHE.with(|cache| {
                cache.borrow_mut().put(ordered_point, result);
            });

            Ok(result)
        }
        _ => Ok(false),
    }
}

impl Default for WithinPointUdf {
    fn default() -> Self {
        Self::new()
    }
}
