use std::{str::FromStr, sync::Arc};

use arrow::array::{Array, StringArray};
use datafusion::{
    functions::strings::StringArrayBuilder,
    logical_expr::{ScalarUDFImpl, Signature},
    scalar::ScalarValue,
};
use geo::{Contains, Geometry};
use wkt::{ToWkt, Wkt};

#[derive(Debug)]
pub struct GeoJsonAsWktUdf {
    signature: Signature,
}

impl GeoJsonAsWktUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![arrow::datatypes::DataType::Utf8],
                datafusion::logical_expr::Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for GeoJsonAsWktUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "st_geojson_as_wkt"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> datafusion::error::Result<arrow::datatypes::DataType> {
        Ok(arrow::datatypes::DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
        if args.args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "st_geojson_as_wkt expects 1 argument".to_string(),
            ));
        }
        match &args.args[0] {
            datafusion::logical_expr::ColumnarValue::Array(array) => {
                if let Some(string_arr) = array.as_any().downcast_ref::<StringArray>() {
                    let mut new_builder = arrow::array::StringBuilder::new();

                    for geo_json_str in string_arr.iter() {
                        match geo_json_str {
                            Some(geo_json_str) => {
                                let wkt = geojson_to_wkt_str(geo_json_str).map_err(|e| {
                                    datafusion::error::DataFusionError::Internal(e.to_string())
                                })?;
                                new_builder.append_value(&wkt);
                            }
                            None => {
                                new_builder.append_null();
                            }
                        }
                    }

                    Ok(datafusion::logical_expr::ColumnarValue::Array({
                        Arc::new(new_builder.finish())
                    }))
                } else {
                    return Err(datafusion::error::DataFusionError::Internal(
                        "st_geojson_as_wkt expects a string array argument".to_string(),
                    ));
                }
            }
            datafusion::logical_expr::ColumnarValue::Scalar(scalar_value) => {
                if let ScalarValue::Utf8(Some(geojson)) = scalar_value {
                    let wkt = geojson_to_wkt_str(&geojson)
                        .map_err(|e| datafusion::error::DataFusionError::Internal(e.to_string()))?;
                    Ok(datafusion::logical_expr::ColumnarValue::Scalar(
                        ScalarValue::Utf8(Some(wkt)),
                    ))
                } else {
                    return Err(datafusion::error::DataFusionError::Internal(
                        "st_geojson_as_wkt expects a string argument".to_string(),
                    ));
                }
            }
        }
    }
}

fn geojson_to_wkt_str(geo_json_str: &str) -> anyhow::Result<String> {
    let parsed_geometry = geojson::Geometry::from_str(geo_json_str)?;
    let geom: Geometry<f64> = Geometry::try_from(parsed_geometry)?;
    Ok(geom.to_wkt().to_string())
}

impl Default for GeoJsonAsWktUdf {
    fn default() -> Self {
        Self::new()
    }
}
