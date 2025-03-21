use std::{collections::HashMap, sync::Arc};

use arrow::{array::PrimitiveArray, datatypes::Float64Type};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};
use lazy_static::lazy_static;

macro_rules! create_conversion_fn {
    ($name:ident, $factor:expr, $offset: expr) => {
        fn $name(x: f64) -> f64 {
            $factor * x + $offset
        }
    };
}

macro_rules! chain_conversion_fn {
    ($name:ident, $first:ident $(, $rest:ident)*) => {
        fn $name(x: f64) -> f64 {
            // Apply the first function, then the rest in sequence
            let mut value = $first(x);
            $(
                value = $rest(value);
            )*
            value
        }
    };
}

create_conversion_fn!(kgum_upox, 1.025, 0.0);
create_conversion_fn!(upox_kgum, 1.0 / 1.025, 0.0);
create_conversion_fn!(umll_upox, 44.66080, 0.0);
create_conversion_fn!(upox_umll, 1.0 / 44.66080, 0.0);

chain_conversion_fn!(kgum_umll, kgum_upox, upox_umll);
chain_conversion_fn!(umll_kgum, umll_upox, upox_kgum);

lazy_static! {
    static ref UNIT_CONV_MAP: HashMap<(&'static str, &'static str), fn(f64) -> f64> = {
        let mut map: HashMap<(&str, &str), fn(f64) -> f64> = HashMap::new();
        map.insert(("SDN:P06::KGUM", "SDN:P06::UPOX"), kgum_upox);
        map.insert(("SDN:P06::UPOX", "SDN:P06::KGUM"), upox_kgum);
        map.insert(("SDN:P06::UMLL", "SDN:P06::UPOX"), umll_upox);
        map.insert(("SDN:P06::UPOX", "SDN:P06::UMLL"), upox_umll);
        map.insert(("SDN:P06::KGUM", "SDN:P06::UMLL"), kgum_umll);
        map.insert(("SDN:P06::UMLL", "SDN:P06::KGUM"), umll_kgum);

        map
    };
}

pub fn map_units_seadatanet() -> ScalarUDF {
    ScalarUDF::new_from_impl(MapUnitsSeaDataNet::new())
}

#[derive(Clone, Debug)]
struct MapUnitsSeaDataNet {
    signature: datafusion::logical_expr::Signature,
}

impl MapUnitsSeaDataNet {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], datafusion::logical_expr::Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapUnitsSeaDataNet {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "map_units_seadatanet"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> datafusion::error::Result<arrow::datatypes::DataType> {
        Ok(arrow::datatypes::DataType::Float64)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let ScalarFunctionArgs {
            mut args,
            number_rows,
            ..
        } = args;

        let arg0 = args[0].clone();
        let arg1 = args[1].clone();
        let arg2 = args[2].clone();

        let from_unit = match arg0 {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar_value) => {
                scalar_value.to_array_of_size(number_rows).unwrap()
            }
        };
        let to_unit = match arg1 {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar_value) => {
                scalar_value.to_array_of_size(number_rows).unwrap()
            }
        };

        let values = match arg2 {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar_value) => {
                scalar_value.to_array_of_size(number_rows).unwrap()
            }
        };

        let from_unit = from_unit
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let to_unit = to_unit
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        let values = values
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();

        let array = PrimitiveArray::<Float64Type>::from_iter(
            values
                .iter()
                .zip(from_unit.iter().zip(to_unit.iter()))
                .map(|(value, (from, to))| {
                    //zip all the values together
                    let row = value.zip(from).zip(to);
                    row.map(|((value, from), to)| {
                        //get the conversion function
                        let conversion_fn = UNIT_CONV_MAP.get(&(from, to));
                        //apply the conversion function
                        conversion_fn.map(|f| f(value))
                    })
                })
                .flatten(),
        );

        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}
