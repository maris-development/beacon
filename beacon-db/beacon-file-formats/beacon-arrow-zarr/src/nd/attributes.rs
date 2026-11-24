//! Conversion of Zarr JSON attributes to ND framework attributes.

use arrow::datatypes::DataType;
use nd_arrow_array::{Attributes, IntoScalar};
use serde_json::{Map, Value};

/// The CF keys that [`decode_attributes`] keeps in the stored integer type.
const EXACT_CF_KEYS: [&str; 5] = [
    "_FillValue",
    "missing_value",
    "valid_min",
    "valid_max",
    "valid_range",
];

/// The attributes of a Zarr array or group.
///
/// A string is `Utf8`, a number is `Float64`, a bool is `Boolean` and an
/// array of numbers is a `List<Float64>`. Other values are not kept.
pub(crate) fn attributes_of(json: &Map<String, Value>) -> Attributes {
    let mut attrs = Attributes::new();
    for (name, value) in json {
        match value {
            Value::String(text) => attrs.insert(name.as_str(), text.as_str()),
            Value::Number(number) => {
                if let Some(number) = number.as_f64() {
                    attrs.insert(name.as_str(), number);
                }
            }
            Value::Bool(flag) => attrs.insert(name.as_str(), *flag),
            Value::Array(items) => {
                if let Some(numbers) = items.iter().map(Value::as_f64).collect::<Option<Vec<_>>>() {
                    attrs.insert(name.as_str(), numbers);
                }
            }
            Value::Null | Value::Object(_) => {}
        }
    }
    attrs
}

/// The attributes for the CF decode of a variable stored as `stored`.
///
/// These are the attributes of [`attributes_of`]. For an integer `stored`
/// type, the CF keys of [`EXACT_CF_KEYS`] hold the exact JSON integers in
/// type `stored`. A value that is not an integer or does not fit stays `Float64`.
pub(crate) fn decode_attributes(json: &Map<String, Value>, stored: &DataType) -> Attributes {
    let mut attrs = attributes_of(json);
    for name in EXACT_CF_KEYS {
        let Some(value) = json.get(name) else {
            continue;
        };
        match stored {
            DataType::Int8 => insert_exact::<i8>(&mut attrs, name, value),
            DataType::Int16 => insert_exact::<i16>(&mut attrs, name, value),
            DataType::Int32 => insert_exact::<i32>(&mut attrs, name, value),
            DataType::Int64 => insert_exact::<i64>(&mut attrs, name, value),
            DataType::UInt8 => insert_exact::<u8>(&mut attrs, name, value),
            DataType::UInt16 => insert_exact::<u16>(&mut attrs, name, value),
            DataType::UInt32 => insert_exact::<u32>(&mut attrs, name, value),
            DataType::UInt64 => insert_exact::<u64>(&mut attrs, name, value),
            _ => return attrs,
        }
    }
    attrs
}

/// Replace attribute `name` with `value` in type `T` when each number fits `T`.
fn insert_exact<T>(attrs: &mut Attributes, name: &str, value: &Value)
where
    T: TryFrom<i64> + TryFrom<u64> + IntoScalar,
    Vec<T>: IntoScalar,
{
    match value {
        Value::Number(_) => {
            if let Some(number) = exact::<T>(value) {
                attrs.insert(name, number);
            }
        }
        Value::Array(items) => {
            if let Some(numbers) = items.iter().map(exact::<T>).collect::<Option<Vec<_>>>() {
                attrs.insert(name, numbers);
            }
        }
        _ => {}
    }
}

/// The JSON integer `value` as a `T`, or `None` when it is not an integer or does not fit.
fn exact<T: TryFrom<i64> + TryFrom<u64>>(value: &Value) -> Option<T> {
    match value.as_i64() {
        Some(signed) => T::try_from(signed).ok(),
        None => T::try_from(value.as_u64()?).ok(),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Field, Float64Type, Int64Type, UInt64Type};

    use super::*;

    fn json(text: &str) -> Map<String, Value> {
        serde_json::from_str(text).unwrap()
    }

    fn float64(attrs: &Attributes, name: &str) -> f64 {
        attrs.get_primitive::<Float64Type>(name).unwrap()
    }

    #[test]
    fn a_string_a_number_and_a_bool_keep_their_scalar_type() {
        let attrs = attributes_of(&json(r#"{"long_name":"t","scale_factor":2,"flag":true}"#));

        assert_eq!(attrs.len(), 3);
        assert_eq!(attrs.get_str("long_name"), Some("t"));
        assert_eq!(
            attrs.get("scale_factor").unwrap().data_type(),
            &DataType::Float64
        );
        assert_eq!(float64(&attrs, "scale_factor"), 2.0);
        let flag = attrs.get("flag").unwrap();
        assert_eq!(flag.data_type(), &DataType::Boolean);
        assert!(flag.as_boolean().value(0));
    }

    #[test]
    fn an_array_of_numbers_is_a_float64_list() {
        let attrs = attributes_of(&json(r#"{"valid_range":[0,10]}"#));

        let range = attrs.get("valid_range").unwrap();
        assert_eq!(
            range.data_type(),
            &DataType::List(Field::new("item", DataType::Float64, true).into())
        );
        assert_eq!(range.len(), 1);
        let items = range.as_list::<i32>().value(0);
        assert_eq!(items.as_primitive::<Float64Type>().values(), &[0.0, 10.0]);
    }

    #[test]
    fn null_object_and_mixed_array_are_not_kept() {
        let attrs = attributes_of(&json(r#"{"a":null,"b":{"x":1},"c":[1,"x"]}"#));

        assert!(attrs.is_empty());
    }

    #[test]
    fn an_int64_fill_is_exact_for_the_decode() {
        let fill = json(r#"{"_FillValue": 9223372036854775807}"#);

        let decode = decode_attributes(&fill, &DataType::Int64);
        assert_eq!(
            decode.get_primitive::<Int64Type>("_FillValue"),
            Some(i64::MAX)
        );
        let plain = attributes_of(&fill);
        assert_eq!(
            plain.get("_FillValue").unwrap().data_type(),
            &DataType::Float64
        );
    }

    #[test]
    fn a_uint64_fill_is_exact_for_the_decode() {
        let fill = json(r#"{"_FillValue": 18446744073709551615}"#);

        let decode = decode_attributes(&fill, &DataType::UInt64);
        assert_eq!(
            decode.get_primitive::<UInt64Type>("_FillValue"),
            Some(u64::MAX)
        );
    }

    #[test]
    fn a_fill_that_does_not_fit_stays_float64() {
        let too_large = decode_attributes(&json(r#"{"_FillValue": 40000}"#), &DataType::Int16);
        assert_eq!(float64(&too_large, "_FillValue"), 40000.0);

        let fraction = decode_attributes(&json(r#"{"_FillValue": 1.5}"#), &DataType::Int32);
        assert_eq!(float64(&fraction, "_FillValue"), 1.5);
    }

    #[test]
    fn a_float_variable_keeps_float64_cf_keys() {
        let attrs = decode_attributes(&json(r#"{"_FillValue": 7}"#), &DataType::Float32);

        assert_eq!(float64(&attrs, "_FillValue"), 7.0);
    }

    #[test]
    fn other_keys_stay_float64_for_an_integer_variable() {
        let attrs = decode_attributes(&json(r#"{"scale_factor": 2}"#), &DataType::Int16);

        assert_eq!(float64(&attrs, "scale_factor"), 2.0);
    }
}
