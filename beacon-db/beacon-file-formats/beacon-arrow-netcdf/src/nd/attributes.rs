//! Conversion from [`oxcdf`] attributes to ND framework [`Attributes`].

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    ListArray, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::Field;
use nd_arrow_array::Attributes;
use oxcdf::{AttributeValue, NcAttribute};

/// Convert NetCDF attributes to [`Attributes`], in input order.
///
/// A scalar becomes a length-1 array of the matching Arrow type. A plural
/// value with one item is the same scalar. A plural value with more items is
/// a length-1 `List`. A value with no Arrow type is left out.
pub fn attributes_of<'a>(attributes: impl IntoIterator<Item = &'a NcAttribute>) -> Attributes {
    let mut result = Attributes::new();
    for attribute in attributes {
        let Some(value) = value_array(&attribute.value) else {
            tracing::debug!(
                attribute = %attribute.name,
                "skip a NetCDF attribute with no Arrow type"
            );
            continue;
        };
        if let Err(error) = result.try_insert_array(attribute.name.as_str(), value) {
            tracing::debug!(attribute = %attribute.name, %error, "skip a NetCDF attribute");
        }
    }
    result
}

/// The length-1 Arrow array for one attribute value.
fn value_array(value: &AttributeValue) -> Option<ArrayRef> {
    macro_rules! convert {
        ($($single:ident / $plural:ident => $array:ty),* $(,)?) => {
            match value {
                $(
                    AttributeValue::$single(item) => {
                        Some(Arc::new(<$array>::from(vec![item.clone()])) as ArrayRef)
                    }
                    AttributeValue::$plural(items) => {
                        Some(one_row(Arc::new(<$array>::from(items.clone()))))
                    }
                )*
                _ => None,
            }
        };
    }

    convert!(
        Uchar / Uchars => UInt8Array,
        Schar / Schars => Int8Array,
        Ushort / Ushorts => UInt16Array,
        Short / Shorts => Int16Array,
        Uint / Uints => UInt32Array,
        Int / Ints => Int32Array,
        Ulonglong / Ulonglongs => UInt64Array,
        Longlong / Longlongs => Int64Array,
        Float / Floats => Float32Array,
        Double / Doubles => Float64Array,
        Str / Strs => StringArray,
    )
}

/// One item stays a scalar; more items become one `List` row.
fn one_row(values: ArrayRef) -> ArrayRef {
    if values.len() == 1 {
        return values;
    }
    let field = Arc::new(Field::new_list_field(values.data_type().clone(), true));
    let offsets = OffsetBuffer::from_lengths([values.len()]);
    Arc::new(ListArray::new(field, offsets, values, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, Float64Type, Int16Type};

    fn attribute(name: &str, value: AttributeValue) -> NcAttribute {
        NcAttribute {
            name: name.to_string(),
            value,
        }
    }

    #[test]
    fn a_scalar_attribute_is_a_length_one_array() {
        let input = [
            attribute("scale", AttributeValue::Double(1.5)),
            attribute("units", AttributeValue::Str("m".to_string())),
        ];
        let attributes = attributes_of(&input);

        let scale = attributes.get("scale").unwrap();
        assert_eq!(scale.data_type(), &DataType::Float64);
        assert_eq!(scale.len(), 1);
        assert_eq!(scale.as_primitive::<Float64Type>().value(0), 1.5);

        let units = attributes.get("units").unwrap();
        assert_eq!(units.data_type(), &DataType::Utf8);
        assert_eq!(units.len(), 1);
        assert_eq!(units.as_string::<i32>().value(0), "m");
    }

    #[test]
    fn a_one_item_list_is_a_scalar() {
        let input = [attribute("fill", AttributeValue::Shorts(vec![7]))];
        let attributes = attributes_of(&input);

        let fill = attributes.get("fill").unwrap();
        assert_eq!(fill.data_type(), &DataType::Int16);
        assert_eq!(fill.len(), 1);
        assert_eq!(fill.as_primitive::<Int16Type>().value(0), 7);
    }

    #[test]
    fn a_list_attribute_is_a_list_of_one_row() {
        let input = [attribute(
            "missing_value",
            AttributeValue::Shorts(vec![-1, -2]),
        )];
        let attributes = attributes_of(&input);

        let missing = attributes.get("missing_value").unwrap();
        assert!(
            matches!(missing.data_type(), DataType::List(field) if field.data_type() == &DataType::Int16)
        );
        assert_eq!(missing.len(), 1);
        let row = missing.as_list::<i32>().value(0);
        assert_eq!(row.as_primitive::<Int16Type>().values(), &[-1, -2]);
    }

    #[test]
    fn the_attribute_order_is_the_file_order() {
        let input = [
            attribute("zeta", AttributeValue::Int(1)),
            attribute("alpha", AttributeValue::Float(2.0)),
            attribute("raw", AttributeValue::Raw(vec![0, 1])),
            attribute("mid", AttributeValue::Uchars(vec![1, 2, 3])),
        ];
        let attributes = attributes_of(&input);

        let names: Vec<&str> = attributes.iter().map(|(name, _)| name).collect();
        assert_eq!(names, ["zeta", "alpha", "mid"]);
    }
}
