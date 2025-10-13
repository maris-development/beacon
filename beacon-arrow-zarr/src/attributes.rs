use std::sync::Arc;

use nd_arrow_array::NdArrowArray;

pub enum AttributeValue {
    String(String),
    Float64(f64),
    Bool(bool),
}

impl AttributeValue {
    pub fn from_json_value(value: &serde_json::Value) -> Option<Self> {
        match value {
            serde_json::Value::String(s) => Some(AttributeValue::String(s.clone())),
            serde_json::Value::Number(n) => n.as_f64().map(AttributeValue::Float64),
            serde_json::Value::Bool(b) => Some(AttributeValue::Bool(*b)),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            AttributeValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            AttributeValue::Float64(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            AttributeValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn arrow_data_type(&self) -> arrow::datatypes::DataType {
        match self {
            AttributeValue::String(_) => arrow::datatypes::DataType::Utf8,
            AttributeValue::Float64(_) => arrow::datatypes::DataType::Float64,
            AttributeValue::Bool(_) => arrow::datatypes::DataType::Boolean,
        }
    }

    pub fn as_nd_arrow_array(&self) -> NdArrowArray {
        match self {
            AttributeValue::String(s) => {
                let array = arrow::array::StringArray::from(vec![s.as_str()]);
                NdArrowArray::new(
                    Arc::new(array) as arrow::array::ArrayRef,
                    nd_arrow_array::dimensions::Dimensions::Scalar,
                )
                .unwrap()
            }
            AttributeValue::Float64(f) => {
                let array = arrow::array::Float64Array::from(vec![*f]);
                NdArrowArray::new(
                    Arc::new(array) as arrow::array::ArrayRef,
                    nd_arrow_array::dimensions::Dimensions::Scalar,
                )
                .unwrap()
            }
            AttributeValue::Bool(b) => {
                let array = arrow::array::BooleanArray::from(vec![*b]);
                NdArrowArray::new(
                    Arc::new(array) as arrow::array::ArrayRef,
                    nd_arrow_array::dimensions::Dimensions::Scalar,
                )
                .unwrap()
            }
        }
    }
}
