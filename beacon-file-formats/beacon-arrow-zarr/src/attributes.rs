//! Minimal model of zarr JSON attribute values supported by Beacon.

/// A scalar zarr attribute value parsed from JSON.
#[derive(Debug, Clone)]
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
}
