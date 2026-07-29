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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_scalar_json_values() {
        assert!(matches!(
            AttributeValue::from_json_value(&json!("hello")),
            Some(AttributeValue::String(s)) if s == "hello"
        ));
        assert!(matches!(
            AttributeValue::from_json_value(&json!(true)),
            Some(AttributeValue::Bool(true))
        ));
        // Both integers and floats collapse to Float64.
        assert_eq!(
            AttributeValue::from_json_value(&json!(42)).and_then(|a| a.as_f64()),
            Some(42.0)
        );
        assert_eq!(
            AttributeValue::from_json_value(&json!(1.5)).and_then(|a| a.as_f64()),
            Some(1.5)
        );
    }

    #[test]
    fn rejects_non_scalar_json_values() {
        // Arrays, objects and null are not representable scalar attributes.
        assert!(AttributeValue::from_json_value(&json!([1, 2, 3])).is_none());
        assert!(AttributeValue::from_json_value(&json!({"a": 1})).is_none());
        assert!(AttributeValue::from_json_value(&serde_json::Value::Null).is_none());
    }

    #[test]
    fn accessors_are_type_specific() {
        let s = AttributeValue::String("x".to_string());
        assert_eq!(s.as_str(), Some("x"));
        assert_eq!(s.as_f64(), None);
        assert_eq!(s.as_bool(), None);

        let f = AttributeValue::Float64(2.0);
        assert_eq!(f.as_f64(), Some(2.0));
        assert_eq!(f.as_str(), None);

        let b = AttributeValue::Bool(false);
        assert_eq!(b.as_bool(), Some(false));
        assert_eq!(b.as_f64(), None);
    }
}
