//! Fold `variable.attribute` columns into NetCDF attributes on write.
//!
//! The readers show a variable attribute as the column `var.attr` and a global
//! attribute as `.attr`. With [`CombineAttrs`] set, the flat writer writes a
//! column with one unique value as a NetCDF attribute.

use std::collections::HashSet;

use arrow::datatypes::{DataType, Schema};

/// How the flat NetCDF writer combines the values of one attribute column.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum CombineAttrs {
    /// One unique value becomes an attribute. More values stay a variable on `obs`.
    Identical,
    /// The first non-null value becomes an attribute. The writer drops the other values.
    First,
    /// One unique value becomes an attribute. The writer drops a column with more values.
    DropConflicts,
}

impl std::str::FromStr for CombineAttrs {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "identical" => Ok(Self::Identical),
            "first" => Ok(Self::First),
            "drop_conflicts" => Ok(Self::DropConflicts),
            other => Err(format!(
                "invalid combine_attrs '{other}': expected identical, first or drop_conflicts"
            )),
        }
    }
}

/// The NetCDF attribute that one attribute column describes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttributeTarget {
    /// Attribute `name` on the variable `variable`.
    Variable { variable: String, name: String },
    /// Global attribute `name`.
    Global { name: String },
}

impl AttributeTarget {
    /// The host variable, or `None` for a global attribute.
    pub fn variable(&self) -> Option<&str> {
        match self {
            Self::Variable { variable, .. } => Some(variable),
            Self::Global { .. } => None,
        }
    }

    /// The attribute name.
    pub fn name(&self) -> &str {
        match self {
            Self::Variable { name, .. } | Self::Global { name } => name,
        }
    }
}

/// Attributes that use the packed type of a packed variable (CF §8.1).
const PACKED_TYPE: [&str; 4] = ["valid_min", "valid_max", "valid_range", "missing_value"];
/// Attributes the encoder writes itself on a timestamp variable.
const TIME_OWNED: [&str; 2] = ["units", "calendar"];

/// The attribute columns of one schema.
#[derive(Debug, Default)]
pub struct Classification {
    /// Column name and attribute target, in schema order.
    pub attributes: Vec<(String, AttributeTarget)>,
    /// Attribute columns that the writer always drops.
    pub dropped: HashSet<String>,
}

/// Find the attribute columns of `schema`.
pub fn classify(schema: &Schema) -> Classification {
    let names: HashSet<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let mut candidates: Vec<(String, AttributeTarget)> = Vec::new();
    for field in schema.fields() {
        let column = field.name();
        if let Some(name) = column.strip_prefix('.') {
            if !name.is_empty() {
                candidates.push((column.clone(), AttributeTarget::Global { name: name.to_string() }));
            }
            continue;
        }
        // An attribute name has no dot, so the last dot ends the variable name.
        if let Some((variable, name)) = column.rsplit_once('.') {
            if !name.is_empty() && names.contains(variable) {
                candidates.push((
                    column.clone(),
                    AttributeTarget::Variable {
                        variable: variable.to_string(),
                        name: name.to_string(),
                    },
                ));
            }
        }
    }

    let attribute_columns: HashSet<String> = candidates.iter().map(|(c, _)| c.clone()).collect();
    candidates.retain(|(_, target)| target.variable().is_none_or(|v| !attribute_columns.contains(v)));

    let packed: HashSet<String> = candidates
        .iter()
        .filter(|(_, target)| matches!(target.name(), "scale_factor" | "add_offset"))
        .filter_map(|(_, target)| target.variable().map(str::to_string))
        .collect();

    let mut classification = Classification::default();
    for (column, target) in candidates {
        if is_dropped(&target, schema, &packed) {
            classification.dropped.insert(column);
        } else {
            classification.attributes.push((column, target));
        }
    }
    classification
}

/// Whether the writer drops this attribute, because the reader decoded it into the data.
fn is_dropped(target: &AttributeTarget, schema: &Schema, packed: &HashSet<String>) -> bool {
    let name = target.name();
    if name.starts_with('_') || name == "scale_factor" || name == "add_offset" {
        return true;
    }
    let Some(variable) = target.variable() else {
        return false;
    };
    if packed.contains(variable) && PACKED_TYPE.contains(&name) {
        return true;
    }
    let is_time = schema
        .field_with_name(variable)
        .is_ok_and(|f| matches!(f.data_type(), DataType::Timestamp(_, _)));
    is_time && TIME_OWNED.contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema, TimeUnit};

    fn schema(fields: &[(&str, DataType)]) -> Schema {
        Schema::new(
            fields
                .iter()
                .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
                .collect::<Vec<_>>(),
        )
    }

    fn target_of<'a>(classification: &'a Classification, column: &str) -> Option<&'a AttributeTarget> {
        classification
            .attributes
            .iter()
            .find(|(name, _)| name == column)
            .map(|(_, target)| target)
    }

    fn variable(variable: &str, name: &str) -> AttributeTarget {
        AttributeTarget::Variable { variable: variable.to_string(), name: name.to_string() }
    }

    #[test]
    fn a_dotted_column_with_a_host_is_a_variable_attribute() {
        let c = classify(&schema(&[("temp", DataType::Float64), ("temp.units", DataType::Utf8)]));
        assert_eq!(target_of(&c, "temp.units"), Some(&variable("temp", "units")));
    }

    #[test]
    fn a_dotted_column_without_a_host_is_a_plain_column() {
        let c = classify(&schema(&[("temp.units", DataType::Utf8)]));
        assert!(c.attributes.is_empty());
        assert!(c.dropped.is_empty());
    }

    #[test]
    fn a_leading_dot_column_is_a_global_attribute() {
        let c = classify(&schema(&[("temp", DataType::Float64), (".title", DataType::Utf8)]));
        assert_eq!(
            target_of(&c, ".title"),
            Some(&AttributeTarget::Global { name: "title".to_string() })
        );
    }

    #[test]
    fn the_last_dot_ends_the_variable_name() {
        let c = classify(&schema(&[
            ("sea.temp", DataType::Float64),
            ("sea.temp.units", DataType::Utf8),
        ]));
        assert_eq!(target_of(&c, "sea.temp.units"), Some(&variable("sea.temp", "units")));
    }

    #[test]
    fn an_attribute_column_is_no_host() {
        let c = classify(&schema(&[
            ("temp", DataType::Float64),
            ("temp.units", DataType::Utf8),
            ("temp.units.x", DataType::Utf8),
        ]));
        assert_eq!(target_of(&c, "temp.units"), Some(&variable("temp", "units")));
        assert_eq!(target_of(&c, "temp.units.x"), None);
        assert!(!c.dropped.contains("temp.units.x"));
    }

    #[test]
    fn reserved_and_packed_attributes_are_dropped() {
        let c = classify(&schema(&[
            ("temp", DataType::Float64),
            ("temp._FillValue", DataType::Float64),
            ("temp.scale_factor", DataType::Float64),
            ("temp.add_offset", DataType::Float64),
            ("temp.valid_min", DataType::Float64),
            ("temp.missing_value", DataType::Float64),
            ("._NCProperties", DataType::Utf8),
        ]));
        for column in [
            "temp._FillValue",
            "temp.scale_factor",
            "temp.add_offset",
            "temp.valid_min",
            "temp.missing_value",
            "._NCProperties",
        ] {
            assert!(c.dropped.contains(column), "{column} is dropped");
            assert_eq!(target_of(&c, column), None, "{column} is no attribute");
        }
    }

    #[test]
    fn valid_min_of_an_unpacked_variable_is_kept() {
        let c = classify(&schema(&[("temp", DataType::Float64), ("temp.valid_min", DataType::Float64)]));
        assert_eq!(target_of(&c, "temp.valid_min"), Some(&variable("temp", "valid_min")));
    }

    #[test]
    fn time_units_on_a_timestamp_are_dropped() {
        let c = classify(&schema(&[
            ("time", DataType::Timestamp(TimeUnit::Nanosecond, None)),
            ("time.units", DataType::Utf8),
            ("time.calendar", DataType::Utf8),
            ("time.long_name", DataType::Utf8),
        ]));
        assert!(c.dropped.contains("time.units"));
        assert!(c.dropped.contains("time.calendar"));
        assert_eq!(target_of(&c, "time.long_name"), Some(&variable("time", "long_name")));
    }

    #[test]
    fn combine_attrs_parses_without_case() {
        assert_eq!("Identical".parse::<CombineAttrs>(), Ok(CombineAttrs::Identical));
        assert_eq!("first".parse::<CombineAttrs>(), Ok(CombineAttrs::First));
        assert_eq!(" DROP_CONFLICTS ".parse::<CombineAttrs>(), Ok(CombineAttrs::DropConflicts));
        let error = "union".parse::<CombineAttrs>().unwrap_err();
        assert!(error.contains("combine_attrs"), "{error}");
    }
}
