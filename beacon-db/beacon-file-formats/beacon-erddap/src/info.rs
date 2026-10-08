//! `/info/<id>/index.json`: the variables of a dataset, their types and units.

use std::sync::Arc;

use anyhow::{Context, anyhow, bail};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

/// The units ERDDAP gives every time variable.
pub const EPOCH_SECONDS_UNITS: &str = "seconds since 1970-01-01T00:00:00Z";

/// The role of a variable in a dataset.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum VariableKind {
    /// A griddap axis (`dimension` row).
    Axis,
    /// A tabledap column or a griddap data variable.
    Data,
}

/// One variable of an ERDDAP dataset.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VariableInfo {
    /// The variable name.
    pub name: String,
    /// Whether the variable is an axis or a data variable.
    pub kind: VariableKind,
    /// The ERDDAP type name, e.g. `double` or `String`.
    pub data_type: String,
    /// The `units` attribute, if the variable has one.
    pub units: Option<String>,
}

impl VariableInfo {
    /// True when the variable holds time as seconds since the epoch.
    pub fn is_time(&self) -> bool {
        self.units.as_deref() == Some(EPOCH_SECONDS_UNITS)
    }
}

/// The variables of an ERDDAP dataset.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DatasetInfo {
    /// In info order: axes in dimension order, then data variables.
    pub variables: Vec<VariableInfo>,
}

#[derive(serde::Deserialize)]
struct InfoJson {
    table: InfoTable,
}

#[derive(serde::Deserialize)]
struct InfoTable {
    rows: Vec<Vec<serde_json::Value>>,
}

impl DatasetInfo {
    /// Parse the body of an `/info/<id>/index.json` response.
    pub fn parse(json: &[u8]) -> anyhow::Result<Self> {
        let parsed: InfoJson =
            serde_json::from_slice(json).context("ERDDAP info response is not an info table")?;
        let text = |row: &[serde_json::Value], i: usize| {
            row.get(i)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string()
        };
        let mut variables: Vec<VariableInfo> = Vec::new();
        for row in &parsed.table.rows {
            let kind = match text(row, 0).as_str() {
                "dimension" => VariableKind::Axis,
                "variable" => VariableKind::Data,
                "attribute" => {
                    let (owner, attribute) = (text(row, 1), text(row, 2));
                    if attribute == "units"
                        && let Some(v) = variables.iter_mut().find(|v| v.name == owner)
                    {
                        v.units = Some(text(row, 4));
                    }
                    continue;
                }
                _ => continue,
            };
            variables.push(VariableInfo {
                name: text(row, 1),
                kind,
                data_type: text(row, 3),
                units: None,
            });
        }
        if variables.is_empty() {
            bail!("ERDDAP info response lists no variables");
        }
        Ok(Self { variables })
    }

    /// The axes in dimension order.
    pub fn axes(&self) -> Vec<&VariableInfo> {
        self.variables
            .iter()
            .filter(|v| v.kind == VariableKind::Axis)
            .collect()
    }

    /// The data variables in info order.
    pub fn data_variables(&self) -> Vec<&VariableInfo> {
        self.variables
            .iter()
            .filter(|v| v.kind == VariableKind::Data)
            .collect()
    }

    /// Find a variable by name.
    pub fn variable(&self, name: &str) -> Option<&VariableInfo> {
        self.variables.iter().find(|v| v.name == name)
    }

    /// One nullable column per variable, in info order.
    pub fn tabledap_schema(&self) -> anyhow::Result<SchemaRef> {
        let fields = self
            .data_variables()
            .into_iter()
            .map(|v| {
                Ok(Field::new(
                    &v.name,
                    arrow_type(&v.data_type, v.units.as_deref())?,
                    true,
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Arc::new(Schema::new(fields)))
    }
}

/// The Arrow type of an ERDDAP variable.
pub fn arrow_type(erddap_type: &str, units: Option<&str>) -> anyhow::Result<DataType> {
    if units == Some(EPOCH_SECONDS_UNITS) {
        return Ok(DataType::Timestamp(TimeUnit::Nanosecond, None));
    }
    Ok(match erddap_type {
        "byte" => DataType::Int8,
        "ubyte" => DataType::UInt8,
        "short" => DataType::Int16,
        "ushort" => DataType::UInt16,
        "int" => DataType::Int32,
        "uint" => DataType::UInt32,
        "long" => DataType::Int64,
        "ulong" => DataType::UInt64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "char" | "String" => DataType::Utf8,
        "boolean" => DataType::Boolean,
        other => return Err(anyhow!("unsupported ERDDAP data type '{other}'")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, TimeUnit};

    fn fixture(name: &str) -> Vec<u8> {
        std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("test-files")
                .join(name),
        )
        .unwrap()
    }

    #[test]
    fn maps_erddap_types() {
        assert_eq!(arrow_type("byte", None).unwrap(), DataType::Int8);
        assert_eq!(arrow_type("ushort", None).unwrap(), DataType::UInt16);
        assert_eq!(arrow_type("long", None).unwrap(), DataType::Int64);
        assert_eq!(arrow_type("float", None).unwrap(), DataType::Float32);
        assert_eq!(arrow_type("String", None).unwrap(), DataType::Utf8);
        assert_eq!(arrow_type("char", None).unwrap(), DataType::Utf8);
        assert_eq!(
            arrow_type("double", Some(EPOCH_SECONDS_UNITS)).unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert!(arrow_type("complex", None).is_err());
    }

    #[test]
    fn parses_tabledap_info_in_order() {
        let info = DatasetInfo::parse(&fixture("tabledap_info.json")).unwrap();
        assert!(info.axes().is_empty());
        let schema = info.tabledap_schema().unwrap();
        assert_eq!(schema.fields().len(), info.data_variables().len());
        let time = schema.field_with_name("time").unwrap();
        assert_eq!(
            time.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert!(schema.fields().iter().all(|f| f.is_nullable()));
    }

    #[test]
    fn parses_griddap_axes_in_dimension_order() {
        let info = DatasetInfo::parse(&fixture("griddap_info.json")).unwrap();
        let axes: Vec<&str> = info.axes().iter().map(|v| v.name.as_str()).collect();
        assert_eq!(axes, ["time", "latitude", "longitude"]);
        assert!(info.variable("sst").is_some());
        assert!(info.axes()[0].is_time());
    }

    #[test]
    fn rejects_non_info_json() {
        assert!(DatasetInfo::parse(br#"{"x":1}"#).is_err());
    }
}
