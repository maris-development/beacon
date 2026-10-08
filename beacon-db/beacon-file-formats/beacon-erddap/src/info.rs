//! `/info/<id>/index.json`: the variables of a dataset, their types and attributes.
//!
//! The table schema carries every ERDDAP attribute as Arrow metadata: the global
//! attributes on the schema, the variable attributes on each field. The `comment`
//! key holds Beacon's comment (table: `title`; column: `long_name` and `units`).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{Context, anyhow, bail};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

/// The units ERDDAP gives every time variable.
pub const EPOCH_SECONDS_UNITS: &str = "seconds since 1970-01-01T00:00:00Z";

/// The metadata key that Beacon reads as a table or column comment.
pub const COMMENT_KEY: &str = "comment";

/// The metadata key for an ERDDAP attribute named `comment`.
pub const ERDDAP_COMMENT_KEY: &str = "erddap_comment";

/// The owner name ERDDAP gives the global attributes.
const GLOBAL_OWNER: &str = "NC_GLOBAL";

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
    /// Every attribute of the variable, as ERDDAP gives it.
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
}

impl VariableInfo {
    /// True when the variable holds time as seconds since the epoch.
    pub fn is_time(&self) -> bool {
        self.units.as_deref() == Some(EPOCH_SECONDS_UNITS)
    }

    /// The column comment: `long_name`, then `units` in parentheses.
    ///
    /// A time column shows no units: Beacon reads it as a timestamp.
    pub fn comment(&self) -> Option<String> {
        let long_name = self.attributes.get("long_name").filter(|s| !s.is_empty());
        let units = self
            .units
            .as_ref()
            .filter(|s| !s.is_empty() && !self.is_time());
        match (long_name, units) {
            (Some(name), Some(units)) => Some(format!("{name} ({units})")),
            (Some(name), None) => Some(name.clone()),
            (None, Some(units)) => Some(units.clone()),
            (None, None) => None,
        }
    }
}

/// The variables and global attributes of an ERDDAP dataset.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DatasetInfo {
    /// In info order: axes in dimension order, then data variables.
    pub variables: Vec<VariableInfo>,
    /// The global (`NC_GLOBAL`) attributes.
    #[serde(default)]
    pub global_attributes: BTreeMap<String, String>,
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
        let mut global_attributes = BTreeMap::new();
        for row in &parsed.table.rows {
            let kind = match text(row, 0).as_str() {
                "dimension" => VariableKind::Axis,
                "variable" => VariableKind::Data,
                "attribute" => {
                    let (owner, attribute, value) = (text(row, 1), text(row, 2), text(row, 4));
                    if owner == GLOBAL_OWNER {
                        global_attributes.insert(attribute, value);
                    } else if let Some(v) = variables.iter_mut().find(|v| v.name == owner) {
                        if attribute == "units" {
                            v.units = Some(value.clone());
                        }
                        v.attributes.insert(attribute, value);
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
                attributes: BTreeMap::new(),
            });
        }
        if variables.is_empty() {
            bail!("ERDDAP info response lists no variables");
        }
        Ok(Self {
            variables,
            global_attributes,
        })
    }

    /// The table comment: the `title` attribute.
    pub fn comment(&self) -> Option<String> {
        self.global_attributes
            .get("title")
            .filter(|s| !s.is_empty())
            .cloned()
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

    /// One nullable column per variable, in info order, with the attributes as metadata.
    pub fn tabledap_schema(&self) -> anyhow::Result<SchemaRef> {
        let fields = self
            .data_variables()
            .into_iter()
            .map(|v| {
                Ok(
                    Field::new(&v.name, arrow_type(&v.data_type, v.units.as_deref())?, true)
                        .with_metadata(metadata(&v.attributes, v.comment())),
                )
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Arc::new(Schema::new_with_metadata(
            fields,
            metadata(&self.global_attributes, self.comment()),
        )))
    }
}

/// ERDDAP attributes as Arrow metadata, with Beacon's `comment`.
fn metadata(
    attributes: &BTreeMap<String, String>,
    comment: Option<String>,
) -> HashMap<String, String> {
    let mut metadata: HashMap<String, String> = attributes
        .iter()
        .map(|(key, value)| {
            // `comment` belongs to Beacon, so ERDDAP's own comment moves aside.
            let key = if key == COMMENT_KEY {
                ERDDAP_COMMENT_KEY
            } else {
                key
            };
            (key.to_string(), value.clone())
        })
        .collect();
    if let Some(comment) = comment {
        metadata.insert(COMMENT_KEY.to_string(), comment);
    }
    metadata
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
    fn schema_carries_attributes_and_comments() {
        let info = DatasetInfo::parse(&fixture("tabledap_info.json")).unwrap();
        let schema = info.tabledap_schema().unwrap();

        let global = schema.metadata();
        assert_eq!(
            global.get(COMMENT_KEY).map(String::as_str),
            Some("GLOBEC NEP Rosette Bottle Data (2002)")
        );
        assert!(
            global
                .get("summary")
                .is_some_and(|s| s.starts_with("GLOBEC"))
        );
        assert_eq!(global.len(), info.global_attributes.len() + 1);

        let temperature = schema.field_with_name("temperature0").unwrap().metadata();
        assert_eq!(
            temperature.get(COMMENT_KEY).map(String::as_str),
            Some("Sea Water Temperature from T0 Sensor (degree_C)")
        );
        assert_eq!(
            temperature.get("standard_name").map(String::as_str),
            Some("sea_water_temperature")
        );
        assert_eq!(
            temperature.get("actual_range").map(String::as_str),
            Some("3.6186, 16.871")
        );

        // A time column shows no epoch units: Beacon reads it as a timestamp.
        let time = schema.field_with_name("time").unwrap().metadata();
        assert_eq!(time.get(COMMENT_KEY).map(String::as_str), Some("Time"));
        assert_eq!(
            time.get("units").map(String::as_str),
            Some(EPOCH_SECONDS_UNITS)
        );
    }

    #[test]
    fn an_erddap_comment_attribute_moves_aside() {
        let json = br#"{"table":{"rows":[
            ["attribute","NC_GLOBAL","comment","String","global note"],
            ["variable","depth","","int",""],
            ["attribute","depth","comment","String","depth note"],
            ["attribute","depth","units","String","m"]
        ]}}"#;
        let schema = DatasetInfo::parse(json).unwrap().tabledap_schema().unwrap();

        let global = schema.metadata();
        assert_eq!(global.get(COMMENT_KEY), None, "no title, no table comment");
        assert_eq!(
            global.get(ERDDAP_COMMENT_KEY).map(String::as_str),
            Some("global note")
        );

        let depth = schema.field_with_name("depth").unwrap().metadata();
        assert_eq!(depth.get(COMMENT_KEY).map(String::as_str), Some("m"));
        assert_eq!(
            depth.get(ERDDAP_COMMENT_KEY).map(String::as_str),
            Some("depth note")
        );
    }

    #[test]
    fn info_without_attributes_still_deserializes() {
        let json = r#"{"variables":[{"name":"a","kind":"Data","data_type":"int","units":null}]}"#;
        let info: DatasetInfo = serde_json::from_str(json).unwrap();
        assert!(info.global_attributes.is_empty());
        assert!(info.variables[0].attributes.is_empty());
    }

    #[test]
    fn rejects_non_info_json() {
        assert!(DatasetInfo::parse(br#"{"x":1}"#).is_err());
    }
}
