//! Fold `variable.attribute` columns into NetCDF attributes on write.
//!
//! The readers show a variable attribute as the column `var.attr` and a global
//! attribute as `.attr`. With [`CombineAttrs`] set, the flat writer writes a
//! column with one unique value as a NetCDF attribute.

use std::collections::{HashMap, HashSet};

use arrow::{
    array::{Array, ArrayRef, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use datafusion::common::ScalarValue;
use netcdf::AttributeValue;

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
/// The fill value attribute. The encoder sets it when it defines the variable.
pub const FILL_VALUE: &str = "_FillValue";

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
                candidates.push((
                    column.clone(),
                    AttributeTarget::Global {
                        name: name.to_string(),
                    },
                ));
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
    candidates.retain(|(_, target)| {
        target
            .variable()
            .is_none_or(|v| !attribute_columns.contains(v))
    });

    let packed: HashSet<String> = candidates
        .iter()
        .filter(|(_, target)| matches!(target.name(), "scale_factor" | "add_offset"))
        .filter_map(|(_, target)| target.variable().map(str::to_string))
        .collect();

    let mut classification = Classification::default();
    for (column, target) in candidates {
        if is_dropped(&column, &target, schema, &packed) {
            classification.dropped.insert(column);
        } else {
            classification.attributes.push((column, target));
        }
    }
    classification
}

/// Whether the writer drops this attribute, because the reader decoded it into the data.
fn is_dropped(
    column: &str,
    target: &AttributeTarget,
    schema: &Schema,
    packed: &HashSet<String>,
) -> bool {
    let name = target.name();
    let data_type = |name: &str| schema.field_with_name(name).ok().map(Field::data_type);
    if name == FILL_VALUE {
        // The writer uses a source fill value only when it fits the type of the variable.
        return match target.variable() {
            Some(variable) => {
                packed.contains(variable)
                    || data_type(column) != data_type(variable)
                    || !data_type(variable).is_some_and(|t| t.is_integer() || t.is_floating())
            }
            None => true,
        };
    }
    if name.starts_with('_') || name == "scale_factor" || name == "add_offset" {
        return true;
    }
    let Some(variable) = target.variable() else {
        return false;
    };
    if PACKED_TYPE.contains(&name) {
        // CF gives a packed-type attribute the type of the packed variable.
        if packed.contains(variable) || data_type(column) != data_type(variable) {
            return true;
        }
    }
    let is_time = schema
        .field_with_name(variable)
        .is_ok_and(|f| matches!(f.data_type(), DataType::Timestamp(_, _)));
    is_time && TIME_OWNED.contains(&name)
}

/// The name prefix of a global attribute column that stays a variable.
pub const GLOBAL_PREFIX: &str = "global.";

/// What the writer does with the columns at `finish`.
#[derive(Debug, Default)]
pub struct AttributePlan {
    /// Attributes to write, in schema order.
    pub attributes: Vec<(AttributeTarget, AttributeValue)>,
    /// Columns the writer does not write as variables.
    pub skipped: HashSet<String>,
    /// Columns that stay variables under a new name.
    pub renamed: HashMap<String, String>,
    /// Source `_FillValue` of each variable. The encoder writes a null as this value.
    pub fill_values: HashMap<String, AttributeValue>,
}

/// The values one attribute column held so far.
#[derive(Debug, Clone, PartialEq)]
enum Seen {
    /// No rows.
    Empty,
    /// Every row holds this value. `None` is null.
    One(Option<ScalarValue>),
    /// The rows hold more than one value.
    Many,
}

impl Seen {
    fn merge(self, other: Seen) -> Seen {
        match (self, other) {
            (Seen::Empty, seen) | (seen, Seen::Empty) => seen,
            (Seen::One(a), Seen::One(b)) if a == b => Seen::One(a),
            _ => Seen::Many,
        }
    }
}

/// The values of one attribute column in one batch.
fn seen_in(column: &ArrayRef) -> anyhow::Result<Seen> {
    if column.is_empty() {
        return Ok(Seen::Empty);
    }
    if column.null_count() == column.len() {
        return Ok(Seen::One(None));
    }
    if column.null_count() > 0 {
        return Ok(Seen::Many);
    }
    let first = ScalarValue::try_from_array(column, 0)?;
    // Arrow compares floats by total order, so NaN equals NaN.
    let equal = arrow::compute::kernels::cmp::eq(column, &first.to_scalar()?)?;
    Ok(if equal.true_count() == column.len() {
        Seen::One(Some(first))
    } else {
        Seen::Many
    })
}

/// The first non-null value of one attribute column in one batch.
fn first_value(column: &ArrayRef) -> anyhow::Result<Option<ScalarValue>> {
    match (0..column.len()).find(|&row| column.is_valid(row)) {
        Some(row) => Ok(Some(ScalarValue::try_from_array(column, row)?)),
        None => Ok(None),
    }
}

/// Whether `to_attribute_value` converts a column of this type.
fn is_attribute_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Int8
            | DataType::UInt8
            | DataType::Int16
            | DataType::UInt16
            | DataType::Int32
            | DataType::UInt32
            | DataType::Int64
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

/// The NetCDF attribute value of `value`, or `None` when NetCDF has no such type.
fn to_attribute_value(value: &ScalarValue) -> Option<AttributeValue> {
    Some(match value {
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => AttributeValue::Str(v.clone()),
        ScalarValue::Int8(Some(v)) => AttributeValue::Schar(*v),
        ScalarValue::UInt8(Some(v)) => AttributeValue::Uchar(*v),
        ScalarValue::Int16(Some(v)) => AttributeValue::Short(*v),
        ScalarValue::UInt16(Some(v)) => AttributeValue::Ushort(*v),
        ScalarValue::Int32(Some(v)) => AttributeValue::Int(*v),
        ScalarValue::UInt32(Some(v)) => AttributeValue::Uint(*v),
        ScalarValue::Int64(Some(v)) => AttributeValue::Longlong(*v),
        ScalarValue::UInt64(Some(v)) => AttributeValue::Ulonglong(*v),
        ScalarValue::Float32(Some(v)) => AttributeValue::Float(*v),
        ScalarValue::Float64(Some(v)) => AttributeValue::Double(*v),
        _ => return None,
    })
}

/// The state of one attribute column.
#[derive(Debug)]
struct Tracked {
    column: String,
    target: AttributeTarget,
    index: usize,
    seen: Seen,
    first: Option<ScalarValue>,
}

/// Records the values of the attribute columns while the writer spools batches.
#[derive(Debug)]
pub struct AttributeCombiner {
    policy: CombineAttrs,
    columns: HashSet<String>,
    dropped: HashSet<String>,
    tracked: Vec<Tracked>,
    /// Global attribute columns with no NetCDF attribute type. They stay variables.
    untracked_globals: Vec<(String, AttributeTarget)>,
}

impl AttributeCombiner {
    /// Classify the columns of `schema`. Use the schema of the batches that `observe` gets.
    pub fn new(schema: &Schema, policy: CombineAttrs) -> Self {
        let classification = classify(schema);
        let mut tracked = Vec::new();
        let mut untracked_globals = Vec::new();
        for (column, target) in classification.attributes {
            let index = schema
                .index_of(&column)
                .expect("classify returns schema columns");
            if is_attribute_type(schema.field(index).data_type()) {
                tracked.push(Tracked {
                    index,
                    column,
                    target,
                    seen: Seen::Empty,
                    first: None,
                });
            } else if let AttributeTarget::Global { .. } = target {
                untracked_globals.push((column, target));
            }
        }
        Self {
            policy,
            columns: schema.fields().iter().map(|f| f.name().clone()).collect(),
            dropped: classification.dropped,
            tracked,
            untracked_globals,
        }
    }

    /// Record the attribute values of one batch.
    pub fn observe(&mut self, batch: &RecordBatch) -> anyhow::Result<()> {
        for tracked in &mut self.tracked {
            if tracked.seen == Seen::Many && tracked.first.is_some() {
                continue;
            }
            let column = batch.column(tracked.index);
            let seen = std::mem::replace(&mut tracked.seen, Seen::Empty);
            tracked.seen = seen.merge(seen_in(column)?);
            if tracked.first.is_none() {
                tracked.first = first_value(column)?;
            }
        }
        Ok(())
    }

    /// Decide, for each attribute column, between attribute, variable and drop.
    pub fn resolve(self) -> anyhow::Result<AttributePlan> {
        let mut plan = AttributePlan {
            skipped: self.dropped,
            ..AttributePlan::default()
        };
        for (column, target) in self.untracked_globals {
            keep_as_variable(&mut plan, &self.columns, column, &target)?;
        }
        for tracked in self.tracked {
            if let AttributeTarget::Variable { variable, name } = &tracked.target {
                if name == FILL_VALUE {
                    // A fill value with more values has no meaning, because the reader masked them.
                    let value = match tracked.seen {
                        Seen::One(Some(value)) => to_attribute_value(&value),
                        _ => None,
                    };
                    if let Some(value) = value {
                        plan.fill_values.insert(variable.clone(), value);
                    }
                    plan.skipped.insert(tracked.column);
                    continue;
                }
            }
            let value = match (tracked.seen, self.policy) {
                (Seen::Empty | Seen::One(None), _) => None,
                (Seen::One(Some(value)), _) => Some(value),
                (Seen::Many, CombineAttrs::First) => {
                    tracing::warn!(
                        column = %tracked.column,
                        "attribute column holds more than one value; combine_attrs=first keeps the first"
                    );
                    tracked.first
                }
                (Seen::Many, CombineAttrs::DropConflicts) => None,
                (Seen::Many, CombineAttrs::Identical) => {
                    keep_as_variable(&mut plan, &self.columns, tracked.column, &tracked.target)?;
                    continue;
                }
            };
            let Some(value) = value else {
                plan.skipped.insert(tracked.column);
                continue;
            };
            match to_attribute_value(&value) {
                Some(attribute) => {
                    plan.skipped.insert(tracked.column);
                    plan.attributes.push((tracked.target, attribute));
                }
                None => {
                    keep_as_variable(&mut plan, &self.columns, tracked.column, &tracked.target)?
                }
            }
        }
        Ok(plan)
    }
}

/// Keep `column` as a variable. A global gets a name that libnetcdf accepts.
fn keep_as_variable(
    plan: &mut AttributePlan,
    columns: &HashSet<String>,
    column: String,
    target: &AttributeTarget,
) -> anyhow::Result<()> {
    if let AttributeTarget::Global { name } = target {
        let renamed = format!("{GLOBAL_PREFIX}{name}");
        if columns.contains(&renamed) {
            anyhow::bail!(
                "cannot keep global attribute column '{column}' as variable '{renamed}': \
                 the output already has a column '{renamed}'"
            );
        }
        plan.renamed.insert(column, renamed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, Float64Array, Int16Array, ListArray, RecordBatch, StringArray,
        TimestampNanosecondArray,
    };
    use arrow::datatypes::{Field, Float64Type, Schema, TimeUnit};
    use netcdf::AttributeValue;
    use std::sync::Arc;

    fn schema(fields: &[(&str, DataType)]) -> Schema {
        Schema::new(
            fields
                .iter()
                .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
                .collect::<Vec<_>>(),
        )
    }

    fn target_of<'a>(
        classification: &'a Classification,
        column: &str,
    ) -> Option<&'a AttributeTarget> {
        classification
            .attributes
            .iter()
            .find(|(name, _)| name == column)
            .map(|(_, target)| target)
    }

    fn variable(variable: &str, name: &str) -> AttributeTarget {
        AttributeTarget::Variable {
            variable: variable.to_string(),
            name: name.to_string(),
        }
    }

    #[test]
    fn a_dotted_column_with_a_host_is_a_variable_attribute() {
        let c = classify(&schema(&[
            ("temp", DataType::Float64),
            ("temp.units", DataType::Utf8),
        ]));
        assert_eq!(
            target_of(&c, "temp.units"),
            Some(&variable("temp", "units"))
        );
    }

    #[test]
    fn a_dotted_column_without_a_host_is_a_plain_column() {
        let c = classify(&schema(&[("temp.units", DataType::Utf8)]));
        assert!(c.attributes.is_empty());
        assert!(c.dropped.is_empty());
    }

    #[test]
    fn a_leading_dot_column_is_a_global_attribute() {
        let c = classify(&schema(&[
            ("temp", DataType::Float64),
            (".title", DataType::Utf8),
        ]));
        assert_eq!(
            target_of(&c, ".title"),
            Some(&AttributeTarget::Global {
                name: "title".to_string()
            })
        );
    }

    #[test]
    fn the_last_dot_ends_the_variable_name() {
        let c = classify(&schema(&[
            ("sea.temp", DataType::Float64),
            ("sea.temp.units", DataType::Utf8),
        ]));
        assert_eq!(
            target_of(&c, "sea.temp.units"),
            Some(&variable("sea.temp", "units"))
        );
    }

    #[test]
    fn an_attribute_column_is_no_host() {
        let c = classify(&schema(&[
            ("temp", DataType::Float64),
            ("temp.units", DataType::Utf8),
            ("temp.units.x", DataType::Utf8),
        ]));
        assert_eq!(
            target_of(&c, "temp.units"),
            Some(&variable("temp", "units"))
        );
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
        let c = classify(&schema(&[
            ("temp", DataType::Float64),
            ("temp.valid_min", DataType::Float64),
        ]));
        assert_eq!(
            target_of(&c, "temp.valid_min"),
            Some(&variable("temp", "valid_min"))
        );
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
        assert_eq!(
            target_of(&c, "time.long_name"),
            Some(&variable("time", "long_name"))
        );
    }

    #[test]
    fn combine_attrs_parses_without_case() {
        assert_eq!(
            "Identical".parse::<CombineAttrs>(),
            Ok(CombineAttrs::Identical)
        );
        assert_eq!("first".parse::<CombineAttrs>(), Ok(CombineAttrs::First));
        assert_eq!(
            " DROP_CONFLICTS ".parse::<CombineAttrs>(),
            Ok(CombineAttrs::DropConflicts)
        );
        let error = "union".parse::<CombineAttrs>().unwrap_err();
        assert!(error.contains("combine_attrs"), "{error}");
    }

    /// Schema `temp`, `temp.units`, `.title`.
    fn units_schema() -> Arc<Schema> {
        Arc::new(schema(&[
            ("temp", DataType::Float64),
            ("temp.units", DataType::Utf8),
            (".title", DataType::Utf8),
        ]))
    }

    fn units_batch(
        schema: &Arc<Schema>,
        units: Vec<Option<&str>>,
        title: Vec<Option<&str>>,
    ) -> RecordBatch {
        let temp: ArrayRef = Arc::new(Float64Array::from(vec![1.0; units.len()]));
        RecordBatch::try_new(
            schema.clone(),
            vec![
                temp,
                Arc::new(StringArray::from(units)),
                Arc::new(StringArray::from(title)),
            ],
        )
        .unwrap()
    }

    fn combine(
        policy: CombineAttrs,
        schema: &Arc<Schema>,
        batches: &[RecordBatch],
    ) -> anyhow::Result<AttributePlan> {
        let mut combiner = AttributeCombiner::new(schema, policy);
        for batch in batches {
            combiner.observe(batch)?;
        }
        combiner.resolve()
    }

    fn attribute<'a>(
        plan: &'a AttributePlan,
        target: &AttributeTarget,
    ) -> Option<&'a AttributeValue> {
        plan.attributes
            .iter()
            .find(|(t, _)| t == target)
            .map(|(_, value)| value)
    }

    fn global(name: &str) -> AttributeTarget {
        AttributeTarget::Global {
            name: name.to_string(),
        }
    }

    #[test]
    fn one_value_over_all_batches_becomes_an_attribute() {
        let s = units_schema();
        let batches = [
            units_batch(
                &s,
                vec![Some("degC"), Some("degC")],
                vec![Some("Argo"), Some("Argo")],
            ),
            units_batch(&s, vec![Some("degC")], vec![Some("Argo")]),
        ];
        let plan = combine(CombineAttrs::Identical, &s, &batches).unwrap();
        assert!(
            matches!(attribute(&plan, &variable("temp", "units")), Some(AttributeValue::Str(v)) if v == "degC")
        );
        assert!(
            matches!(attribute(&plan, &global("title")), Some(AttributeValue::Str(v)) if v == "Argo")
        );
        assert!(plan.skipped.contains("temp.units"));
        assert!(plan.skipped.contains(".title"));
        assert!(plan.renamed.is_empty());
    }

    #[test]
    fn two_values_stay_a_variable_under_identical() {
        let s = units_schema();
        let batches = [
            units_batch(&s, vec![Some("degC")], vec![Some("Argo")]),
            units_batch(&s, vec![Some("K")], vec![Some("Glider")]),
        ];
        let plan = combine(CombineAttrs::Identical, &s, &batches).unwrap();
        assert!(attribute(&plan, &variable("temp", "units")).is_none());
        assert!(!plan.skipped.contains("temp.units"));
        assert_eq!(
            plan.renamed.get(".title").map(String::as_str),
            Some("global.title")
        );
    }

    #[test]
    fn drop_conflicts_skips_a_column_with_two_values() {
        let s = units_schema();
        let batches = [units_batch(
            &s,
            vec![Some("degC"), Some("K")],
            vec![Some("Argo"), Some("Argo")],
        )];
        let plan = combine(CombineAttrs::DropConflicts, &s, &batches).unwrap();
        assert!(attribute(&plan, &variable("temp", "units")).is_none());
        assert!(plan.skipped.contains("temp.units"));
        assert!(
            matches!(attribute(&plan, &global("title")), Some(AttributeValue::Str(v)) if v == "Argo")
        );
    }

    #[test]
    fn first_keeps_the_first_non_null_value() {
        let s = units_schema();
        let batches = [
            units_batch(&s, vec![None, Some("K")], vec![Some("Argo"), Some("Argo")]),
            units_batch(&s, vec![Some("degC")], vec![Some("Argo")]),
        ];
        let plan = combine(CombineAttrs::First, &s, &batches).unwrap();
        assert!(
            matches!(attribute(&plan, &variable("temp", "units")), Some(AttributeValue::Str(v)) if v == "K")
        );
        assert!(plan.skipped.contains("temp.units"));
    }

    #[test]
    fn a_null_counts_as_a_value() {
        let s = units_schema();
        let batches = [units_batch(
            &s,
            vec![Some("degC"), None],
            vec![Some("Argo"), Some("Argo")],
        )];
        let plan = combine(CombineAttrs::Identical, &s, &batches).unwrap();
        assert!(attribute(&plan, &variable("temp", "units")).is_none());
        assert!(!plan.skipped.contains("temp.units"));
    }

    #[test]
    fn an_all_null_column_is_skipped() {
        let s = units_schema();
        let batches = [units_batch(
            &s,
            vec![None, None],
            vec![Some("Argo"), Some("Argo")],
        )];
        let plan = combine(CombineAttrs::Identical, &s, &batches).unwrap();
        assert!(attribute(&plan, &variable("temp", "units")).is_none());
        assert!(plan.skipped.contains("temp.units"));
    }

    #[test]
    fn zero_rows_skip_the_column() {
        let s = units_schema();
        let plan = combine(CombineAttrs::Identical, &s, &[]).unwrap();
        assert!(plan.attributes.is_empty());
        assert!(plan.skipped.contains("temp.units"));
        assert!(plan.skipped.contains(".title"));
    }

    #[test]
    fn nan_in_every_row_is_one_value() {
        let s = Arc::new(schema(&[
            ("temp", DataType::Float64),
            ("temp.valid_min", DataType::Float64),
        ]));
        let batch = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
                Arc::new(Float64Array::from(vec![f64::NAN, f64::NAN])),
            ],
        )
        .unwrap();
        let plan = combine(CombineAttrs::Identical, &s, &[batch]).unwrap();
        assert!(
            matches!(attribute(&plan, &variable("temp", "valid_min")), Some(AttributeValue::Double(v)) if v.is_nan())
        );
    }

    #[test]
    fn an_int16_value_is_a_short_attribute() {
        let s = Arc::new(schema(&[
            ("temp", DataType::Float64),
            ("temp.flag", DataType::Int16),
        ]));
        let batch = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(Int16Array::from(vec![7])),
            ],
        )
        .unwrap();
        let plan = combine(CombineAttrs::Identical, &s, &[batch]).unwrap();
        assert!(matches!(
            attribute(&plan, &variable("temp", "flag")),
            Some(AttributeValue::Short(7))
        ));
    }

    #[test]
    fn a_rename_that_hits_a_column_is_an_error() {
        let s = Arc::new(schema(&[
            ("temp", DataType::Float64),
            (".title", DataType::Utf8),
            ("global.title", DataType::Utf8),
        ]));
        let batch = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
                Arc::new(StringArray::from(vec!["Argo", "Glider"])),
                Arc::new(StringArray::from(vec!["x", "x"])),
            ],
        )
        .unwrap();
        let error = combine(CombineAttrs::Identical, &s, &[batch])
            .unwrap_err()
            .to_string();
        assert!(error.contains("global.title"), "{error}");
    }

    #[test]
    fn dropped_columns_are_skipped() {
        let s = Arc::new(schema(&[
            ("temp", DataType::Float64),
            ("temp._FillValue", DataType::Float64),
        ]));
        let plan = combine(CombineAttrs::Identical, &s, &[]).unwrap();
        assert!(plan.skipped.contains("temp._FillValue"));
    }

    fn fill_batch(s: &Arc<Schema>, fills: Vec<f64>) -> RecordBatch {
        RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(Float64Array::from(vec![1.0; fills.len()])) as ArrayRef,
                Arc::new(Float64Array::from(fills)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn a_fill_value_of_the_variable_type_becomes_the_fill_value() {
        let s = Arc::new(schema(&[
            ("temp", DataType::Float64),
            ("temp._FillValue", DataType::Float64),
        ]));
        let plan = combine(
            CombineAttrs::Identical,
            &s,
            &[fill_batch(&s, vec![-999.0, -999.0])],
        )
        .unwrap();
        assert_eq!(
            plan.fill_values.get("temp"),
            Some(&AttributeValue::Double(-999.0))
        );
        assert!(plan.skipped.contains("temp._FillValue"));
        assert!(plan.attributes.is_empty());
    }

    #[test]
    fn a_fill_value_with_two_values_is_dropped() {
        let s = Arc::new(schema(&[
            ("temp", DataType::Float64),
            ("temp._FillValue", DataType::Float64),
        ]));
        let plan = combine(
            CombineAttrs::Identical,
            &s,
            &[fill_batch(&s, vec![-999.0, 99999.0])],
        )
        .unwrap();
        assert!(plan.fill_values.is_empty());
        assert!(plan.skipped.contains("temp._FillValue"));
    }

    #[test]
    fn a_fill_value_of_another_type_is_dropped() {
        let c = classify(&schema(&[
            ("temp", DataType::Float32),
            ("temp._FillValue", DataType::Float64),
            ("name", DataType::Utf8),
            ("name._FillValue", DataType::Utf8),
        ]));
        assert!(c.dropped.contains("temp._FillValue"));
        assert!(c.dropped.contains("name._FillValue"));
    }

    #[test]
    fn an_unsupported_type_stays_a_variable() {
        let list = DataType::List(Arc::new(Field::new("item", DataType::Float64, true)));
        let s = Arc::new(schema(&[
            ("temp", DataType::Float64),
            ("temp.actual_range", list),
        ]));
        let range = ListArray::from_iter_primitive::<Float64Type, _, _>(vec![Some(vec![
            Some(0.0),
            Some(40.0),
        ])]);
        let batch = RecordBatch::try_new(
            s.clone(),
            vec![Arc::new(Float64Array::from(vec![1.0])), Arc::new(range)],
        )
        .unwrap();
        let plan = combine(CombineAttrs::Identical, &s, &[batch]).unwrap();
        assert!(attribute(&plan, &variable("temp", "actual_range")).is_none());
        assert!(!plan.skipped.contains("temp.actual_range"));
        assert!(plan.renamed.is_empty());
    }

    #[test]
    fn a_global_of_an_unsupported_type_stays_a_renamed_variable() {
        let s = Arc::new(schema(&[
            ("temp", DataType::Float64),
            (
                ".date_created",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
        ]));
        let batch = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(TimestampNanosecondArray::from(vec![0])),
            ],
        )
        .unwrap();
        let plan = combine(CombineAttrs::Identical, &s, &[batch]).unwrap();
        assert!(plan.attributes.is_empty());
        assert!(!plan.skipped.contains(".date_created"));
        assert_eq!(
            plan.renamed.get(".date_created").map(String::as_str),
            Some("global.date_created")
        );
    }

    #[test]
    fn a_renamed_unsupported_global_that_hits_a_column_is_an_error() {
        let s = Arc::new(schema(&[
            ("temp", DataType::Float64),
            (
                ".date_created",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            ("global.date_created", DataType::Float64),
        ]));
        let error = combine(CombineAttrs::Identical, &s, &[])
            .unwrap_err()
            .to_string();
        assert!(error.contains("global.date_created"), "{error}");
    }

    #[test]
    fn a_packed_type_attribute_of_another_type_is_dropped_without_a_scale_factor() {
        let c = classify(&schema(&[
            ("temp", DataType::Float64),
            ("temp.valid_min", DataType::Int16),
            ("temp.valid_max", DataType::Float64),
        ]));
        assert!(c.dropped.contains("temp.valid_min"));
        assert!(!c.dropped.contains("temp.valid_max"));
        assert_eq!(target_of(&c, "temp.valid_min"), None);
    }

    #[test]
    fn a_null_then_a_value_stays_a_variable_under_identical() {
        let s = units_schema();
        let batches = [
            units_batch(&s, vec![None], vec![Some("Argo")]),
            units_batch(&s, vec![Some("degC")], vec![Some("Argo")]),
        ];
        let plan = combine(CombineAttrs::Identical, &s, &batches).unwrap();
        assert!(attribute(&plan, &variable("temp", "units")).is_none());
        assert!(!plan.skipped.contains("temp.units"));
    }

    #[test]
    fn nan_in_every_row_of_two_batches_is_one_attribute() {
        let s = Arc::new(schema(&[
            ("temp", DataType::Float64),
            ("temp.valid_min", DataType::Float64),
        ]));
        let batch = || {
            RecordBatch::try_new(
                s.clone(),
                vec![
                    Arc::new(Float64Array::from(vec![1.0, 2.0])),
                    Arc::new(Float64Array::from(vec![f64::NAN, f64::NAN])),
                ],
            )
            .unwrap()
        };
        let plan = combine(CombineAttrs::Identical, &s, &[batch(), batch()]).unwrap();
        assert_eq!(plan.attributes.len(), 1);
        assert!(
            matches!(attribute(&plan, &variable("temp", "valid_min")), Some(AttributeValue::Double(v)) if v.is_nan())
        );
        assert!(plan.skipped.contains("temp.valid_min"));
    }
}
