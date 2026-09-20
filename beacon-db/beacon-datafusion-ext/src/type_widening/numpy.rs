//! The numpy strategy: the promotion rules of `numpy.result_type`.
//!
//! A boolean joins the numbers, `Float16` joins the floats, an integer beside
//! a float gives the float whose mantissa holds it, and a number beside a
//! string gives the string. A date beside a timestamp gives a timestamp at the
//! finer unit. Two families are a conflict, and [`TypeConflict`] settles it.
//! numpy resolves a set of types at once, so this strategy gathers the types
//! of a column and resolves them once, and the listing order changes no result.
//!
//! Arrow has no cast for four numpy rules, and each is a conflict here: an
//! integer beside a duration, a duration beside a timestamp, and a number or a
//! string beside a binary. A time of day keeps the rule of the default strategy.

use arrow_schema::{ArrowError, DataType, SchemaRef, TimeUnit};

use super::common::{
    self, Resolved, StatedTypes, finer_unit, incompatible_types, integer_join, wider, zone_join,
};
use super::{ArrowTypeWideningStrategy, LabeledSchema, TypeConflict};

/// Merges the schemas of a table with the rules of the [module docs](self).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NumpyArrowTypeWidening {
    /// What the merge does with a column that no type holds.
    pub on_conflict: TypeConflict,
}

impl NumpyArrowTypeWidening {
    /// The rule that refuses such a column.
    pub const fn new() -> Self {
        Self {
            on_conflict: TypeConflict::Fail,
        }
    }

    /// The rule that keeps its first type.
    pub const fn keeping_first_type() -> Self {
        Self {
            on_conflict: TypeConflict::KeepFirst,
        }
    }
}

impl ArrowTypeWideningStrategy for NumpyArrowTypeWidening {
    fn is_order_independent(&self) -> bool {
        // The set decides, and a chunk result hides the set.
        false
    }

    fn merge_schemas(&self, schemas: &[LabeledSchema]) -> Result<SchemaRef, ArrowError> {
        common::merge_schemas_with(schemas, |name, types| {
            resolve(name, types, self.on_conflict)
        })
    }

    fn on_conflict(&self) -> TypeConflict {
        self.on_conflict
    }

    fn casts_leniently(&self, source: &DataType, target: &DataType) -> bool {
        // Two families never promote, so only the setting put them in one column.
        self.on_conflict == TypeConflict::KeepFirst && Family::of(source) != Family::of(target)
    }
}

/// The type that holds every stated type. The first stated type names the
/// family, and a type of another family is a conflict that `on_conflict`
/// settles.
fn resolve(
    name: &str,
    types: &StatedTypes,
    on_conflict: TypeConflict,
) -> Result<Resolved, ArrowError> {
    let mut stated = types
        .iter()
        .filter(|(data_type, _)| data_type != &DataType::Null);
    let Some((first, _)) = stated.next() else {
        return Ok(Resolved {
            data_type: DataType::Null,
            conflict: false,
        });
    };

    let family = Family::of(first);
    let mut members = vec![first];
    let mut conflict = false;
    for (data_type, source) in stated {
        if Family::of(data_type) == family {
            members.push(data_type);
            continue;
        }
        match on_conflict {
            TypeConflict::Fail => {
                let so_far = family.result(&members);
                // The source that states the type the family holds so far.
                let so_far_source = types
                    .iter()
                    .find(|(stated, _)| stated == &so_far)
                    .and_then(|(_, source)| source.as_deref());
                return Err(incompatible_types(
                    name,
                    &so_far,
                    so_far_source,
                    data_type,
                    source.as_deref(),
                ));
            }
            TypeConflict::KeepFirst => conflict = true,
        }
    }

    Ok(Resolved {
        data_type: family.result(&members),
        conflict,
    })
}

/// The kinds that promote with one another.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Family {
    /// numpy writes a number as text beside a string, so the four kinds share
    /// one family.
    NumberOrText,
    Datetime,
    Duration,
    Time,
    Binary,
    /// A type numpy has no rule for. It promotes with itself alone.
    Other(DataType),
}

impl Family {
    fn of(data_type: &DataType) -> Self {
        use DataType::*;
        match data_type {
            Boolean | Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float16
            | Float32 | Float64 | Utf8 | Utf8View | LargeUtf8 => Self::NumberOrText,
            Date32 | Date64 | Timestamp(_, _) => Self::Datetime,
            Duration(_) => Self::Duration,
            Time32(_) | Time64(_) => Self::Time,
            Binary | BinaryView | LargeBinary => Self::Binary,
            other => Self::Other(other.clone()),
        }
    }

    /// The type that holds every member of this family.
    fn result(&self, members: &[&DataType]) -> DataType {
        match self {
            Self::NumberOrText => number_or_text(members),
            Self::Datetime => members
                .iter()
                .skip(1)
                .fold(members[0].clone(), |so_far, member| {
                    datetime_join(&so_far, member)
                }),
            Self::Duration | Self::Time | Self::Binary => {
                let mut widest = members[0];
                for member in members {
                    widest = wider(widest, member);
                }
                widest.clone()
            }
            Self::Other(data_type) => data_type.clone(),
        }
    }
}

/// `numpy.result_type` over booleans, integers, floats and strings. Each kind
/// keeps its widest member, and the widest members meet once.
fn number_or_text(members: &[&DataType]) -> DataType {
    use DataType::*;
    let (mut text, mut signed, mut unsigned, mut float) = (None, None, None, None);
    for &member in members {
        let widest: &mut Option<&DataType> = match member {
            Utf8 | Utf8View | LargeUtf8 => &mut text,
            Int8 | Int16 | Int32 | Int64 => &mut signed,
            UInt8 | UInt16 | UInt32 | UInt64 => &mut unsigned,
            Float16 | Float32 | Float64 => &mut float,
            _ => continue,
        };
        *widest = Some(widest.map_or(member, |held| wider(held, member)));
    }

    match (text, signed, unsigned, float) {
        (Some(text), _, _, _) => text.clone(),
        (None, None, None, None) => Boolean,
        (None, signed, unsigned, Some(float)) => [signed, unsigned]
            .into_iter()
            .flatten()
            .map(|int| float_that_holds(float, int))
            .fold(float.clone(), |so_far, holds| {
                wider(&so_far, &holds).clone()
            }),
        (None, Some(signed), Some(unsigned), None) => integer_join(signed, unsigned),
        (None, Some(int), None, None) | (None, None, Some(int), None) => int.clone(),
    }
}

/// The float whose mantissa holds `int`, or `float` when it is wider.
fn float_that_holds(float: &DataType, int: &DataType) -> DataType {
    use DataType::*;
    match (float, int) {
        (Float64, _) | (_, Int32 | UInt32 | Int64 | UInt64) => Float64,
        (Float32, _) | (_, Int16 | UInt16) => Float32,
        _ => Float16,
    }
}

/// `numpy.result_type` over two dates or timestamps. `Date32` counts days and
/// `Date64` milliseconds, so a timestamp beside a `Date64` is at least
/// milliseconds.
fn datetime_join(left: &DataType, right: &DataType) -> DataType {
    use DataType::*;
    match (left, right) {
        (Date32, Date64) | (Date64, Date32) => Date64,
        (Date32, stamp @ Timestamp(_, _)) | (stamp @ Timestamp(_, _), Date32) => stamp.clone(),
        (Date64, Timestamp(unit, zone)) | (Timestamp(unit, zone), Date64) => {
            Timestamp(finer_unit(*unit, TimeUnit::Millisecond), zone.clone())
        }
        (Timestamp(left_unit, left_zone), Timestamp(right_unit, right_zone)) => Timestamp(
            finer_unit(*left_unit, *right_unit),
            zone_join(left_zone, right_zone),
        ),
        (left, _) => left.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::compute::can_cast_types;
    use arrow_schema::{Field, FieldRef, Fields, IntervalUnit, Schema};

    use super::super::ArrowTypeWidening;
    use super::*;

    fn schema(fields: &[(&str, DataType)]) -> LabeledSchema {
        LabeledSchema::unlabeled(schema_ref(fields))
    }

    fn from_file(label: &str, fields: &[(&str, DataType)]) -> LabeledSchema {
        LabeledSchema::new(schema_ref(fields), label)
    }

    fn schema_ref(fields: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, dt)| Field::new(*name, dt.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    fn numpy() -> ArrowTypeWidening {
        ArrowTypeWidening::new(Arc::new(NumpyArrowTypeWidening::new()))
    }

    fn keeping_first() -> ArrowTypeWidening {
        ArrowTypeWidening::new(Arc::new(NumpyArrowTypeWidening::keeping_first_type()))
    }

    fn field_of<'a>(schema: &'a Schema, name: &str) -> &'a FieldRef {
        schema
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .unwrap_or_else(|| panic!("the merge holds '{name}'"))
    }

    fn promote(types: &[DataType]) -> Result<DataType, ArrowError> {
        let schemas: Vec<LabeledSchema> = types
            .iter()
            .map(|data_type| schema(&[("a", data_type.clone())]))
            .collect();
        numpy()
            .merge_schemas(&schemas)
            .map(|merged| field_of(&merged, "a").data_type().clone())
    }

    fn message_of(error: ArrowError) -> String {
        match error {
            ArrowError::SchemaError(message) => message,
            other => panic!("expected SchemaError, got {other:?}"),
        }
    }

    /// `numpy.promote_types` for every pair of [`NUMPY_TYPES`], row by row.
    /// Generated with numpy 2.5.2. `ERR` marks a `TypeError`.
    const NUMPY_TYPES: [&str; 23] = [
        "bool", "int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64",
        "float16", "float32", "float64", "M8[D]", "M8[s]", "M8[ms]", "M8[us]", "M8[ns]", "m8[s]",
        "m8[ms]", "m8[us]", "m8[ns]", "U", "S",
    ];
    const NUMPY_PROMOTE_TYPES: [[&str; 23]; 23] = [
        // bool
        [
            "bool", "int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64",
            "float16", "float32", "float64", "ERR", "ERR", "ERR", "ERR", "ERR", "m8[s]", "m8[ms]",
            "m8[us]", "m8[ns]", "U", "S",
        ],
        // int8
        [
            "int8", "int8", "int16", "int16", "int32", "int32", "int64", "int64", "float64",
            "float16", "float32", "float64", "ERR", "ERR", "ERR", "ERR", "ERR", "m8[s]", "m8[ms]",
            "m8[us]", "m8[ns]", "U", "S",
        ],
        // uint8
        [
            "uint8", "int16", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64",
            "float16", "float32", "float64", "ERR", "ERR", "ERR", "ERR", "ERR", "m8[s]", "m8[ms]",
            "m8[us]", "m8[ns]", "U", "S",
        ],
        // int16
        [
            "int16", "int16", "int16", "int16", "int32", "int32", "int64", "int64", "float64",
            "float32", "float32", "float64", "ERR", "ERR", "ERR", "ERR", "ERR", "m8[s]", "m8[ms]",
            "m8[us]", "m8[ns]", "U", "S",
        ],
        // uint16
        [
            "uint16", "int32", "uint16", "int32", "uint16", "int32", "uint32", "int64", "uint64",
            "float32", "float32", "float64", "ERR", "ERR", "ERR", "ERR", "ERR", "m8[s]", "m8[ms]",
            "m8[us]", "m8[ns]", "U", "S",
        ],
        // int32
        [
            "int32", "int32", "int32", "int32", "int32", "int32", "int64", "int64", "float64",
            "float64", "float64", "float64", "ERR", "ERR", "ERR", "ERR", "ERR", "m8[s]", "m8[ms]",
            "m8[us]", "m8[ns]", "U", "S",
        ],
        // uint32
        [
            "uint32", "int64", "uint32", "int64", "uint32", "int64", "uint32", "int64", "uint64",
            "float64", "float64", "float64", "ERR", "ERR", "ERR", "ERR", "ERR", "m8[s]", "m8[ms]",
            "m8[us]", "m8[ns]", "U", "S",
        ],
        // int64
        [
            "int64", "int64", "int64", "int64", "int64", "int64", "int64", "int64", "float64",
            "float64", "float64", "float64", "ERR", "ERR", "ERR", "ERR", "ERR", "m8[s]", "m8[ms]",
            "m8[us]", "m8[ns]", "U", "S",
        ],
        // uint64
        [
            "uint64", "float64", "uint64", "float64", "uint64", "float64", "uint64", "float64",
            "uint64", "float64", "float64", "float64", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR",
            "ERR", "ERR", "ERR", "U", "S",
        ],
        // float16
        [
            "float16", "float16", "float16", "float32", "float32", "float64", "float64", "float64",
            "float64", "float16", "float32", "float64", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR",
            "ERR", "ERR", "ERR", "U", "S",
        ],
        // float32
        [
            "float32", "float32", "float32", "float32", "float32", "float64", "float64", "float64",
            "float64", "float32", "float32", "float64", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR",
            "ERR", "ERR", "ERR", "U", "S",
        ],
        // float64
        [
            "float64", "float64", "float64", "float64", "float64", "float64", "float64", "float64",
            "float64", "float64", "float64", "float64", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR",
            "ERR", "ERR", "ERR", "U", "S",
        ],
        // M8[D]
        [
            "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR",
            "M8[D]", "M8[s]", "M8[ms]", "M8[us]", "M8[ns]", "M8[s]", "M8[ms]", "M8[us]", "M8[ns]",
            "ERR", "ERR",
        ],
        // M8[s]
        [
            "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR",
            "M8[s]", "M8[s]", "M8[ms]", "M8[us]", "M8[ns]", "M8[s]", "M8[ms]", "M8[us]", "M8[ns]",
            "ERR", "ERR",
        ],
        // M8[ms]
        [
            "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR",
            "M8[ms]", "M8[ms]", "M8[ms]", "M8[us]", "M8[ns]", "M8[ms]", "M8[ms]", "M8[us]",
            "M8[ns]", "ERR", "ERR",
        ],
        // M8[us]
        [
            "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR",
            "M8[us]", "M8[us]", "M8[us]", "M8[us]", "M8[ns]", "M8[us]", "M8[us]", "M8[us]",
            "M8[ns]", "ERR", "ERR",
        ],
        // M8[ns]
        [
            "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR", "ERR",
            "M8[ns]", "M8[ns]", "M8[ns]", "M8[ns]", "M8[ns]", "M8[ns]", "M8[ns]", "M8[ns]",
            "M8[ns]", "ERR", "ERR",
        ],
        // m8[s]
        [
            "m8[s]", "m8[s]", "m8[s]", "m8[s]", "m8[s]", "m8[s]", "m8[s]", "m8[s]", "ERR", "ERR",
            "ERR", "ERR", "M8[s]", "M8[s]", "M8[ms]", "M8[us]", "M8[ns]", "m8[s]", "m8[ms]",
            "m8[us]", "m8[ns]", "ERR", "ERR",
        ],
        // m8[ms]
        [
            "m8[ms]", "m8[ms]", "m8[ms]", "m8[ms]", "m8[ms]", "m8[ms]", "m8[ms]", "m8[ms]", "ERR",
            "ERR", "ERR", "ERR", "M8[ms]", "M8[ms]", "M8[ms]", "M8[us]", "M8[ns]", "m8[ms]",
            "m8[ms]", "m8[us]", "m8[ns]", "ERR", "ERR",
        ],
        // m8[us]
        [
            "m8[us]", "m8[us]", "m8[us]", "m8[us]", "m8[us]", "m8[us]", "m8[us]", "m8[us]", "ERR",
            "ERR", "ERR", "ERR", "M8[us]", "M8[us]", "M8[us]", "M8[us]", "M8[ns]", "m8[us]",
            "m8[us]", "m8[us]", "m8[ns]", "ERR", "ERR",
        ],
        // m8[ns]
        [
            "m8[ns]", "m8[ns]", "m8[ns]", "m8[ns]", "m8[ns]", "m8[ns]", "m8[ns]", "m8[ns]", "ERR",
            "ERR", "ERR", "ERR", "M8[ns]", "M8[ns]", "M8[ns]", "M8[ns]", "M8[ns]", "m8[ns]",
            "m8[ns]", "m8[ns]", "m8[ns]", "ERR", "ERR",
        ],
        // U
        [
            "U", "U", "U", "U", "U", "U", "U", "U", "U", "U", "U", "U", "ERR", "ERR", "ERR", "ERR",
            "ERR", "ERR", "ERR", "ERR", "ERR", "U", "U",
        ],
        // S
        [
            "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "ERR", "ERR", "ERR", "ERR",
            "ERR", "ERR", "ERR", "ERR", "ERR", "U", "S",
        ],
    ];

    fn numpy_promote_types(left: &str, right: &str) -> &'static str {
        let at = |name: &str| {
            NUMPY_TYPES
                .iter()
                .position(|candidate| *candidate == name)
                .unwrap_or_else(|| panic!("{name} is not a numpy type of the oracle"))
        };
        NUMPY_PROMOTE_TYPES[at(left)][at(right)]
    }

    /// The numpy name of an Arrow type. `Date64` and `Timestamp(Millisecond)`
    /// are both `M8[ms]`, and every string layout is `U`.
    fn as_numpy(data_type: &DataType) -> String {
        let unit = |unit: &TimeUnit| match unit {
            TimeUnit::Second => "s",
            TimeUnit::Millisecond => "ms",
            TimeUnit::Microsecond => "us",
            TimeUnit::Nanosecond => "ns",
        };
        match data_type {
            DataType::Boolean => "bool".into(),
            DataType::Int8 => "int8".into(),
            DataType::Int16 => "int16".into(),
            DataType::Int32 => "int32".into(),
            DataType::Int64 => "int64".into(),
            DataType::UInt8 => "uint8".into(),
            DataType::UInt16 => "uint16".into(),
            DataType::UInt32 => "uint32".into(),
            DataType::UInt64 => "uint64".into(),
            DataType::Float16 => "float16".into(),
            DataType::Float32 => "float32".into(),
            DataType::Float64 => "float64".into(),
            DataType::Date32 => "M8[D]".into(),
            DataType::Date64 => "M8[ms]".into(),
            DataType::Timestamp(u, _) => format!("M8[{}]", unit(u)),
            DataType::Duration(u) => format!("m8[{}]", unit(u)),
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "U".into(),
            DataType::Binary | DataType::BinaryView | DataType::LargeBinary => "S".into(),
            other => panic!("{other:?} has no numpy name"),
        }
    }

    fn arrow_types() -> Vec<DataType> {
        let mut types = vec![
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::Date64,
            DataType::Utf8,
            DataType::Utf8View,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::BinaryView,
            DataType::LargeBinary,
        ];
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            types.push(DataType::Timestamp(unit, None));
            types.push(DataType::Duration(unit));
        }
        types
    }

    /// The pairs numpy promotes and this strategy refuses.
    fn leaves_numpy(left: &str, right: &str) -> bool {
        let kind = |name: &str| name.chars().next().unwrap();
        // A boolean or an integer counts time units in numpy. A `uint64` and a float do not.
        let counts = |name: &str| matches!(kind(name), 'b' | 'i' | 'u') && name != "uint64";
        // A boolean, a number or a text string spells as ASCII bytes.
        let spells = |name: &str| matches!(kind(name), 'b' | 'i' | 'u' | 'f' | 'U');
        (counts(left) && kind(right) == 'm')
            || (kind(left) == 'm' && counts(right))
            || (kind(left) == 'm' && kind(right) == 'M')
            || (kind(left) == 'M' && kind(right) == 'm')
            || (kind(left) == 'S' && spells(right))
            || (kind(right) == 'S' && spells(left))
    }

    #[test]
    fn every_pair_matches_the_oracle() {
        for left in arrow_types() {
            for right in arrow_types() {
                let (left_name, right_name) = (as_numpy(&left), as_numpy(&right));
                let expected = numpy_promote_types(&left_name, &right_name);
                let actual = promote(&[left.clone(), right.clone()])
                    .map(|data_type| as_numpy(&data_type))
                    .unwrap_or_else(|_| "ERR".to_string());
                if leaves_numpy(&left_name, &right_name) {
                    assert_ne!(expected, "ERR", "{left:?} and {right:?} is no deviation");
                    assert_eq!(actual, "ERR", "{left:?} and {right:?} must be refused");
                } else {
                    assert_eq!(actual, expected, "{left:?} and {right:?}");
                }
            }
        }
    }

    #[test]
    fn a_pair_reads_the_same_in_both_orders() {
        for left in arrow_types() {
            for right in arrow_types() {
                assert_eq!(
                    promote(&[left.clone(), right.clone()]).ok(),
                    promote(&[right.clone(), left.clone()]).ok(),
                    "{left:?} and {right:?} promote two ways"
                );
            }
            assert_eq!(promote(&[left.clone(), left.clone()]).unwrap(), left);
        }
    }

    #[test]
    fn every_promotion_is_a_cast_arrow_performs() {
        for left in arrow_types() {
            for right in arrow_types() {
                if let Ok(result) = promote(&[left.clone(), right.clone()]) {
                    for operand in [&left, &right] {
                        assert!(
                            can_cast_types(operand, &result),
                            "{left:?} and {right:?} give {result:?}, and Arrow casts no \
                             {operand:?} to it"
                        );
                    }
                }
            }
        }
    }

    /// Checked with numpy 2.5.2 over every permutation of each set.
    #[test]
    fn a_set_promotes_as_numpy_result_type_does() {
        for (set, expected) in [
            (
                vec![DataType::Int8, DataType::UInt8, DataType::Float16],
                DataType::Float16,
            ),
            (
                vec![DataType::Int16, DataType::UInt16, DataType::Float32],
                DataType::Float32,
            ),
            (
                vec![
                    DataType::Int8,
                    DataType::Int16,
                    DataType::UInt16,
                    DataType::Float16,
                ],
                DataType::Float32,
            ),
            (
                vec![DataType::Int8, DataType::UInt16, DataType::Float16],
                DataType::Float32,
            ),
            (
                vec![DataType::Int32, DataType::UInt32, DataType::Float32],
                DataType::Float64,
            ),
            (
                vec![DataType::Boolean, DataType::Int8, DataType::Float16],
                DataType::Float16,
            ),
            (
                vec![DataType::Int8, DataType::UInt8, DataType::UInt64],
                DataType::Float64,
            ),
            (
                vec![DataType::Int64, DataType::UInt64, DataType::Float32],
                DataType::Float64,
            ),
            (
                vec![DataType::Int32, DataType::Float64, DataType::Utf8],
                DataType::Utf8,
            ),
            (
                vec![
                    DataType::Date32,
                    DataType::Date64,
                    DataType::Timestamp(TimeUnit::Second, None),
                ],
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ),
        ] {
            for order in permutations(&set) {
                assert_eq!(promote(&order).unwrap(), expected, "{order:?} as one set");
            }
        }
    }

    #[test]
    fn the_listing_order_does_not_change_the_result() {
        let a = schema(&[("v", DataType::Int8), ("w", DataType::Utf8)]);
        let b = schema(&[("v", DataType::UInt8), ("w", DataType::Int64)]);
        let c = schema(&[("v", DataType::Float16), ("w", DataType::Float64)]);

        for order in permutations(&[a, b, c]) {
            let merged = numpy().merge_schemas(&order).unwrap();
            assert_eq!(field_of(&merged, "v").data_type(), &DataType::Float16);
            assert_eq!(field_of(&merged, "w").data_type(), &DataType::Utf8);
        }
    }

    #[test]
    fn a_repeated_schema_changes_nothing() {
        let a = schema(&[("v", DataType::Int8)]);
        let b = schema(&[("v", DataType::Float16)]);
        let once = numpy().merge_schemas(&[a.clone(), b.clone()]).unwrap();
        let thrice = numpy()
            .merge_schemas(&[a.clone(), b.clone(), a.clone(), a, b])
            .unwrap();
        assert_eq!(once, thrice);
    }

    fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
        if items.len() <= 1 {
            return vec![items.to_vec()];
        }
        let mut all = Vec::new();
        for (index, item) in items.iter().enumerate() {
            let mut rest = items.to_vec();
            rest.remove(index);
            for mut tail in permutations(&rest) {
                tail.insert(0, item.clone());
                all.push(tail);
            }
        }
        all
    }

    #[test]
    fn numpy_answers_where_the_default_answers_otherwise() {
        for (left, right, numpy_type, default_type) in [
            (
                DataType::Int16,
                DataType::Float32,
                DataType::Float32,
                DataType::Float64,
            ),
            (
                DataType::UInt8,
                DataType::Float32,
                DataType::Float32,
                DataType::Float64,
            ),
        ] {
            assert_eq!(
                promote(&[left.clone(), right.clone()]).unwrap(),
                numpy_type,
                "{left:?} and {right:?} under numpy"
            );
            let merged = ArrowTypeWidening::default_extension()
                .merge_schemas(&[
                    schema(&[("a", left.clone())]),
                    schema(&[("a", right.clone())]),
                ])
                .unwrap();
            assert_eq!(
                field_of(&merged, "a").data_type(),
                &default_type,
                "{left:?} and {right:?} under the default"
            );
        }
    }

    #[test]
    fn numpy_promotes_what_the_default_refuses() {
        for (left, right, expected) in [
            (DataType::Boolean, DataType::Int32, DataType::Int32),
            (DataType::Boolean, DataType::Float64, DataType::Float64),
            (DataType::Int8, DataType::Float16, DataType::Float16),
            (DataType::Float16, DataType::Float64, DataType::Float64),
            (DataType::Int32, DataType::Utf8, DataType::Utf8),
            (DataType::Float64, DataType::LargeUtf8, DataType::LargeUtf8),
            (DataType::Boolean, DataType::Utf8View, DataType::Utf8View),
            (
                DataType::Date32,
                DataType::Timestamp(TimeUnit::Second, None),
                DataType::Timestamp(TimeUnit::Second, None),
            ),
            (
                DataType::Date64,
                DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            ),
            (
                DataType::Duration(TimeUnit::Second),
                DataType::Duration(TimeUnit::Nanosecond),
                DataType::Duration(TimeUnit::Nanosecond),
            ),
        ] {
            assert_eq!(
                promote(&[left.clone(), right.clone()]).unwrap(),
                expected,
                "{left:?} and {right:?}"
            );
            assert!(
                ArrowTypeWidening::default_extension()
                    .merge_schemas(&[
                        schema(&[("a", left.clone())]),
                        schema(&[("a", right.clone())])
                    ])
                    .is_err(),
                "the default strategy promotes {left:?} and {right:?}"
            );
        }
    }

    #[test]
    fn numpy_keeps_the_rules_of_the_default() {
        for (left, right, expected) in [
            (DataType::Int32, DataType::Int64, DataType::Int64),
            (DataType::UInt16, DataType::Int8, DataType::Int32),
            (DataType::Int64, DataType::UInt64, DataType::Float64),
            (DataType::Int32, DataType::Float64, DataType::Float64),
            (DataType::Utf8, DataType::LargeUtf8, DataType::LargeUtf8),
            (DataType::Binary, DataType::BinaryView, DataType::BinaryView),
            (DataType::Date32, DataType::Date64, DataType::Date64),
            (
                DataType::Time32(TimeUnit::Second),
                DataType::Time64(TimeUnit::Nanosecond),
                DataType::Time64(TimeUnit::Nanosecond),
            ),
            (
                DataType::Timestamp(TimeUnit::Second, Some("Europe/Berlin".into())),
                DataType::Timestamp(TimeUnit::Nanosecond, Some("America/Lima".into())),
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            ),
            (
                DataType::Timestamp(TimeUnit::Second, None),
                DataType::Timestamp(TimeUnit::Millisecond, Some("Europe/Berlin".into())),
                DataType::Timestamp(TimeUnit::Millisecond, Some("Europe/Berlin".into())),
            ),
        ] {
            assert_eq!(
                promote(&[left.clone(), right.clone()]).unwrap(),
                expected,
                "{left:?} and {right:?}"
            );
        }
    }

    #[test]
    fn a_null_column_takes_the_other_type() {
        for other in [DataType::Int32, DataType::Utf8, DataType::Date32] {
            assert_eq!(
                promote(&[DataType::Null, other.clone()]).unwrap(),
                other,
                "{other:?}"
            );
        }
        assert_eq!(
            promote(&[DataType::Null, DataType::Null]).unwrap(),
            DataType::Null
        );
    }

    #[test]
    fn a_type_without_a_rule_merges_with_itself() {
        for data_type in [
            DataType::Decimal128(10, 2),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            DataType::Struct(Fields::from(vec![Field::new(
                "x",
                DataType::Float64,
                false,
            )])),
            DataType::FixedSizeBinary(16),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::Interval(IntervalUnit::DayTime),
        ] {
            assert_eq!(
                promote(&[data_type.clone(), data_type.clone()]).unwrap(),
                data_type
            );
            assert_eq!(
                promote(&[data_type.clone(), DataType::Null]).unwrap(),
                data_type
            );
            for other in [DataType::Int32, DataType::Utf8, DataType::Decimal128(12, 4)] {
                if other == data_type {
                    continue;
                }
                assert!(
                    promote(&[data_type.clone(), other.clone()]).is_err(),
                    "{data_type:?} and {other:?}"
                );
            }
        }
    }

    /// The type the family holds so far stands on the left.
    #[test]
    fn a_refused_column_names_both_files() {
        let narrow = from_file("a.nc", &[("depth", DataType::Int32)]);
        let wide = from_file("b.nc", &[("depth", DataType::Int64)]);
        let stamp = from_file(
            "c.nc",
            &[("depth", DataType::Timestamp(TimeUnit::Second, None))],
        );

        assert_eq!(
            message_of(numpy().merge_schemas(&[narrow, wide, stamp]).unwrap_err()),
            "Incompatible types for field 'depth': Int64 in 'b.nc' vs \
             Timestamp(Second, None) in 'c.nc'"
        );

        // A result no file states names no file.
        let signed = from_file("a.nc", &[("depth", DataType::Int64)]);
        let unsigned = from_file("b.nc", &[("depth", DataType::UInt64)]);
        let date = from_file("c.nc", &[("depth", DataType::Date32)]);
        assert_eq!(
            message_of(
                numpy()
                    .merge_schemas(&[signed, unsigned, date])
                    .unwrap_err()
            ),
            "Incompatible types for field 'depth': Float64 vs Date32 in 'c.nc'"
        );
    }

    #[test]
    fn keep_first_keeps_the_first_family() {
        let schemas = [
            from_file("a.nc", &[("v", DataType::Int64)]),
            from_file("b.nc", &[("v", DataType::Date32)]),
            from_file("c.nc", &[("v", DataType::Float64)]),
        ];
        assert!(numpy().merge_schemas(&schemas).is_err());

        let merged = keeping_first().merge_schemas(&schemas).unwrap();
        let field = field_of(&merged, "v");
        assert_eq!(field.data_type(), &DataType::Float64);
        assert!(field.is_nullable(), "the `Date32` file reads null");
        let strategy = NumpyArrowTypeWidening::keeping_first_type();
        assert!(strategy.casts_leniently(&DataType::Date32, &DataType::Float64));
        assert!(!strategy.casts_leniently(&DataType::Int64, &DataType::Float64));

        // The other order keeps the date.
        let merged = keeping_first()
            .merge_schemas(&[schemas[1].clone(), schemas[0].clone(), schemas[2].clone()])
            .unwrap();
        assert_eq!(field_of(&merged, "v").data_type(), &DataType::Date32);
        assert!(strategy.casts_leniently(&DataType::Int64, &DataType::Date32));
    }

    #[test]
    fn a_promoted_column_casts_strictly() {
        let schemas = [
            schema(&[("v", DataType::Int32)]),
            schema(&[("v", DataType::Utf8)]),
        ];
        for widening in [numpy(), keeping_first()] {
            let merged = widening.merge_schemas(&schemas).unwrap();
            assert_eq!(field_of(&merged, "v").data_type(), &DataType::Utf8);
            assert!(
                !widening
                    .strategy
                    .casts_leniently(&DataType::Int32, &DataType::Utf8)
            );
        }
        assert!(
            !NumpyArrowTypeWidening::new().casts_leniently(&DataType::Date32, &DataType::Int64)
        );
        assert_eq!(
            NumpyArrowTypeWidening::new().on_conflict(),
            TypeConflict::Fail
        );
        assert_eq!(
            NumpyArrowTypeWidening::keeping_first_type().on_conflict(),
            TypeConflict::KeepFirst
        );
    }

    #[test]
    fn merging_no_schemas_is_an_error() {
        assert!(matches!(
            numpy().merge_schemas(&[]).unwrap_err(),
            ArrowError::SchemaError(_)
        ));
    }

    #[test]
    fn fields_keep_first_seen_order_and_first_metadata() {
        let first = LabeledSchema::unlabeled(Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false)
                .with_metadata([("k".to_string(), "first".to_string())].into()),
        ])));
        let second = LabeledSchema::unlabeled(Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int64, false)
                .with_metadata([("k".to_string(), "second".to_string())].into()),
            Field::new("c", DataType::Float64, false),
        ])));

        let merged = numpy().merge_schemas(&[first, second]).unwrap();
        let names: Vec<&str> = merged.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["a", "b", "c"]);
        let b = field_of(&merged, "b");
        assert_eq!(b.data_type(), &DataType::Utf8);
        assert_eq!(b.metadata().get("k").map(String::as_str), Some("first"));
        assert!(!b.is_nullable());
        assert!(field_of(&merged, "a").is_nullable());
        assert!(field_of(&merged, "c").is_nullable());
    }

    #[test]
    fn one_nullable_file_makes_the_column_nullable() {
        let required = LabeledSchema::unlabeled(Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Int32,
            false,
        )])));
        let optional = LabeledSchema::unlabeled(Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Int64,
            true,
        )])));
        for order in [
            [required.clone(), optional.clone()],
            [optional, required.clone()],
        ] {
            let merged = numpy().merge_schemas(&order).unwrap();
            assert!(field_of(&merged, "v").is_nullable());
        }
        let alone = numpy().merge_schemas(&[required]).unwrap();
        assert!(!field_of(&alone, "v").is_nullable());
    }

    #[test]
    fn the_method_reads_every_schema_in_one_fold() {
        assert!(!NumpyArrowTypeWidening::new().is_order_independent());
        assert!(!NumpyArrowTypeWidening::keeping_first_type().is_order_independent());
    }
}
