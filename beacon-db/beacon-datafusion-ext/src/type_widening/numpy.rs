//! The numpy strategy: the promotion rules of `numpy.result_type`.
//!
//! [`NumpyArrowTypeWidening`] merges the schemas of a table with the rules that
//! numpy applies to arrays of two types. `RuntimeBuilder::with_type_widening`
//! takes it, and `BEACON_TYPE_WIDENING_STRATEGY=numpy` selects it on the server.
//! The [parent module](super) holds the default strategy and the merge contract.
//! The field union, the first seen order, the nullability rule and the
//! [`TypeConflict`] setting hold under both strategies.
//!
//! # The rules
//!
//! numpy sorts its types into kinds, and a kind absorbs the kinds below it. Two
//! files, one column name:
//!
//! | The files state | The table reports | numpy |
//! | --- | --- | --- |
//! | `Boolean` and a number | that number | `bool` + `int8` is `int8` |
//! | two integers of one sign | the wider one | `int8` + `int16` is `int16` |
//! | two integers of two signs | the signed type of the next width, which holds the unsigned one | `uint8` + `int8` is `int16`, `uint16` + `int32` is `int32` |
//! | `UInt64` and a signed integer | `Float64`, because no integer holds both | `uint64` + `int8` is `float64` |
//! | an integer and a float | the float whose mantissa holds the integer, or the wider of the two | `int8` + `float16` is `float16`, `int16` + `float16` is `float32`, `int32` + `float32` is `float64` |
//! | two floats | the wider one | `float16` + `float32` is `float32` |
//! | a number or a boolean, and a string | the string. The scan writes each number as text | `int32` + `str` is `str` |
//! | two strings | the wider layout: `Utf8`, `Utf8View`, `LargeUtf8` | one `str` |
//! | two timestamps, or a date and a timestamp | a timestamp at the finer unit | `datetime64[D]` + `datetime64[s]` is `datetime64[s]` |
//! | `Date32` and `Date64` | `Date64` | `datetime64[D]` + `datetime64[ms]` is `datetime64[ms]` |
//! | two durations | the finer unit | `timedelta64[s]` + `timedelta64[ms]` is `timedelta64[ms]` |
//! | `Null` and any type | that type | no rule: a null column holds no value |
//!
//! Every other pair is a conflict, and [`TypeConflict`] settles it. A number
//! beside a timestamp, a string beside a timestamp, and a string beside a
//! duration are conflicts in numpy too.
//!
//! **`Float16` joins the float chain.** The default strategy has no rule for it.
//!
//! **A time zone follows the default strategy.** numpy has no zone. One zone wins
//! over none, and two zones give `UTC`.
//!
//! # The set decides, not the pair
//!
//! numpy promotes a set of types at once, and the answer differs from a chain of
//! pairs. `int8` + `uint8` is `int16`, and `int16` + `float16` is `float32`. Yet
//! `numpy.result_type(int8, uint8, float16)` is `float16`, because a `float16`
//! holds both `int8` and `uint8`. This strategy gathers the types of each column
//! across every schema and resolves the set once. Its answer matches
//! `numpy.result_type`, and the listing order does not change it.
//!
//! A chunk result hides the set behind one type. The strategy therefore answers
//! `false` to [`is_order_independent`], and the entry point gives it one fold
//! over every schema. The dedup and the threads of the default strategy do not
//! apply. A collection of 100000 files takes one pass, as `keep_first` does.
//!
//! # A CSV file states no types
//!
//! The CSV reader parses each column as the merged type, because a text file
//! holds no type of its own. A number beside a string therefore reads as the
//! text of the file: `2`, not `2.0`. A boolean literal beside a number parses
//! as no number, so two CSV files that hold `true` and `7` in one column fail at
//! read time under this strategy. The default strategy refuses the same pair at
//! plan time. Every typed format casts the value instead, and `true` reads as
//! `1`.
//!
//! # Where this strategy leaves numpy
//!
//! The rules above follow numpy where Arrow has the type and the cast. Four
//! rules stay behind:
//!
//! - numpy reads an integer or a boolean beside a `timedelta64` as a
//!   `timedelta64` (a `uint64` aside), and a `timedelta64` beside a
//!   `datetime64` as a `datetime64`. Arrow casts no such column, so both pairs
//!   are conflicts here.
//! - numpy writes a number as ASCII bytes beside a byte string (`S`), and reads
//!   a byte string beside a text string as text. An Arrow binary column holds
//!   any bytes, and Arrow casts no number to it. The binary family therefore
//!   widens with itself alone, as in the default strategy.
//! - numpy has no time of day. `Time32` and `Time64` keep the chain of the
//!   default strategy.
//! - numpy has no decimal, no list, no struct and no dictionary. Such a type
//!   widens with itself alone.
//!
//! [`is_order_independent`]: super::ArrowTypeWideningStrategy::is_order_independent

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{ArrowError, DataType, FieldRef, Schema, SchemaRef, TimeUnit};

use super::{
    ArrowTypeWideningStrategy, Chain, LabeledSchema, TypeConflict, chain_member, chain_rank,
    incompatible_types, mark_type_conflict, time_unit_rank, zone_join,
};

/// Merge the schemas of a table with the promotion rules of numpy.
///
/// The [module docs](self) hold the rule table. The merge contract is the one of
/// [`DefaultArrowTypeWidening`](super::DefaultArrowTypeWidening). The fields keep
/// first seen order. A field keeps the metadata of the first source. A field is
/// nullable unless every schema holds it and every schema requires it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NumpyArrowTypeWidening {
    /// What the merge does with a column that two sources type in two families.
    pub on_conflict: TypeConflict,
}

impl NumpyArrowTypeWidening {
    /// The rule that refuses a column two sources type in two families.
    pub const fn new() -> Self {
        Self {
            on_conflict: TypeConflict::Fail,
        }
    }

    /// The rule that keeps the first type of such a column. See
    /// [`TypeConflict::KeepFirst`].
    pub const fn keeping_first_type() -> Self {
        Self {
            on_conflict: TypeConflict::KeepFirst,
        }
    }
}

/// One column, as the fold gathers it over every schema.
struct Column {
    /// The field of the first source that holds the column. The result keeps its
    /// name and its metadata.
    first: FieldRef,
    /// Each distinct type the sources state, in first seen order, with the first
    /// source that states it.
    types: Vec<(DataType, Option<Arc<str>>)>,
    /// How many schemas hold the column.
    held_by: usize,
    /// Whether one schema permits nulls.
    nullable: bool,
}

impl ArrowTypeWideningStrategy for NumpyArrowTypeWidening {
    fn is_order_independent(&self) -> bool {
        // The set of types decides, and a chunk result hides the set. See the
        // module docs. `KeepFirst` reads the order as well.
        false
    }

    fn merge_schemas(&self, schemas: &[LabeledSchema]) -> Result<SchemaRef, ArrowError> {
        if schemas.is_empty() {
            return Err(ArrowError::SchemaError(
                "No schemas provided for merging".to_string(),
            ));
        }

        let mut columns: Vec<Column> = Vec::new();
        let mut positions: HashMap<String, usize> = HashMap::new();
        for labeled in schemas {
            for field in labeled.schema.fields() {
                match positions.get(field.name()) {
                    Some(&at) => {
                        let column = &mut columns[at];
                        column.held_by += 1;
                        column.nullable |= field.is_nullable();
                        if !column
                            .types
                            .iter()
                            .any(|(data_type, _)| data_type == field.data_type())
                        {
                            column
                                .types
                                .push((field.data_type().clone(), labeled.label.clone()));
                        }
                    }
                    None => {
                        positions.insert(field.name().clone(), columns.len());
                        columns.push(Column {
                            first: Arc::clone(field),
                            types: vec![(field.data_type().clone(), labeled.label.clone())],
                            held_by: 1,
                            nullable: field.is_nullable(),
                        });
                    }
                }
            }
        }

        let mut fields = Vec::with_capacity(columns.len());
        for column in columns {
            let resolved = resolve(column.first.name(), &column.types, self.on_conflict)?;
            let mut field = column.first.as_ref().clone();
            if &resolved.data_type != column.first.data_type() {
                field = field.with_data_type(resolved.data_type);
            }
            // One file that permits nulls makes the column nullable. So does a
            // file that lacks the column, because the scan fills it with nulls.
            if column.nullable || column.held_by < schemas.len() {
                field = field.with_nullable(true);
            }
            if resolved.conflict {
                field = mark_type_conflict(field);
            }
            fields.push(Arc::new(field));
        }

        Ok(Arc::new(Schema::new(fields)))
    }
}

/// The type of one column, and whether the setting settled it.
struct Resolved {
    data_type: DataType,
    conflict: bool,
}

/// The type that holds every member of `types`, as `numpy.result_type` reports
/// it.
///
/// `Null` holds no value, so it takes no part. The first stated type names the
/// family. A type of another family is a conflict, and `on_conflict` settles it.
/// `Fail` reports the type the family holds so far beside the offender.
/// `KeepFirst` drops the offender and marks the column.
fn resolve(
    name: &str,
    types: &[(DataType, Option<Arc<str>>)],
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
                // The source that states the type the family holds so far. A
                // result that no source states has none.
                let so_far_source = types
                    .iter()
                    .find(|(stated, _)| stated == &so_far)
                    .and_then(|(_, source)| source.as_deref());
                return Err(ArrowError::SchemaError(incompatible_types(
                    name,
                    &so_far,
                    so_far_source,
                    data_type,
                    source.as_deref(),
                )));
            }
            TypeConflict::KeepFirst => conflict = true,
        }
    }

    Ok(Resolved {
        data_type: family.result(&members),
        conflict,
    })
}

/// The kinds that promote with one another. Two families never promote.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Family {
    /// A boolean, an integer, a float or a string. numpy writes a number as
    /// text beside a string, so the four kinds share one family.
    NumberOrText,
    /// A date or a timestamp: a `datetime64` at one unit.
    Datetime,
    /// A `timedelta64` at one unit.
    Duration,
    /// A time of day. numpy has none, so the chain of the default strategy holds.
    Time,
    /// A byte string. The module docs state why no number joins it.
    Binary,
    /// A type numpy has no rule for. It promotes with itself alone.
    Other(DataType),
}

impl Family {
    fn of(data_type: &DataType) -> Self {
        if let Some((_, chain)) = chain_rank(data_type) {
            return match chain {
                Chain::String => Self::NumberOrText,
                Chain::Binary => Self::Binary,
                Chain::Date => Self::Datetime,
                Chain::Time => Self::Time,
            };
        }
        match data_type {
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64 => Self::NumberOrText,
            DataType::Timestamp(_, _) => Self::Datetime,
            DataType::Duration(_) => Self::Duration,
            other => Self::Other(other.clone()),
        }
    }

    /// The type that holds every member. Each member belongs to this family.
    fn result(&self, members: &[&DataType]) -> DataType {
        match self {
            Self::NumberOrText => number_or_text(members),
            Self::Datetime => datetime(members),
            Self::Duration => {
                let finest = members
                    .iter()
                    .filter_map(|member| match member {
                        DataType::Duration(unit) => Some(time_unit_rank(unit)),
                        _ => None,
                    })
                    .max()
                    .unwrap_or(0);
                DataType::Duration(unit_at(finest))
            }
            Self::Time | Self::Binary => members
                .iter()
                .filter_map(|member| chain_rank(member))
                .max_by_key(|(rank, _)| *rank)
                .map(|(rank, chain)| chain_member(chain, rank))
                .unwrap_or_else(|| members[0].clone()),
            Self::Other(data_type) => data_type.clone(),
        }
    }
}

/// `numpy.result_type` over booleans, integers, floats and strings.
///
/// A string absorbs every number, and the scan writes the number as text. A
/// float absorbs every integer, at the width whose mantissa holds it: 11 bits
/// hold an 8-bit integer, 24 bits hold a 16-bit one, and 53 bits hold the rest.
/// Two signs meet in the signed type of the next width, and in `Float64` above
/// `UInt64`. A boolean adds nothing.
fn number_or_text(members: &[&DataType]) -> DataType {
    let mut text: Option<u8> = None;
    let mut float_bits = 0u8;
    let mut signed_bits = 0u8;
    let mut unsigned_bits = 0u8;
    for member in members {
        match member {
            DataType::Boolean => {}
            DataType::Int8 => signed_bits = signed_bits.max(8),
            DataType::Int16 => signed_bits = signed_bits.max(16),
            DataType::Int32 => signed_bits = signed_bits.max(32),
            DataType::Int64 => signed_bits = signed_bits.max(64),
            DataType::UInt8 => unsigned_bits = unsigned_bits.max(8),
            DataType::UInt16 => unsigned_bits = unsigned_bits.max(16),
            DataType::UInt32 => unsigned_bits = unsigned_bits.max(32),
            DataType::UInt64 => unsigned_bits = unsigned_bits.max(64),
            DataType::Float16 => float_bits = float_bits.max(16),
            DataType::Float32 => float_bits = float_bits.max(32),
            DataType::Float64 => float_bits = float_bits.max(64),
            other => {
                if let Some((rank, Chain::String)) = chain_rank(other) {
                    text = Some(text.map_or(rank, |widest| widest.max(rank)));
                }
            }
        }
    }

    if let Some(rank) = text {
        return chain_member(Chain::String, rank);
    }
    if float_bits > 0 {
        // The float whose mantissa holds an integer of this width.
        let holds = |int_bits: u8| match int_bits {
            0 => 0,
            1..=8 => 16,
            9..=16 => 32,
            _ => 64,
        };
        return float_of(float_bits.max(holds(signed_bits)).max(holds(unsigned_bits)));
    }
    match (signed_bits, unsigned_bits) {
        (0, 0) => DataType::Boolean,
        (signed, 0) => signed_of(signed),
        (0, unsigned) => unsigned_of(unsigned),
        // No signed integer holds a `UInt64`.
        (_, 64) => DataType::Float64,
        (signed, unsigned) => signed_of(signed.max(unsigned * 2)),
    }
}

fn signed_of(bits: u8) -> DataType {
    match bits {
        8 => DataType::Int8,
        16 => DataType::Int16,
        32 => DataType::Int32,
        _ => DataType::Int64,
    }
}

fn unsigned_of(bits: u8) -> DataType {
    match bits {
        8 => DataType::UInt8,
        16 => DataType::UInt16,
        32 => DataType::UInt32,
        _ => DataType::UInt64,
    }
}

fn float_of(bits: u8) -> DataType {
    match bits {
        16 => DataType::Float16,
        32 => DataType::Float32,
        _ => DataType::Float64,
    }
}

/// `numpy.result_type` over dates and timestamps: the finer unit holds both.
///
/// `Date32` counts days and `Date64` counts milliseconds, so a date is a
/// `datetime64[D]` or a `datetime64[ms]`. One timestamp makes the result a
/// timestamp. Its unit is the finest of the members, and `Second` at least,
/// because a timestamp counts no days. The zone follows the default strategy.
fn datetime(members: &[&DataType]) -> DataType {
    // Days sit below every timestamp unit. A timestamp unit sits one above its
    // `time_unit_rank`, and `Date64` shares the rank of `Millisecond`.
    const DAY: u8 = 0;
    let mut timestamp = false;
    let mut rank = DAY;
    let mut zone: Option<Option<Arc<str>>> = None;
    for member in members {
        match member {
            DataType::Date32 => {}
            DataType::Date64 => rank = rank.max(time_unit_rank(&TimeUnit::Millisecond) + 1),
            DataType::Timestamp(unit, member_zone) => {
                timestamp = true;
                rank = rank.max(time_unit_rank(unit) + 1);
                zone = Some(match zone {
                    None => member_zone.clone(),
                    Some(so_far) => zone_join(&so_far, member_zone),
                });
            }
            _ => {}
        }
    }

    if timestamp {
        DataType::Timestamp(unit_at(rank.max(1) - 1), zone.unwrap_or(None))
    } else if rank == DAY {
        DataType::Date32
    } else {
        DataType::Date64
    }
}

/// The unit at `rank`. The inverse of [`time_unit_rank`].
fn unit_at(rank: u8) -> TimeUnit {
    match rank {
        0 => TimeUnit::Second,
        1 => TimeUnit::Millisecond,
        2 => TimeUnit::Microsecond,
        _ => TimeUnit::Nanosecond,
    }
}

#[cfg(test)]
mod tests {
    use arrow::compute::can_cast_types;
    use arrow_schema::{Field, Fields, IntervalUnit};

    use super::super::{ArrowTypeWidening, is_type_conflict};
    use super::*;

    /// A schema without a source name.
    fn schema(fields: &[(&str, DataType)]) -> LabeledSchema {
        LabeledSchema::unlabeled(schema_ref(fields))
    }

    /// A schema with the name of the file it came from.
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

    /// The column of a merge, by name.
    fn field_of<'a>(schema: &'a Schema, name: &str) -> &'a FieldRef {
        schema
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .unwrap_or_else(|| panic!("the merge holds '{name}'"))
    }

    /// The type the strategy reports for column `a` over one schema per type.
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

    // ── the oracle ─────────────────────────────────────────────────────

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

    /// What numpy reports for a pair, by numpy name.
    fn numpy_promote_types(left: &str, right: &str) -> &'static str {
        let at = |name: &str| {
            NUMPY_TYPES
                .iter()
                .position(|candidate| *candidate == name)
                .unwrap_or_else(|| panic!("{name} is not a numpy type of the oracle"))
        };
        NUMPY_PROMOTE_TYPES[at(left)][at(right)]
    }

    /// The numpy name of an Arrow type. Two Arrow types can share one name:
    /// `Date64` and `Timestamp(Millisecond)` are both `M8[ms]`, and every string
    /// layout is `U`.
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

    /// Every Arrow type that has a numpy name.
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

    /// The pairs numpy promotes and this strategy refuses. The module docs state
    /// each reason.
    fn leaves_numpy(left: &str, right: &str) -> bool {
        let kind = |name: &str| name.chars().next().unwrap();
        // numpy reads a boolean or an integer as a count of time units. A
        // `uint64` and a float are no such count.
        let counts = |name: &str| matches!(kind(name), 'b' | 'i' | 'u') && name != "uint64";
        // numpy writes a boolean, a number or a text string as ASCII bytes.
        let spells = |name: &str| matches!(kind(name), 'b' | 'i' | 'u' | 'f' | 'U');
        // An integer or a boolean beside a duration.
        (counts(left) && kind(right) == 'm')
            || (kind(left) == 'm' && counts(right))
            // A duration beside a datetime.
            || (kind(left) == 'm' && kind(right) == 'M')
            || (kind(left) == 'M' && kind(right) == 'm')
            // A byte string beside a text string or a number.
            || (kind(left) == 'S' && spells(right))
            || (kind(right) == 'S' && spells(left))
    }

    /// Every pair of two Arrow types promotes as `numpy.promote_types` does, or
    /// sits in the list the module docs give.
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

    /// The oracle is symmetric, and so is the strategy. Each pair reads the same
    /// in both orders, and one type beside itself stays as it is.
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

    /// Every type the strategy reports is a cast Arrow performs on both operands.
    /// A merge that reports a type the scan cannot reach fails on the first
    /// batch instead of at plan time.
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

    // ── the set decides ────────────────────────────────────────────────

    /// The results `numpy.result_type` gives for these sets, checked with numpy
    /// 2.5.2 over every permutation. A chain of pairs gives a wider type for
    /// the first three.
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

    /// A chain of pairs gives what it gives. The set gives the numpy answer, so
    /// the listing order does not change the result.
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

    /// A repeated schema adds no type, so it changes no result.
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

    /// Every order of `items`.
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

    // ── the rules the default strategy lacks ─────────────────────────────

    /// The pairs both strategies promote, and to two types. The default strategy
    /// takes `Float64` for every integer beside a `Float32`, because a lattice
    /// holds no other answer. numpy keeps the `Float32` that holds the integer.
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

    /// The pairs the default strategy refuses and numpy promotes.
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

    /// The rules both strategies share.
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

    /// A `Null` column holds no value, so it takes the type of the other file.
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

    /// A type numpy has no rule for merges with itself alone.
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

    // ── the setting for a column that no type holds ────────────────────

    /// A refused column names both files, with the type the family holds so far
    /// on the left.
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

    /// `KeepFirst` keeps the family of the first file, marks the column, and
    /// still promotes the later files of that family.
    #[test]
    fn keep_first_keeps_the_first_family_and_marks_the_column() {
        let schemas = [
            from_file("a.nc", &[("v", DataType::Int64)]),
            from_file("b.nc", &[("v", DataType::Date32)]),
            from_file("c.nc", &[("v", DataType::Float64)]),
        ];
        assert!(numpy().merge_schemas(&schemas).is_err());

        let merged = keeping_first().merge_schemas(&schemas).unwrap();
        let field = field_of(&merged, "v");
        assert_eq!(field.data_type(), &DataType::Float64);
        assert!(is_type_conflict(field), "the `Date32` file casts to null");
        assert!(field.is_nullable());

        // The other order keeps the date.
        let merged = keeping_first()
            .merge_schemas(&[schemas[1].clone(), schemas[0].clone(), schemas[2].clone()])
            .unwrap();
        assert_eq!(field_of(&merged, "v").data_type(), &DataType::Date32);
        assert!(is_type_conflict(field_of(&merged, "v")));
    }

    /// A column that promotes carries no mark under either setting.
    #[test]
    fn a_promoted_column_carries_no_mark() {
        let schemas = [
            schema(&[("v", DataType::Int32)]),
            schema(&[("v", DataType::Utf8)]),
        ];
        for widening in [numpy(), keeping_first()] {
            let merged = widening.merge_schemas(&schemas).unwrap();
            let field = field_of(&merged, "v");
            assert_eq!(field.data_type(), &DataType::Utf8);
            assert!(!is_type_conflict(field));
        }
    }

    // ── the merge contract ─────────────────────────────────────────────

    #[test]
    fn merging_no_schemas_is_an_error() {
        assert!(matches!(
            numpy().merge_schemas(&[]).unwrap_err(),
            ArrowError::SchemaError(_)
        ));
    }

    /// The fields keep first seen order, and a field keeps the metadata of the
    /// first file.
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
        // Every file holds `b` and requires it.
        assert!(!b.is_nullable());
        // One file lacks `a` and `c`.
        assert!(field_of(&merged, "a").is_nullable());
        assert!(field_of(&merged, "c").is_nullable());
    }

    /// One file that permits nulls makes the column nullable, in either order.
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

    /// The set decides, so the entry point may not split the schemas.
    #[test]
    fn the_method_reads_every_schema_in_one_fold() {
        assert!(!NumpyArrowTypeWidening::new().is_order_independent());
        assert!(!NumpyArrowTypeWidening::keeping_first_type().is_order_independent());
    }
}
