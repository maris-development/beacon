//! The default strategy: widen inside a family, refuse across families.
//!
//! Two types of one family join in the member that holds both: the wider
//! integer, float, string, binary or date, and the finer time or timestamp
//! unit. Two signs meet in the signed type of the next width, and `Int64`
//! beside `UInt64` in `Float64`. An integer beside a float gives `Float64`.
//! Two families are a conflict, and [`TypeConflict`] settles it. `Boolean`,
//! `Float16` and every other type join with themselves alone.

use std::sync::Arc;

use arrow_schema::{ArrowError, DataType, SchemaRef};

use super::common::{
    self, Resolved, StatedTypes, finer_unit, incompatible_types, integer_join, wider, zone_join,
};
use super::{ArrowTypeWideningStrategy, LabeledSchema, TypeConflict};

/// Merges the schemas of a table with the rules of the [module docs](self).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DefaultArrowTypeWidening {
    /// What the merge does with a column that no type holds.
    pub on_conflict: TypeConflict,
}

impl DefaultArrowTypeWidening {
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

impl ArrowTypeWideningStrategy for DefaultArrowTypeWidening {
    fn is_order_independent(&self) -> bool {
        // `KeepFirst` reads the order: the first type wins.
        matches!(self.on_conflict, TypeConflict::Fail)
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
        // A pair the join widens reached the scan on its own. Any other pair
        // got there through the setting.
        self.on_conflict == TypeConflict::KeepFirst
            && super_type(source, target).as_ref() != Some(target)
    }
}

/// The type that holds every stated type, one pair at a time. A pair no type
/// holds is a conflict, and `on_conflict` settles it.
fn resolve(
    name: &str,
    types: &StatedTypes,
    on_conflict: TypeConflict,
) -> Result<Resolved, ArrowError> {
    let mut current = DataType::Null;
    // The source that states `current`.
    let mut source: Option<Arc<str>> = None;
    let mut conflict = false;
    for (stated, stated_source) in types {
        match super_type(&current, stated) {
            Some(widened) => {
                // A join that equals neither operand has no source.
                if widened != current {
                    source = (widened == *stated)
                        .then(|| stated_source.clone())
                        .flatten();
                }
                current = widened;
            }
            None => match on_conflict {
                TypeConflict::Fail => {
                    return Err(incompatible_types(
                        name,
                        &current,
                        source.as_deref(),
                        stated,
                        stated_source.as_deref(),
                    ));
                }
                TypeConflict::KeepFirst => conflict = true,
            },
        }
    }
    Ok(Resolved {
        data_type: current,
        conflict,
    })
}

/// The type that holds both, or `None` when no type does.
fn super_type(left: &DataType, right: &DataType) -> Option<DataType> {
    use DataType::*;
    Some(match (left, right) {
        (left, right) if left == right => left.clone(),
        (Null, other) | (other, Null) => other.clone(),
        (Timestamp(left_unit, left_zone), Timestamp(right_unit, right_zone)) => Timestamp(
            finer_unit(*left_unit, *right_unit),
            zone_join(left_zone, right_zone),
        ),
        (Utf8 | Utf8View | LargeUtf8, Utf8 | Utf8View | LargeUtf8)
        | (Binary | BinaryView | LargeBinary, Binary | BinaryView | LargeBinary)
        | (Date32 | Date64, Date32 | Date64)
        | (Time32(_) | Time64(_), Time32(_) | Time64(_))
        | (Float32 | Float64, Float32 | Float64) => wider(left, right).clone(),
        // A 24-bit mantissa holds no `Int32`, so an integer beside a float
        // takes `Float64`. `Float16` has no rule here.
        (Float32 | Float64, Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64)
        | (Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64, Float32 | Float64) => {
            Float64
        }
        (
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64,
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64,
        ) => integer_join(left, right),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use arrow_schema::{Field, FieldRef, Fields, IntervalUnit, Schema, TimeUnit};

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

    fn failing() -> DefaultArrowTypeWidening {
        DefaultArrowTypeWidening::new()
    }

    fn keeping_first() -> DefaultArrowTypeWidening {
        DefaultArrowTypeWidening::keeping_first_type()
    }

    fn field_of<'a>(schema: &'a Schema, name: &str) -> &'a FieldRef {
        schema
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .unwrap_or_else(|| panic!("the merge holds '{name}'"))
    }

    fn widen(types: &[DataType]) -> Result<DataType, ArrowError> {
        let schemas: Vec<LabeledSchema> = types
            .iter()
            .map(|data_type| schema(&[("a", data_type.clone())]))
            .collect();
        failing()
            .merge_schemas(&schemas)
            .map(|merged| field_of(&merged, "a").data_type().clone())
    }

    fn message_of(error: ArrowError) -> String {
        match error {
            ArrowError::SchemaError(message) => message,
            other => panic!("expected SchemaError, got {other:?}"),
        }
    }

    #[test]
    fn two_numeric_types_widen_in_either_order() {
        for order in [
            [DataType::Int32, DataType::Int64],
            [DataType::Int64, DataType::Int32],
        ] {
            assert_eq!(widen(&order).unwrap(), DataType::Int64);
        }
    }

    #[test]
    fn a_number_and_a_string_are_refused_in_either_order() {
        for order in [
            [DataType::Int32, DataType::Utf8],
            [DataType::Utf8, DataType::Int32],
        ] {
            let message = message_of(widen(&order).unwrap_err());
            assert!(
                message.contains("'a'"),
                "message should name the field: {message}"
            );
        }
    }

    #[test]
    fn the_setting_keeps_the_type_of_the_first_file() {
        let text = from_file("argo/a.nc", &[("depth", DataType::Utf8)]);
        let number = from_file("argo/b.nc", &[("depth", DataType::Float64)]);

        let merged = keeping_first()
            .merge_schemas(&[text, number])
            .expect("the setting settles the column");
        assert_eq!(field_of(&merged, "depth").data_type(), &DataType::Utf8);
    }

    #[test]
    fn the_setting_reads_the_order() {
        let text = from_file("argo/a.nc", &[("depth", DataType::Utf8)]);
        let number = from_file("argo/b.nc", &[("depth", DataType::Float64)]);

        let first = keeping_first()
            .merge_schemas(&[text.clone(), number.clone()])
            .expect("merge");
        let second = keeping_first()
            .merge_schemas(&[number.clone(), text.clone()])
            .expect("merge");
        assert_eq!(field_of(&first, "depth").data_type(), &DataType::Utf8);
        assert_eq!(field_of(&second, "depth").data_type(), &DataType::Float64);

        assert!(
            failing()
                .merge_schemas(&[text.clone(), number.clone()])
                .is_err()
        );
        assert!(failing().merge_schemas(&[number, text]).is_err());
    }

    #[test]
    fn a_settled_column_turns_nullable() {
        let required = |data_type: DataType| {
            LabeledSchema::unlabeled(Arc::new(Schema::new(vec![Field::new(
                "depth", data_type, false,
            )])))
        };

        let merged = keeping_first()
            .merge_schemas(&[required(DataType::Utf8), required(DataType::Float64)])
            .expect("merge");
        assert!(
            field_of(&merged, "depth").is_nullable(),
            "a null needs a nullable column"
        );
    }

    #[test]
    fn the_setting_changes_no_column_that_widens() {
        let narrow = schema(&[("v", DataType::Int32)]);
        let wide = schema(&[("v", DataType::Float64)]);

        let merged = keeping_first()
            .merge_schemas(&[narrow, wide])
            .expect("merge");
        assert_eq!(field_of(&merged, "v").data_type(), &DataType::Float64);
        assert!(
            !keeping_first().casts_leniently(&DataType::Int32, &DataType::Float64),
            "a widened column needs no lenient cast"
        );
    }

    #[test]
    fn a_later_widening_keeps_the_settled_family_lenient() {
        let schemas = [
            schema(&[("v", DataType::Int64)]),
            schema(&[("v", DataType::Utf8)]),
            schema(&[("v", DataType::Float64)]),
        ];

        let merged = keeping_first().merge_schemas(&schemas).expect("merge");
        let target = field_of(&merged, "v").data_type();
        assert_eq!(target, &DataType::Float64);
        assert!(keeping_first().casts_leniently(&DataType::Utf8, target));
        assert!(!keeping_first().casts_leniently(&DataType::Int64, target));
    }

    #[test]
    fn the_scan_asks_which_casts_read_null() {
        let list = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let lenient = [
            (DataType::Utf8, DataType::Float64),
            (DataType::Float64, DataType::Utf8),
            (list.clone(), DataType::Float64),
            (DataType::Boolean, DataType::Int32),
            // A table declared narrower than a file.
            (DataType::Int64, DataType::Int32),
        ];
        let strict = [
            (DataType::Int32, DataType::Int64),
            (DataType::Int64, DataType::Float64),
            (DataType::Utf8, DataType::LargeUtf8),
            (DataType::Null, DataType::Float64),
            (DataType::Float64, DataType::Float64),
            (
                DataType::Timestamp(TimeUnit::Second, None),
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            ),
        ];

        for (source, target) in lenient.iter().chain(strict.iter()) {
            assert!(
                !failing().casts_leniently(source, target),
                "{source:?} into {target:?} under Fail"
            );
        }
        for (source, target) in &lenient {
            assert!(
                keeping_first().casts_leniently(source, target),
                "{source:?} into {target:?} under KeepFirst"
            );
        }
        for (source, target) in &strict {
            assert!(
                !keeping_first().casts_leniently(source, target),
                "{source:?} into {target:?} under KeepFirst"
            );
        }
        assert_eq!(failing().on_conflict(), TypeConflict::Fail);
        assert_eq!(keeping_first().on_conflict(), TypeConflict::KeepFirst);
    }

    #[test]
    fn only_the_failing_rule_is_order_independent() {
        assert!(failing().is_order_independent());
        assert!(!keeping_first().is_order_independent());
    }

    #[test]
    fn a_refused_column_names_both_files() {
        let text = from_file("argo/a.nc", &[("depth", DataType::Utf8)]);
        let number = from_file("argo/b.nc", &[("depth", DataType::Float64)]);

        assert_eq!(
            message_of(failing().merge_schemas(&[text, number]).unwrap_err()),
            "Incompatible types for field 'depth': Utf8 in 'argo/a.nc' vs \
             Float64 in 'argo/b.nc'"
        );
    }

    #[test]
    fn a_schema_without_a_name_reports_its_type_alone() {
        let text = schema(&[("depth", DataType::Utf8)]);
        let number = from_file("argo/b.nc", &[("depth", DataType::Float64)]);

        assert_eq!(
            message_of(failing().merge_schemas(&[text, number]).unwrap_err()),
            "Incompatible types for field 'depth': Utf8 vs Float64 in 'argo/b.nc'"
        );
    }

    #[test]
    fn a_widened_column_names_the_file_of_its_current_type() {
        let narrow = from_file("a.nc", &[("depth", DataType::Int32)]);
        let wide = from_file("b.nc", &[("depth", DataType::Int64)]);
        let text = from_file("c.nc", &[("depth", DataType::Utf8)]);

        assert_eq!(
            message_of(failing().merge_schemas(&[narrow, wide, text]).unwrap_err()),
            "Incompatible types for field 'depth': Int64 in 'b.nc' vs Utf8 in 'c.nc'"
        );
    }

    #[test]
    fn a_join_of_two_files_names_neither() {
        let signed = from_file("a.nc", &[("depth", DataType::Int64)]);
        let unsigned = from_file("b.nc", &[("depth", DataType::UInt64)]);
        let text = from_file("c.nc", &[("depth", DataType::Utf8)]);

        assert_eq!(
            message_of(
                failing()
                    .merge_schemas(&[signed, unsigned, text])
                    .unwrap_err()
            ),
            "Incompatible types for field 'depth': Float64 vs Utf8 in 'c.nc'"
        );
    }

    #[test]
    fn a_null_column_takes_the_other_type() {
        for other in [DataType::Int32, DataType::Utf8, DataType::Date32] {
            assert_eq!(widen(&[DataType::Null, other.clone()]).unwrap(), other);
        }
        assert_eq!(
            widen(&[DataType::Null, DataType::Null]).unwrap(),
            DataType::Null
        );
    }

    #[test]
    fn a_type_without_a_rule_merges_with_itself() {
        let geometry = |y: DataType| {
            DataType::Struct(Fields::from(vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", y, false),
            ]))
        };

        for data_type in [
            geometry(DataType::Float64),
            DataType::Decimal128(10, 2),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            DataType::FixedSizeBinary(16),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::Duration(TimeUnit::Second),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Boolean,
            DataType::Float16,
        ] {
            assert_eq!(
                widen(&[data_type.clone(), data_type.clone()]).unwrap(),
                data_type,
                "{data_type:?} with itself"
            );
            assert_eq!(
                widen(&[data_type.clone(), DataType::Null]).unwrap(),
                data_type,
                "{data_type:?} with Null"
            );
            assert!(
                widen(&[data_type.clone(), DataType::Int32]).is_err(),
                "{data_type:?} and Int32"
            );
        }

        // Type equality reads the whole struct.
        assert!(
            widen(&[geometry(DataType::Float64), geometry(DataType::Float32)]).is_err(),
            "two point types with two coordinate widths"
        );
        assert!(widen(&[DataType::Float16, DataType::Float32]).is_err());
    }

    #[test]
    fn a_type_outside_the_numeric_set_is_refused() {
        for outsider in [DataType::Date32, DataType::Binary, DataType::Boolean] {
            assert_eq!(
                widen(&[outsider.clone(), outsider.clone()]).unwrap(),
                outsider
            );
            assert!(
                widen(&[outsider.clone(), DataType::Int32]).is_err(),
                "{outsider:?} and Int32"
            );
        }
    }

    fn every_type() -> Vec<DataType> {
        vec![
            DataType::Int8,
            DataType::UInt8,
            DataType::Int16,
            DataType::UInt16,
            DataType::Int32,
            DataType::UInt32,
            DataType::Int64,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::Utf8View,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
            DataType::Timestamp(TimeUnit::Millisecond, Some("Europe/Berlin".into())),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("America/Lima".into())),
            DataType::Boolean,
            DataType::Null,
        ]
    }

    #[test]
    fn widening_does_not_depend_on_the_order_of_its_operands() {
        for left in &every_type() {
            for right in &every_type() {
                assert_eq!(
                    super_type(left, right),
                    super_type(right, left),
                    "{left:?} and {right:?} widen two ways"
                );
            }
        }
    }

    #[test]
    fn widening_does_not_depend_on_the_grouping() {
        for left in &every_type() {
            for middle in &every_type() {
                for right in &every_type() {
                    let left_first =
                        super_type(left, middle).and_then(|pair| super_type(&pair, right));
                    let right_first =
                        super_type(middle, right).and_then(|pair| super_type(left, &pair));
                    assert_eq!(
                        left_first, right_first,
                        "({left:?} v {middle:?}) v {right:?} differs from \
                         {left:?} v ({middle:?} v {right:?})"
                    );
                }
            }
        }
    }

    #[test]
    fn widening_a_type_with_itself_changes_nothing() {
        for data_type in every_type() {
            assert_eq!(
                super_type(&data_type, &data_type),
                Some(data_type.clone()),
                "{data_type:?}"
            );
        }
    }

    #[test]
    fn every_join_is_a_cast_arrow_performs() {
        use arrow::compute::can_cast_types;

        for left in &every_type() {
            for right in &every_type() {
                if let Some(result) = super_type(left, right) {
                    for operand in [left, right] {
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

    #[test]
    fn two_timestamps_take_the_finer_unit() {
        let naive = |unit: TimeUnit| DataType::Timestamp(unit, None);
        assert_eq!(
            super_type(&naive(TimeUnit::Second), &naive(TimeUnit::Nanosecond)),
            Some(naive(TimeUnit::Nanosecond))
        );
        assert_eq!(
            super_type(&naive(TimeUnit::Microsecond), &naive(TimeUnit::Millisecond)),
            Some(naive(TimeUnit::Microsecond))
        );

        let utc = |unit: TimeUnit| DataType::Timestamp(unit, Some("UTC".into()));
        assert_eq!(
            super_type(&utc(TimeUnit::Second), &utc(TimeUnit::Millisecond)),
            Some(utc(TimeUnit::Millisecond))
        );
    }

    #[test]
    fn a_time_zone_coerces_to_utc() {
        let stamp = |zone: Option<&str>| {
            DataType::Timestamp(TimeUnit::Second, zone.map(|zone| zone.into()))
        };

        assert_eq!(
            super_type(&stamp(None), &stamp(Some("Europe/Berlin"))),
            Some(stamp(Some("Europe/Berlin")))
        );
        assert_eq!(
            super_type(&stamp(Some("Europe/Berlin")), &stamp(Some("America/Lima"))),
            Some(stamp(Some("UTC")))
        );
        assert_eq!(
            super_type(&stamp(Some("America/Lima")), &stamp(Some("Europe/Berlin"))),
            Some(stamp(Some("UTC")))
        );
        assert_eq!(super_type(&stamp(None), &stamp(None)), Some(stamp(None)));

        assert_eq!(
            super_type(
                &DataType::Timestamp(TimeUnit::Second, Some("Europe/Berlin".into())),
                &DataType::Timestamp(TimeUnit::Nanosecond, Some("America/Lima".into()))
            ),
            Some(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some("UTC".into())
            ))
        );

        assert_eq!(super_type(&stamp(None), &DataType::Int64), None);
    }

    #[test]
    fn a_family_widens_to_its_wider_member() {
        for (left, right, expected) in [
            (DataType::Utf8, DataType::Utf8View, DataType::Utf8View),
            (DataType::Utf8, DataType::LargeUtf8, DataType::LargeUtf8),
            (DataType::Utf8View, DataType::LargeUtf8, DataType::LargeUtf8),
            (DataType::Binary, DataType::BinaryView, DataType::BinaryView),
            (
                DataType::BinaryView,
                DataType::LargeBinary,
                DataType::LargeBinary,
            ),
            (DataType::Date32, DataType::Date64, DataType::Date64),
            (
                DataType::Time32(TimeUnit::Second),
                DataType::Time64(TimeUnit::Nanosecond),
                DataType::Time64(TimeUnit::Nanosecond),
            ),
            (
                DataType::Time32(TimeUnit::Millisecond),
                DataType::Time64(TimeUnit::Microsecond),
                DataType::Time64(TimeUnit::Microsecond),
            ),
        ] {
            assert_eq!(
                super_type(&left, &right),
                Some(expected.clone()),
                "{left:?} v {right:?}"
            );
        }

        for (left, right) in [
            (DataType::Utf8, DataType::Binary),
            (DataType::Date32, DataType::Int32),
            (DataType::LargeUtf8, DataType::Float64),
            (DataType::Date64, DataType::Time64(TimeUnit::Nanosecond)),
        ] {
            assert_eq!(super_type(&left, &right), None, "{left:?} and {right:?}");
        }
    }

    #[test]
    fn the_numeric_rules_match_the_table() {
        for (left, right, expected) in [
            (DataType::Int32, DataType::Int64, DataType::Int64),
            (DataType::UInt16, DataType::Int8, DataType::Int32),
            (DataType::UInt32, DataType::Int32, DataType::Int64),
            (DataType::Int64, DataType::UInt64, DataType::Float64),
            (DataType::Int32, DataType::Float64, DataType::Float64),
            (DataType::Int8, DataType::Float32, DataType::Float64),
            (DataType::Float32, DataType::Float64, DataType::Float64),
            (DataType::UInt8, DataType::Int8, DataType::Int16),
            (DataType::UInt8, DataType::Int16, DataType::Int16),
            (DataType::UInt8, DataType::Int32, DataType::Int32),
            (DataType::UInt8, DataType::UInt32, DataType::UInt32),
            (DataType::UInt16, DataType::Int32, DataType::Int32),
            (DataType::UInt16, DataType::Int64, DataType::Int64),
            (DataType::UInt32, DataType::Int8, DataType::Int64),
            (DataType::UInt64, DataType::Int8, DataType::Float64),
            (DataType::UInt64, DataType::UInt8, DataType::UInt64),
            (DataType::Int64, DataType::Float32, DataType::Float64),
        ] {
            assert_eq!(
                super_type(&left, &right),
                Some(expected.clone()),
                "{left:?} v {right:?}"
            );
        }
    }
}
