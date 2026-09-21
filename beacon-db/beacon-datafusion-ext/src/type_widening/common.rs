//! The field union both strategies share, and the rules both apply as they are.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{ArrowError, DataType, FieldRef, Schema, SchemaRef, TimeUnit};

use super::LabeledSchema;

/// Each distinct type the sources state for one column, in first seen order,
/// with the first source that states it.
pub(super) type StatedTypes = [(DataType, Option<Arc<str>>)];

/// The type of one column, and whether the conflict setting dropped a source
/// of another family.
pub(super) struct Resolved {
    pub data_type: DataType,
    pub conflict: bool,
}

/// One column, as gathered over every schema.
struct Column {
    /// The field of the first source that holds the column. The result keeps
    /// its name and its metadata.
    first: FieldRef,
    types: Vec<(DataType, Option<Arc<str>>)>,
    held_by: usize,
    nullable: bool,
}

/// Merge `schemas` into one, and let `resolve` pick the type of each column
/// from the types the sources state.
pub(super) fn merge_schemas_with(
    schemas: &[LabeledSchema],
    resolve: impl Fn(&str, &StatedTypes) -> Result<Resolved, ArrowError>,
) -> Result<SchemaRef, ArrowError> {
    if schemas.is_empty() {
        return Err(ArrowError::SchemaError(
            "No schemas provided for merging".to_string(),
        ));
    }

    let mut fields = Vec::new();
    for column in gather(schemas) {
        let resolved = resolve(column.first.name(), &column.types)?;
        let mut field = column.first.as_ref().clone();
        if &resolved.data_type != column.first.data_type() {
            field = field.with_data_type(resolved.data_type);
        }
        // A source that permits nulls, lacks the column, or holds a dropped
        // type reads null.
        if column.nullable || column.held_by < schemas.len() || resolved.conflict {
            field = field.with_nullable(true);
        }
        fields.push(Arc::new(field));
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// The columns of `schemas`, in first seen order.
fn gather(schemas: &[LabeledSchema]) -> Vec<Column> {
    let mut columns: Vec<Column> = Vec::new();
    let mut positions: HashMap<&str, usize> = HashMap::new();
    for labeled in schemas {
        for field in labeled.schema.fields() {
            let Some(&at) = positions.get(field.name().as_str()) else {
                positions.insert(field.name(), columns.len());
                columns.push(Column {
                    first: Arc::clone(field),
                    types: vec![(field.data_type().clone(), labeled.label.clone())],
                    held_by: 1,
                    nullable: field.is_nullable(),
                });
                continue;
            };
            let column = &mut columns[at];
            column.held_by += 1;
            column.nullable |= field.is_nullable();
            let stated = |(data_type, _): &(DataType, _)| data_type == field.data_type();
            if !column.types.iter().any(stated) {
                column
                    .types
                    .push((field.data_type().clone(), labeled.label.clone()));
            }
        }
    }
    columns
}

/// The error for a column that two sources type differently. A side without a
/// source reports its type alone.
pub(super) fn incompatible_types(
    field_name: &str,
    left: &DataType,
    left_source: Option<&str>,
    right: &DataType,
    right_source: Option<&str>,
) -> ArrowError {
    fn describe(data_type: &DataType, source: Option<&str>) -> String {
        match source {
            Some(source) => format!("{data_type:?} in '{source}'"),
            None => format!("{data_type:?}"),
        }
    }
    ArrowError::SchemaError(format!(
        "Incompatible types for field '{field_name}': {} vs {}",
        describe(left, left_source),
        describe(right, right_source)
    ))
}

/// The integer that holds both, or `Float64` where no integer does. Two signs
/// meet in the signed type of the next width. Two of one type give that type.
pub(super) fn integer_join(left: &DataType, right: &DataType) -> DataType {
    use DataType::*;
    match (left, right) {
        (Int8, Int16) | (Int16, Int8) => Int16,
        (Int8 | Int16, Int32) | (Int32, Int8 | Int16) => Int32,
        (Int8 | Int16 | Int32, Int64) | (Int64, Int8 | Int16 | Int32) => Int64,
        (UInt8, UInt16) | (UInt16, UInt8) => UInt16,
        (UInt8 | UInt16, UInt32) | (UInt32, UInt8 | UInt16) => UInt32,
        (UInt8 | UInt16 | UInt32, UInt64) | (UInt64, UInt8 | UInt16 | UInt32) => UInt64,
        (Int8 | Int16, UInt8) | (UInt8, Int8 | Int16) => Int16,
        (Int8 | Int16, UInt16) | (UInt16, Int8 | Int16) => Int32,
        (Int32, UInt8 | UInt16) | (UInt8 | UInt16, Int32) => Int32,
        (Int8 | Int16 | Int32, UInt32) | (UInt32, Int8 | Int16 | Int32) => Int64,
        (Int64, UInt8 | UInt16 | UInt32) | (UInt8 | UInt16 | UInt32, Int64) => Int64,
        (Int8 | Int16 | Int32 | Int64, UInt64) | (UInt64, Int8 | Int16 | Int32 | Int64) => Float64,
        (left, _) => left.clone(),
    }
}

/// The wider of two members of one family.
pub(super) fn wider<'a>(left: &'a DataType, right: &'a DataType) -> &'a DataType {
    if rank(right) > rank(left) {
        right
    } else {
        left
    }
}

/// The position of a type in its family, narrowest first. The string order
/// follows DataFusion.
fn rank(data_type: &DataType) -> u8 {
    use DataType::*;
    match data_type {
        Int8 | UInt8 | Float16 | Utf8 | Binary | Date32 => 0,
        Int16 | UInt16 | Float32 | Utf8View | BinaryView | Date64 => 1,
        Int32 | UInt32 | Float64 | LargeUtf8 | LargeBinary => 2,
        Int64 | UInt64 => 3,
        Time32(unit) | Time64(unit) | Timestamp(unit, _) | Duration(unit) => time_unit_rank(*unit),
        _ => 0,
    }
}

/// The finer of two time units.
pub(super) fn finer_unit(left: TimeUnit, right: TimeUnit) -> TimeUnit {
    if time_unit_rank(right) > time_unit_rank(left) {
        right
    } else {
        left
    }
}

fn time_unit_rank(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

/// The zone two timestamp columns read as: no zone, the one zone stated, or
/// [`UTC`] for two zones.
pub(super) fn zone_join(left: &Option<Arc<str>>, right: &Option<Arc<str>>) -> Option<Arc<str>> {
    match (left, right) {
        (None, None) => None,
        (Some(zone), None) | (None, Some(zone)) => Some(Arc::clone(zone)),
        (Some(left), Some(right)) if left == right => Some(Arc::clone(left)),
        (Some(_), Some(_)) => Some(UTC.into()),
    }
}

/// The zone two other zones read as.
const UTC: &str = "UTC";

#[cfg(test)]
mod tests {
    use arrow_schema::Field;

    use super::*;

    fn schema(fields: &[(&str, DataType)]) -> LabeledSchema {
        LabeledSchema::unlabeled(Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, dt)| Field::new(*name, dt.clone(), true))
                .collect::<Vec<_>>(),
        )))
    }

    fn required(name: &str, data_type: DataType) -> LabeledSchema {
        LabeledSchema::unlabeled(Arc::new(Schema::new(vec![Field::new(
            name, data_type, false,
        )])))
    }

    fn field_of<'a>(schema: &'a Schema, name: &str) -> &'a FieldRef {
        schema
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .unwrap_or_else(|| panic!("the merge holds '{name}'"))
    }

    fn first_type(_: &str, types: &StatedTypes) -> Result<Resolved, ArrowError> {
        Ok(Resolved {
            data_type: types[0].0.clone(),
            conflict: false,
        })
    }

    #[test]
    fn merging_no_schemas_is_an_error() {
        let err = merge_schemas_with(&[], first_type).unwrap_err();
        assert!(
            matches!(err, ArrowError::SchemaError(_)),
            "expected a schema error, got {err:?}"
        );
    }

    #[test]
    fn merge_unions_fields_in_first_seen_order() {
        let merged = merge_schemas_with(
            &[
                schema(&[("a", DataType::Int32), ("b", DataType::Utf8)]),
                schema(&[("b", DataType::Utf8), ("c", DataType::Float64)]),
            ],
            first_type,
        )
        .unwrap();

        let names: Vec<&str> = merged.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn the_resolver_sees_each_distinct_type_once_with_its_first_source() {
        let stated = |name: &str, types: &StatedTypes| {
            assert_eq!(name, "v");
            let seen: Vec<(DataType, Option<&str>)> = types
                .iter()
                .map(|(data_type, source)| (data_type.clone(), source.as_deref()))
                .collect();
            assert_eq!(
                seen,
                [
                    (DataType::Int32, Some("a.nc")),
                    (DataType::Int64, Some("b.nc")),
                    (DataType::Utf8, None),
                ]
            );
            first_type(name, types)
        };
        let named = |label: &str, data_type: DataType| {
            LabeledSchema::new(schema(&[("v", data_type)]).schema, label)
        };

        merge_schemas_with(
            &[
                named("a.nc", DataType::Int32),
                named("b.nc", DataType::Int64),
                named("c.nc", DataType::Int32),
                schema(&[("v", DataType::Utf8)]),
            ],
            stated,
        )
        .unwrap();
    }

    #[test]
    fn the_field_takes_the_resolved_type() {
        let wide = |_: &str, _: &StatedTypes| {
            Ok(Resolved {
                data_type: DataType::Float64,
                conflict: false,
            })
        };
        let merged = merge_schemas_with(&[schema(&[("v", DataType::Int32)])], wide).unwrap();
        assert_eq!(field_of(&merged, "v").data_type(), &DataType::Float64);
    }

    #[test]
    fn a_settled_column_turns_nullable() {
        let settled = |_: &str, types: &StatedTypes| {
            Ok(Resolved {
                data_type: types[0].0.clone(),
                conflict: true,
            })
        };
        let merged = merge_schemas_with(
            &[
                required("depth", DataType::Utf8),
                required("depth", DataType::Float64),
            ],
            settled,
        )
        .unwrap();
        assert!(
            field_of(&merged, "depth").is_nullable(),
            "a null needs a nullable column"
        );
    }

    #[test]
    fn a_field_keeps_its_metadata() {
        let geometry = |extension: &str| {
            LabeledSchema::unlabeled(Arc::new(Schema::new(vec![
                Field::new("geometry", DataType::Float64, false).with_metadata(
                    [("ARROW:extension:name".to_string(), extension.to_string())].into(),
                ),
            ])))
        };

        let merged = merge_schemas_with(
            &[geometry("geoarrow.point"), geometry("geoarrow.linestring")],
            first_type,
        )
        .unwrap();

        let field = field_of(&merged, "geometry");
        assert_eq!(
            field
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("geoarrow.point")
        );
        assert!(
            !field.is_nullable(),
            "every schema holds it, and requires it"
        );
    }

    #[test]
    fn a_column_some_files_lack_comes_out_nullable() {
        let merged = merge_schemas_with(
            &[
                required("TEMP", DataType::Float64),
                required("SALINITY", DataType::Float64),
            ],
            first_type,
        )
        .unwrap();
        assert!(field_of(&merged, "TEMP").is_nullable());
        assert!(field_of(&merged, "SALINITY").is_nullable());

        // Every file holds `TEMP` and requires it. One file permits nulls in `DEPTH`.
        let both_required = LabeledSchema::unlabeled(Arc::new(Schema::new(vec![
            Field::new("TEMP", DataType::Float64, false),
            Field::new("DEPTH", DataType::Int64, false),
        ])));
        let depth_optional = LabeledSchema::unlabeled(Arc::new(Schema::new(vec![
            Field::new("TEMP", DataType::Float64, false),
            Field::new("DEPTH", DataType::Int64, true),
        ])));

        for order in [
            vec![both_required.clone(), depth_optional.clone()],
            vec![depth_optional, both_required.clone()],
        ] {
            let merged = merge_schemas_with(&order, first_type).unwrap();
            assert!(!field_of(&merged, "TEMP").is_nullable());
            assert!(field_of(&merged, "DEPTH").is_nullable());
        }

        let alone = merge_schemas_with(&[both_required], first_type).unwrap();
        assert!(alone.fields().iter().all(|field| !field.is_nullable()));
    }

    #[test]
    fn two_integers_meet_in_the_type_that_holds_both() {
        for (left, right, expected) in [
            (DataType::Int8, DataType::Int64, DataType::Int64),
            (DataType::UInt16, DataType::UInt32, DataType::UInt32),
            (DataType::Int8, DataType::UInt8, DataType::Int16),
            (DataType::Int16, DataType::UInt8, DataType::Int16),
            (DataType::Int8, DataType::UInt16, DataType::Int32),
            (DataType::Int32, DataType::UInt16, DataType::Int32),
            (DataType::Int32, DataType::UInt32, DataType::Int64),
            (DataType::Int64, DataType::UInt8, DataType::Int64),
            (DataType::Int8, DataType::UInt64, DataType::Float64),
            (DataType::Int64, DataType::UInt64, DataType::Float64),
            (DataType::Int32, DataType::Int32, DataType::Int32),
        ] {
            assert_eq!(
                integer_join(&left, &right),
                expected,
                "{left:?} v {right:?}"
            );
            assert_eq!(
                integer_join(&right, &left),
                expected,
                "{right:?} v {left:?}"
            );
        }
    }

    #[test]
    fn the_wider_member_wins_in_either_order() {
        for (narrow, wide) in [
            (DataType::Int8, DataType::Int64),
            (DataType::UInt16, DataType::UInt32),
            (DataType::Float16, DataType::Float64),
            (DataType::Utf8, DataType::Utf8View),
            (DataType::Utf8View, DataType::LargeUtf8),
            (DataType::Binary, DataType::LargeBinary),
            (DataType::Date32, DataType::Date64),
            (
                DataType::Time32(TimeUnit::Millisecond),
                DataType::Time64(TimeUnit::Microsecond),
            ),
            (
                DataType::Duration(TimeUnit::Second),
                DataType::Duration(TimeUnit::Nanosecond),
            ),
        ] {
            assert_eq!(wider(&narrow, &wide), &wide, "{narrow:?} v {wide:?}");
            assert_eq!(wider(&wide, &narrow), &wide, "{wide:?} v {narrow:?}");
        }
        // Two equal ranks keep the left operand.
        assert_eq!(wider(&DataType::Utf8, &DataType::Utf8), &DataType::Utf8);
    }

    #[test]
    fn the_finer_unit_wins() {
        assert_eq!(
            finer_unit(TimeUnit::Second, TimeUnit::Nanosecond),
            TimeUnit::Nanosecond
        );
        assert_eq!(
            finer_unit(TimeUnit::Microsecond, TimeUnit::Millisecond),
            TimeUnit::Microsecond
        );
        assert_eq!(
            finer_unit(TimeUnit::Second, TimeUnit::Second),
            TimeUnit::Second
        );
    }

    #[test]
    fn a_time_zone_coerces_to_utc() {
        let zone = |name: &str| Some(Arc::<str>::from(name));
        assert_eq!(zone_join(&None, &None), None);
        assert_eq!(
            zone_join(&None, &zone("Europe/Berlin")),
            zone("Europe/Berlin")
        );
        assert_eq!(
            zone_join(&zone("Europe/Berlin"), &None),
            zone("Europe/Berlin")
        );
        assert_eq!(zone_join(&zone("+00:00"), &zone("+00:00")), zone("+00:00"));
        assert_eq!(
            zone_join(&zone("Europe/Berlin"), &zone("America/Lima")),
            zone("UTC")
        );
        // "+00:00" names UTC, and the pair reaches UTC through the same rule.
        assert_eq!(zone_join(&zone("+00:00"), &zone("UTC")), zone("UTC"));
    }
}
