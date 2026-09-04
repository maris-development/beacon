//! The single place that merges the schemas of a table.
//!
//! A table over files holds one schema per file. A query plans against one
//! schema. Each format merged its own schemas before this module, and five
//! formats read them in disk answer order. The merged schema changed between
//! runs. See issue #377.
//!
//! Each format now calls [`session_widening`] and merges with
//! [`ArrowTypeWidening::merge_schemas`]. The table does the same.
//!
//! # The rules
//!
//! [`DefaultArrowTypeWidening`] applies them. Two files, one column name:
//!
//! | The files state | The table reports |
//! | --- | --- |
//! | the same type | that type |
//! | two types of one family | the member that holds both, from the tables below |
//! | two families (a number and a string) | an error that names the column, both types and both files, or the first type: see the setting below |
//! | `Null` and any type | that type, because a null column holds no value |
//! | one nullable field, one not | a nullable field |
//! | the column in one file only | a nullable field, because the scan fills the other file with nulls |
//! | the same type, two metadata sets | the metadata of the first file |
//!
//! A family holds one kind of value in more than one width, layout or precision.
//! A wider member reads every value of a narrower one, so the join is the wider
//! member. Five families widen:
//!
//! | Family | The order, narrowest first |
//! | --- | --- |
//! | number | see the table below |
//! | timestamp | `Second`, `Millisecond`, `Microsecond`, `Nanosecond` |
//! | string | `Utf8`, `Utf8View`, `LargeUtf8` |
//! | binary | `Binary`, `BinaryView`, `LargeBinary` |
//! | date | `Date32`, `Date64` |
//! | time | `Time32(Second)`, `Time32(Millisecond)`, `Time64(Microsecond)`, `Time64(Nanosecond)` |
//!
//! **A time zone follows DataFusion.** One file with a zone gives that zone, and
//! the tz-naive column reads as it. Two files with two zones give `UTC`, which is
//! the zone that both read as. DataFusion keeps the zone of the left operand
//! there, and a merge has no left operand, because the file order is the disk
//! answer order. A zone changes what a value means, so both lines are a coercion.
//! One caveat holds for the finest unit: `Nanosecond` covers the years 1677 to
//! 2262, and a `Second` value outside that range overflows in the cast.
//!
//! Every other pair gives an error. `Boolean` widens with no number.
//! `Timestamp` widens with no integer. A decimal, a duration, an interval, a
//! dictionary, a list and a struct widen with themselves alone.
//!
//! ## The numeric family
//!
//! [`NUMERIC_COVERS`] states the order. The join of two members is the least type
//! at or above both:
//!
//! | Left | Right | Result | Reason |
//! | --- | --- | --- | --- |
//! | `Int32` | `Int64` | `Int64` | the wider signed type holds both |
//! | `UInt16` | `Int8` | `Int32` | `UInt16` holds 65535, which needs 32 signed bits |
//! | `UInt32` | `Int32` | `Int64` | the same rule, one width up |
//! | `Int64` | `UInt64` | `Float64` | no integer holds both, as in Polars and Numpy |
//! | `Int32` | `Float64` | `Float64` | 53 mantissa bits hold every `Int32` |
//! | `Int8` | `Float32` | `Float64` | see below |
//! | `Float32` | `Float64` | `Float64` | the wider float holds both |
//!
//! **An integer beside a `Float32` widens to `Float64`.** Polars keeps `Float32`
//! for a narrow integer. A `Float32` holds no `Int32`, so the rule must drop
//! `Int32`. A lattice cannot then keep `Int8`, because `UInt16` + `Int8` is
//! `Int32`. The two answers would make the merge depend on the order, which is
//! the fault of issue #377. `Float64` loses no value.
//!
//! A geometry column is not numeric, so the rules above give it no widening.
//! GeoArrow states a geometry as a struct or a list, and it marks the column with
//! field metadata. Two files with the same geometry type therefore merge, and the
//! column keeps its GeoArrow keys. Two files with two geometry types give an
//! error. GeoParquet drops the
//! keys of a column that the files mark differently, so the column goes plain and
//! the spatial functions refuse it: see `reconcile_field_metadata` in
//! `beacon_arrow_geoparquet`. Two coordinate reference systems are an open point.
//! The GeoParquet reader states the extension name and drops the system, so the
//! merge sees two equal fields and joins them.
//!
//! # The setting for a column that no type holds
//!
//! [`TypeConflict`] settles a column that two sources type in two families. The
//! default is [`Fail`], which is the error above. A deployment that reads a
//! collection of many years takes [`KeepFirst`] instead:
//!
//! ```text
//! BEACON_TYPE_WIDENING_ON_CONFLICT=keep_first
//! ```
//!
//! The table then reports the type of the first source, and the scan casts
//! every other source to it. A value the type cannot hold reads as null, and so
//! does a column no cast reaches. The merge marks such a column with
//! [`TYPE_CONFLICT_KEY`], and `scan_adapt` reads that mark. The merged schema
//! carries the decision, so no scan needs the setting itself.
//!
//! Two costs follow. **The merge reads the order**, so it drops no repeat and
//! starts no thread: a collection of 100000 files takes one fold. **The first
//! type is the first in listing order**, which is the disk answer order of issue
//! #377, so a store that lists in two orders reports two types. A column that
//! widens keeps the rules above either way.
//!
//! A deployment replaces the rules through
//! [`RuntimeBuilder::with_type_widening`]. Every merge in the process takes the
//! new strategy.
//!
//! [`Fail`]: TypeConflict::Fail
//! [`KeepFirst`]: TypeConflict::KeepFirst
//!
//! # The source of a conflict
//!
//! A merge that refuses a column names the source of each type. A table over
//! 10000 files is otherwise a search:
//!
//! ```text
//! Incompatible types for field 'depth': Utf8 in 'argo/a.nc' vs Float64 in 'argo/b.nc'
//! ```
//!
//! Each caller passes one [`LabeledSchema`] per source. [`label_by_object`] builds
//! them from a listing. A caller that holds no name passes
//! [`LabeledSchema::unlabeled`], and the error reports the two types alone.
//!
//! A widened column takes the name of the source that states its current type. A
//! join that equals neither operand leaves that side without a name: `Int64`
//! beside `UInt64` gives `Float64`, and no file states `Float64`.
//!
//! # How the merge does less work
//!
//! The rules above are a semilattice join. The numeric order is a lattice, and a
//! lattice join is the source of the three properties. The join is idempotent, commutative
//! and associative, so the schema order, the group boundaries and the repeat
//! count change no result and no failure. A strategy states that property through
//! [`is_order_independent`].
//!
//! The merge then drops work. It drops each repeated schema, because a
//! collection of 100000 files holds few distinct schemas. It gives each
//! contiguous chunk of the rest to a thread. The chunks stay in listing order,
//! because column order follows the listing and `SELECT *` shows it.
//!
//! A strategy that answers `false` gets one fold over every schema, repeats
//! included.
//!
//! Both steps hide the source of a field. A repeat loses its own name, and a
//! chunk result names no source at all. A merge that fails therefore folds the
//! distinct schemas once more, in one thread, to name both sources. The fold
//! stops at the same pair, because the join is associative. A merge that succeeds
//! pays nothing for it.
//!
//! [`is_order_independent`]: ArrowTypeWideningStrategy::is_order_independent
//! [`RuntimeBuilder::with_type_widening`]: ../../beacon_core/runtime_builder/struct.RuntimeBuilder.html#method.with_type_widening

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, LazyLock};

use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use object_store::ObjectMeta;

/// Below this count, one fold costs less than a split. A merge walks field
/// names, so a thread must earn its start cost.
const SEQUENTIAL_MERGE_LIMIT: usize = 64;

/// The least work for one thread.
const MIN_SCHEMAS_PER_THREAD: usize = 32;

/// A schema and the name of the source it was read from.
///
/// A merge that refuses a column reports the name of both sources. The name is
/// the object path of a file, the URL of a table, or the group path of a Zarr
/// leaf. See the [module docs](self).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabeledSchema {
    /// The schema of one source.
    pub schema: SchemaRef,
    /// What an error calls that source. `None` for a caller that holds no name,
    /// and the error then reports the two types alone.
    pub label: Option<Arc<str>>,
}

impl LabeledSchema {
    /// A schema with the name of its source.
    pub fn new(schema: SchemaRef, label: impl Into<Arc<str>>) -> Self {
        Self {
            schema,
            label: Some(label.into()),
        }
    }

    /// A schema without a name.
    pub fn unlabeled(schema: SchemaRef) -> Self {
        Self {
            schema,
            label: None,
        }
    }
}

impl From<SchemaRef> for LabeledSchema {
    fn from(schema: SchemaRef) -> Self {
        Self::unlabeled(schema)
    }
}

/// Name each schema after the object it was read from.
///
/// A format reads one schema per object, in listing order, so the two lists line
/// up. A schema past the end of `objects` gets no name.
pub fn label_by_object(objects: &[ObjectMeta], schemas: &[SchemaRef]) -> Vec<LabeledSchema> {
    schemas
        .iter()
        .enumerate()
        .map(|(index, schema)| match objects.get(index) {
            Some(object) => LabeledSchema::new(Arc::clone(schema), object.location.as_ref()),
            None => LabeledSchema::unlabeled(Arc::clone(schema)),
        })
        .collect()
}

pub struct ArrowTypeWidening {
    pub strategy: Arc<dyn ArrowTypeWideningStrategy>,
}

impl ArrowTypeWidening {
    pub fn new(strategy: Arc<dyn ArrowTypeWideningStrategy>) -> Self {
        Self { strategy }
    }

    /// The strategy for a session that registers none.
    ///
    /// `RuntimeBuilder` registers the extension for a server. [`session_widening`]
    /// falls back to this value, so a test or an embedded use gets the same rule
    /// and no error.
    pub fn default_extension() -> Arc<Self> {
        Arc::new(Self::new(Arc::new(DefaultArrowTypeWidening::new())))
    }

    /// Merge `schemas` into the schema a query plans against.
    ///
    /// The strategy decides the result. This method decides the cost: an
    /// order-independent strategy loses its repeats, and threads merge the rest.
    /// Each schema names its source, and a failed merge reports both names. See
    /// the [module docs](self).
    pub fn merge_schemas(&self, schemas: &[LabeledSchema]) -> Result<SchemaRef, ArrowError> {
        let strategy = self.strategy.as_ref();
        if !strategy.is_order_independent() {
            return strategy.merge_schemas(schemas);
        }
        let distinct = distinct_schemas(schemas);
        match merge_distinct(strategy, &distinct) {
            Ok(schema) => Ok(schema),
            // The dropped repeats and the chunk results hide the source of a
            // field. Fold the named schemas once to name both sources. The fold
            // stops at the same pair, because the join is associative.
            Err(error) => Err(strategy.merge_schemas(&distinct).err().unwrap_or(error)),
        }
    }
}

/// The merge rule of the session, or the default rule when the session has none.
///
/// This function is the entry point. A format calls it in `infer_schema`.
/// `FastObjectTable` calls it for the URLs behind one table. Both then get the
/// rule that a deployment registered through
/// `RuntimeBuilder::with_type_widening`. A session without the extension gets
/// [`ArrowTypeWidening::default_extension`], which is the same rule that
/// `RuntimeBuilder` registers.
pub fn session_widening(session: &dyn Session) -> Arc<ArrowTypeWidening> {
    session
        .config()
        .get_extension::<ArrowTypeWidening>()
        .unwrap_or_else(ArrowTypeWidening::default_extension)
}

pub trait ArrowTypeWideningStrategy: Send + Sync {
    /// Merge these schemas into one, in the order given.
    ///
    /// Each schema names its source. Report both names for a column that two
    /// sources describe with two types. See [`LabeledSchema`].
    fn merge_schemas(&self, schemas: &[LabeledSchema]) -> Result<SchemaRef, ArrowError>;

    /// State whether this merge is a semilattice join: **idempotent, commutative
    /// and associative**. The schema order, the group boundaries and the repeat
    /// count then change no result and no failure.
    ///
    /// `true` is the default, and it lets [`ArrowTypeWidening::merge_schemas`]
    /// drop a repeated schema and thread each chunk.
    /// [`DefaultArrowTypeWidening`] qualifies.
    ///
    /// Answer `false` for a rule that reads the order. One example keeps the first
    /// type of a column. Such a merge gets one fold over every schema.
    fn is_order_independent(&self) -> bool {
        true
    }
}

/// Merge the schemas of a table. Union the fields, and widen a numeric column.
///
/// The [module docs](self) hold the rule table. The fields keep first seen order,
/// which is the order `SELECT *` shows.
///
/// A field is nullable unless every schema holds it and every schema requires it.
/// The scan fills a missing column with nulls, and a non-nullable column holds no
/// nulls. Such a table fails with "Non-nullable column X is missing from the
/// physical schema".
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DefaultArrowTypeWidening {
    /// What the merge does with a column that two sources type in two families.
    pub on_conflict: TypeConflict,
}

impl DefaultArrowTypeWidening {
    /// The rule that refuses a column two sources type in two families.
    pub const fn new() -> Self {
        Self {
            on_conflict: TypeConflict::Fail,
        }
    }

    /// The rule that keeps the first type of such a column.
    ///
    /// See [`TypeConflict::KeepFirst`] for what the table then reports and what
    /// the scan then reads.
    pub const fn keeping_first_type() -> Self {
        Self {
            on_conflict: TypeConflict::KeepFirst,
        }
    }
}

/// What the merge does with a column that no type holds.
///
/// A column that two sources type in one family widens either way. This setting
/// covers the rest: a number beside a string, a list beside a scalar. See the
/// [module docs](self).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TypeConflict {
    /// Refuse the merge. The error names the column, both types and both
    /// sources. The table then answers no query at all.
    #[default]
    Fail,
    /// Keep the type the merge met first, and mark the column with
    /// [`TYPE_CONFLICT_KEY`].
    ///
    /// The table reports that type, and the scan casts every other source to
    /// it. A value the type cannot hold reads as null, and so does a column no
    /// cast reaches. The column is nullable for that reason.
    ///
    /// **The first type is the first in listing order.** The merge therefore
    /// reads the order, and [`is_order_independent`] answers `false` for it:
    /// the merge drops no repeat and starts no thread. A store that lists in
    /// two orders also reports two types. See the [module docs](self).
    ///
    /// [`is_order_independent`]: ArrowTypeWideningStrategy::is_order_independent
    KeepFirst,
}

impl TypeConflict {
    /// The setting one option value names.
    ///
    /// `BEACON_TYPE_WIDENING_ON_CONFLICT` holds the value.
    ///
    /// # Errors
    ///
    /// Returns the offending value when it names no setting.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "fail" | "error" | "" => Ok(Self::Fail),
            "keep_first" | "keep-first" | "first" => Ok(Self::KeepFirst),
            other => Err(other.to_string()),
        }
    }
}

/// The field metadata key that marks a column [`TypeConflict::KeepFirst`]
/// settled.
///
/// The scan reads it. A cast of such a column may not fail, because the sources
/// disagree on what the column holds. `beacon_datafusion_ext::scan_adapt` casts
/// it to null instead. The merged schema carries the decision, so no scan needs
/// the setting itself.
pub const TYPE_CONFLICT_KEY: &str = "beacon.type_conflict";

/// The value of [`TYPE_CONFLICT_KEY`]. The column holds the type of the first
/// source, and every other source casts to it.
pub const TYPE_CONFLICT_FIRST_TYPE: &str = "first_type";

/// Whether [`TypeConflict::KeepFirst`] settled this column.
///
/// A cast onto such a column reads a value it cannot hold as null. Every other
/// cast reports that value as an error.
pub fn is_type_conflict(field: &Field) -> bool {
    field
        .metadata()
        .get(TYPE_CONFLICT_KEY)
        .is_some_and(|value| value == TYPE_CONFLICT_FIRST_TYPE)
}

/// `field`, marked as a column [`TypeConflict::KeepFirst`] settled.
///
/// The field turns nullable. The cast of a source the type cannot hold reads
/// null, and a non-nullable column holds no null.
fn mark_type_conflict(field: Field) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(
        TYPE_CONFLICT_KEY.to_string(),
        TYPE_CONFLICT_FIRST_TYPE.to_string(),
    );
    field.with_nullable(true).with_metadata(metadata)
}

impl ArrowTypeWideningStrategy for DefaultArrowTypeWidening {
    fn is_order_independent(&self) -> bool {
        // `KeepFirst` reads the order. `Int64` then `Utf8` then `Float64` gives
        // `Float64`, and the same three in one other grouping give `Int64`.
        matches!(self.on_conflict, TypeConflict::Fail)
    }

    fn merge_schemas(&self, schemas: &[LabeledSchema]) -> Result<SchemaRef, ArrowError> {
        if schemas.is_empty() {
            return Err(ArrowError::SchemaError(
                "No schemas provided for merging".to_string(),
            ));
        }

        let mut merged_fields: Vec<FieldRef> = Vec::new();
        // The source that gave each merged field its type, at the same index.
        let mut sources: Vec<Option<Arc<str>>> = Vec::new();
        // The position of each field in `merged_fields`, and its schema count.
        let mut field_map: HashMap<String, (usize, usize)> = HashMap::new();

        for labeled in schemas {
            for field in labeled.schema.fields() {
                let field_name = field.name().clone();
                if let Some((at, held_by)) = field_map.get_mut(&field_name) {
                    *held_by += 1;
                    let at = *at;
                    // An `Arc` clone. It ends the borrow of `merged_fields`, so
                    // the write below needs no second lookup.
                    let existing_field = Arc::clone(&merged_fields[at]);
                    // Two types for one column. A numeric pair widens. The
                    // setting settles every other pair.
                    let mut conflict = false;
                    let widened = if existing_field.data_type() == field.data_type() {
                        None
                    } else {
                        match super_type(existing_field.data_type(), field.data_type()) {
                            Some(data_type) => Some(data_type),
                            None => match self.on_conflict {
                                TypeConflict::Fail => {
                                    return Err(ArrowError::SchemaError(incompatible_types(
                                        &field_name,
                                        existing_field.data_type(),
                                        sources[at].as_deref(),
                                        field.data_type(),
                                        labeled.label.as_deref(),
                                    )));
                                }
                                // Keep the type the merge met first, and leave
                                // the source that states it. The mark tells the
                                // scan to read this source as null.
                                TypeConflict::KeepFirst => {
                                    conflict = true;
                                    None
                                }
                            },
                        }
                    };
                    // One file that permits nulls makes the column nullable.
                    let nullable = field.is_nullable() && !existing_field.is_nullable();
                    if let Some(data_type) = &widened {
                        sources[at] =
                            source_of(data_type, &existing_field, &sources[at], field, labeled);
                    }
                    if widened.is_some() || nullable || conflict {
                        let mut merged = existing_field.as_ref().clone();
                        if let Some(data_type) = widened {
                            merged = merged.with_data_type(data_type);
                        }
                        if nullable {
                            merged = merged.with_nullable(true);
                        }
                        if conflict {
                            merged = mark_type_conflict(merged);
                        }
                        merged_fields[at] = Arc::new(merged);
                    }
                } else {
                    // If the field does not exist, add it to the map and merged fields
                    field_map.insert(field_name.clone(), (merged_fields.len(), 1));
                    merged_fields.push(field.clone());
                    sources.push(labeled.label.clone());
                }
            }
        }

        // The scan fills a missing column with nulls. A column that some schemas
        // lack must therefore be nullable.
        for field in merged_fields.iter_mut() {
            let (_, held_by) = field_map[field.name()];
            if held_by < schemas.len() && !field.is_nullable() {
                *field = Arc::new(field.as_ref().clone().with_nullable(true));
            }
        }

        Ok(Arc::new(arrow_schema::Schema::new(merged_fields)))
    }
}

/// The text for a column that two sources describe with two types.
///
/// Each type takes the name of the source that states it. A side without a name
/// reports its type alone. See the [module docs](self).
fn incompatible_types(
    field_name: &str,
    left: &DataType,
    left_source: Option<&str>,
    right: &DataType,
    right_source: Option<&str>,
) -> String {
    fn describe(data_type: &DataType, source: Option<&str>) -> String {
        match source {
            Some(source) => format!("{data_type:?} in '{source}'"),
            None => format!("{data_type:?}"),
        }
    }
    format!(
        "Incompatible types for field '{field_name}': {} vs {}",
        describe(left, left_source),
        describe(right, right_source)
    )
}

/// The source that states `widened`, which is the type the column now holds.
///
/// One operand equals the result, so that source states it. A join that equals
/// neither operand has no such source: `Int64` beside `UInt64` gives `Float64`,
/// and no file states `Float64`.
fn source_of(
    widened: &DataType,
    existing_field: &FieldRef,
    existing_source: &Option<Arc<str>>,
    field: &FieldRef,
    labeled: &LabeledSchema,
) -> Option<Arc<str>> {
    if widened == existing_field.data_type() {
        existing_source.clone()
    } else if widened == field.data_type() {
        labeled.label.clone()
    } else {
        None
    }
}

/// The type that holds both `left` and `right`, or `None` when no type holds
/// both.
///
/// `Null` holds no value, so it widens with every type. Two types of one family
/// widen inside that family. Two families never widen, so a number beside a
/// string gives `None`. The merge reports `None` as an error. See the
/// [module docs](self).
fn super_type(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }
    if left == &DataType::Null {
        return Some(right.clone());
    }
    if right == &DataType::Null {
        return Some(left.clone());
    }

    if let (Some(left), Some(right)) = (Numeric::of(left), Numeric::of(right)) {
        return Some(join(left, right).data_type());
    }
    if let (
        DataType::Timestamp(left_unit, left_zone),
        DataType::Timestamp(right_unit, right_zone),
    ) = (left, right)
    {
        return timestamp_super_type(left_unit, left_zone, right_unit, right_zone);
    }
    if let (Some((left_rank, left_family)), Some((right_rank, right_family))) =
        (chain_rank(left), chain_rank(right))
        && left_family == right_family
    {
        // Two members of one chain. The higher rank holds the lower one.
        return Some(chain_member(left_family, left_rank.max(right_rank)));
    }
    None
}

/// The timestamp that holds both operands.
///
/// The result takes the finer unit, which loses no value. The time zone follows
/// DataFusion:
///
/// - Two files without a zone give a column without a zone.
/// - One file with a zone gives that zone. The other column reads as that zone.
/// - Two files with the same zone keep it.
/// - Two files with two zones give [`UTC`]. DataFusion keeps the zone of the left
///   operand here. A merge has no left operand, because the file order is the
///   disk answer order (issue #377), so this rule takes the zone that both files
///   read as.
///
/// A zone changes what a value means, so the last two lines are a coercion, not a
/// cast of the values. A tz-naive column that holds local time reads as UTC.
///
/// One caveat holds for the unit: `Nanosecond` covers the years 1677 to 2262. A
/// `Second` value outside that range overflows in the cast, not here.
fn timestamp_super_type(
    left_unit: &TimeUnit,
    left_zone: &Option<Arc<str>>,
    right_unit: &TimeUnit,
    right_zone: &Option<Arc<str>>,
) -> Option<DataType> {
    let zone = match (left_zone, right_zone) {
        (None, None) => None,
        (Some(zone), None) | (None, Some(zone)) => Some(Arc::clone(zone)),
        (Some(left), Some(right)) if left == right => Some(Arc::clone(left)),
        (Some(_), Some(_)) => Some(UTC.into()),
    };
    let finer = if time_unit_rank(left_unit) >= time_unit_rank(right_unit) {
        left_unit
    } else {
        right_unit
    };
    Some(DataType::Timestamp(finer.clone(), zone))
}

/// The zone that two other zones read as. `"+00:00"` names the same zone, and it
/// reaches this value through the rule for two zones.
const UTC: &str = "UTC";

/// The precision of a time unit. A higher rank holds every value of a lower one.
fn time_unit_rank(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

/// The families that widen along one chain, from the narrowest member up.
///
/// Each family holds one kind of value in more than one layout or precision. The
/// wider member holds every value of the narrower one, so the join is the higher
/// rank. The string ranks follow DataFusion, which coerces `Utf8` and `Utf8View`
/// to `Utf8View`, and `Utf8View` and `LargeUtf8` to `LargeUtf8`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Chain {
    String,
    Binary,
    Date,
    Time,
}

/// The family and the rank of `data_type`, or `None` for a type outside every
/// chain.
///
/// A rank compares against another rank of the same family alone. Two families
/// never widen, so the caller checks the family first.
fn chain_rank(data_type: &DataType) -> Option<(u8, Chain)> {
    Some(match data_type {
        DataType::Utf8 => (0, Chain::String),
        DataType::Utf8View => (1, Chain::String),
        DataType::LargeUtf8 => (2, Chain::String),
        DataType::Binary => (0, Chain::Binary),
        DataType::BinaryView => (1, Chain::Binary),
        DataType::LargeBinary => (2, Chain::Binary),
        DataType::Date32 => (0, Chain::Date),
        DataType::Date64 => (1, Chain::Date),
        DataType::Time32(TimeUnit::Second) => (0, Chain::Time),
        DataType::Time32(TimeUnit::Millisecond) => (1, Chain::Time),
        DataType::Time64(TimeUnit::Microsecond) => (2, Chain::Time),
        DataType::Time64(TimeUnit::Nanosecond) => (3, Chain::Time),
        _ => return None,
    })
}

/// The member of `family` at `rank`. The inverse of [`chain_rank`].
fn chain_member(family: Chain, rank: u8) -> DataType {
    match (family, rank) {
        (Chain::String, 0) => DataType::Utf8,
        (Chain::String, 1) => DataType::Utf8View,
        (Chain::String, _) => DataType::LargeUtf8,
        (Chain::Binary, 0) => DataType::Binary,
        (Chain::Binary, 1) => DataType::BinaryView,
        (Chain::Binary, _) => DataType::LargeBinary,
        (Chain::Date, 0) => DataType::Date32,
        (Chain::Date, _) => DataType::Date64,
        (Chain::Time, 0) => DataType::Time32(TimeUnit::Second),
        (Chain::Time, 1) => DataType::Time32(TimeUnit::Millisecond),
        (Chain::Time, 2) => DataType::Time64(TimeUnit::Microsecond),
        (Chain::Time, _) => DataType::Time64(TimeUnit::Nanosecond),
    }
}

/// The numeric widening lattice, as its members.
///
/// **The order of this list is a linear extension of the widening order.** Every
/// member sits at a higher index than each member below it. [`join`] reads the
/// lowest index of two upper sets, and that index is the least upper bound only
/// while this holds. A test checks the property.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Numeric {
    Int8 = 0,
    UInt8 = 1,
    Int16 = 2,
    UInt16 = 3,
    Int32 = 4,
    UInt32 = 5,
    Int64 = 6,
    UInt64 = 7,
    Float32 = 8,
    Float64 = 9,
}

/// Every member, by index. [`Numeric::at`] reads it back.
const NUMERICS: [Numeric; 10] = [
    Numeric::Int8,
    Numeric::UInt8,
    Numeric::Int16,
    Numeric::UInt16,
    Numeric::Int32,
    Numeric::UInt32,
    Numeric::Int64,
    Numeric::UInt64,
    Numeric::Float32,
    Numeric::Float64,
];

/// The widening order, as its direct edges. `(lower, upper)` reads "a column of
/// `lower` reads as a column of `upper`".
///
/// This list is the whole rule. [`join`] derives every pair from it.
///
/// - An integer widens along its own sign, and into the next signed width that
///   holds it. `UInt16` holds 65535, so it needs an `Int32`.
/// - `Int64` and `UInt64` reach `Float64` and nothing else. The cast loses
///   precision above 2^53. Polars and Numpy make the same trade.
/// - `Float32` sits under `Float64` and over no integer. A 24-bit mantissa holds
///   no `Int32`. A lattice cannot hold `Int8` and drop `Int32`, because
///   `UInt16` + `Int8` is `Int32`. An integer beside a `Float32` therefore
///   widens to `Float64`, where Polars keeps `Float32` for a narrow integer.
const NUMERIC_COVERS: &[(Numeric, Numeric)] = &[
    (Numeric::Int8, Numeric::Int16),
    (Numeric::UInt8, Numeric::UInt16),
    (Numeric::UInt8, Numeric::Int16),
    (Numeric::Int16, Numeric::Int32),
    (Numeric::UInt16, Numeric::UInt32),
    (Numeric::UInt16, Numeric::Int32),
    (Numeric::Int32, Numeric::Int64),
    (Numeric::UInt32, Numeric::UInt64),
    (Numeric::UInt32, Numeric::Int64),
    (Numeric::Int64, Numeric::Float64),
    (Numeric::UInt64, Numeric::Float64),
    (Numeric::Float32, Numeric::Float64),
];

/// The upper set of each member, as a bit mask. Bit `i` marks `NUMERICS[i]` at or
/// above that member.
///
/// The transitive closure of [`NUMERIC_COVERS`], computed once. One descending
/// pass closes it, because every edge runs from a lower index to a higher one.
static NUMERIC_UPPER_SETS: LazyLock<[u16; NUMERICS.len()]> = LazyLock::new(|| {
    let mut upper = [0u16; NUMERICS.len()];
    for index in (0..NUMERICS.len()).rev() {
        upper[index] |= 1 << index;
        for (lower, above) in NUMERIC_COVERS {
            if *lower as usize == index {
                upper[index] |= upper[*above as usize];
            }
        }
    }
    upper
});

/// The least member at or above both operands.
///
/// One bitwise AND gives the common upper bounds. The lowest index of them is the
/// least, because the list is a linear extension of the order. The intersection
/// is never empty: `Float64` sits above every member.
fn join(left: Numeric, right: Numeric) -> Numeric {
    let common = NUMERIC_UPPER_SETS[left as usize] & NUMERIC_UPPER_SETS[right as usize];
    Numeric::at(common.trailing_zeros() as usize)
}

impl Numeric {
    fn at(index: usize) -> Self {
        NUMERICS[index]
    }

    /// The member of `data_type`, or `None` for a type outside the lattice.
    fn of(data_type: &DataType) -> Option<Self> {
        Some(match data_type {
            DataType::Int8 => Numeric::Int8,
            DataType::UInt8 => Numeric::UInt8,
            DataType::Int16 => Numeric::Int16,
            DataType::UInt16 => Numeric::UInt16,
            DataType::Int32 => Numeric::Int32,
            DataType::UInt32 => Numeric::UInt32,
            DataType::Int64 => Numeric::Int64,
            DataType::UInt64 => Numeric::UInt64,
            DataType::Float32 => Numeric::Float32,
            DataType::Float64 => Numeric::Float64,
            _ => return None,
        })
    }

    fn data_type(self) -> DataType {
        match self {
            Numeric::Int8 => DataType::Int8,
            Numeric::UInt8 => DataType::UInt8,
            Numeric::Int16 => DataType::Int16,
            Numeric::UInt16 => DataType::UInt16,
            Numeric::Int32 => DataType::Int32,
            Numeric::UInt32 => DataType::UInt32,
            Numeric::Int64 => DataType::Int64,
            Numeric::UInt64 => DataType::UInt64,
            Numeric::Float32 => DataType::Float32,
            Numeric::Float64 => DataType::Float64,
        }
    }
}

/// The schemas without a repeat, in first seen order.
///
/// An idempotent join gives the same answer without the repeats, and a collection
/// holds far fewer distinct schemas than files. The check uses a fingerprint. A
/// full compare runs only for two equal fingerprints.
fn distinct_schemas(schemas: &[LabeledSchema]) -> Cow<'_, [LabeledSchema]> {
    if schemas.len() < 2 {
        return Cow::Borrowed(schemas);
    }

    let mut kept: Vec<LabeledSchema> = Vec::new();
    let mut seen: HashMap<u64, Vec<usize>> = HashMap::new();
    for labeled in schemas {
        let candidates = seen.entry(fingerprint(&labeled.schema)).or_default();
        // The first of a repeat stays, with its own name. A later copy names the
        // same types, so the name it drops adds nothing to an error.
        if candidates.iter().any(|index| {
            Arc::ptr_eq(&kept[*index].schema, &labeled.schema)
                || kept[*index].schema == labeled.schema
        }) {
            continue;
        }
        candidates.push(kept.len());
        kept.push(labeled.clone());
    }

    if kept.len() == schemas.len() {
        // Nothing repeated. Hand back what we were given rather than the copy.
        return Cow::Borrowed(schemas);
    }
    Cow::Owned(kept)
}

/// A hash over every part that [`Schema`] equality reads.
///
/// One process makes and reads these values, so the algorithm needs no stability
/// across releases. The schema cache stores its keys, so
/// [`SchemaOptions`](crate::format_ext::SchemaOptions) avoids the `std` hasher.
fn fingerprint(schema: &Schema) -> u64 {
    let mut hasher = DefaultHasher::new();
    for field in schema.fields() {
        field.name().hash(&mut hasher);
        field.data_type().hash(&mut hasher);
        field.is_nullable().hash(&mut hasher);
    }
    schema.metadata().len().hash(&mut hasher);
    hasher.finish()
}

/// Merge with threads. Each thread takes one contiguous chunk, and the chunk
/// results then merge in the same way.
///
/// The chunks combine in order, so the columns keep the order of the listing.
fn merge_distinct(
    strategy: &dyn ArrowTypeWideningStrategy,
    schemas: &[LabeledSchema],
) -> Result<SchemaRef, ArrowError> {
    let threads = std::thread::available_parallelism()
        .map(|threads| threads.get())
        .unwrap_or(1);
    if schemas.len() <= SEQUENTIAL_MERGE_LIMIT || threads < 2 {
        return strategy.merge_schemas(schemas);
    }

    let per_thread = schemas.len().div_ceil(threads).max(MIN_SCHEMAS_PER_THREAD);
    let results: Vec<Result<SchemaRef, ArrowError>> = std::thread::scope(|scope| {
        schemas
            .chunks(per_thread)
            .map(|chunk| scope.spawn(move || strategy.merge_schemas(chunk)))
            .collect::<Vec<_>>()
            .into_iter()
            // A merge is pure. A panic is therefore a defect in this process, not
            // a bad schema. Send the panic to the thread of the caller. Do not
            // report it as a schema error.
            .map(|handle| {
                handle
                    .join()
                    .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
            })
            .collect()
    });

    // Read the results in chunk order. A table with a conflict then reports the
    // first conflict, as one fold does.
    let mut partial = Vec::with_capacity(results.len());
    for result in results {
        // A chunk result holds the fields of every schema in that chunk, so it
        // names no single source. `ArrowTypeWidening::merge_schemas` folds the
        // named schemas again when such a merge fails.
        partial.push(LabeledSchema::unlabeled(result?));
    }
    // `per_thread` is 32 or more. This call therefore gets at most one part in 32
    // of the input, and the recursion stops.
    merge_distinct(strategy, &distinct_schemas(&partial))
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema, TimeUnit};

    use super::*;

    /// A schema without a source name. Most tests read the types alone.
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

    fn names(schema: &Schema) -> Vec<&str> {
        schema.fields().iter().map(|f| f.name().as_str()).collect()
    }

    #[test]
    fn merging_no_schemas_is_an_error() {
        let widening = ArrowTypeWidening::default_extension();
        let err = widening.merge_schemas(&[]).unwrap_err();
        assert!(
            matches!(err, ArrowError::SchemaError(_)),
            "expected a schema error, got {err:?}"
        );
    }

    #[test]
    fn merge_unions_fields_in_first_seen_order() {
        let widening = ArrowTypeWidening::default_extension();
        let merged = widening
            .merge_schemas(&[
                schema(&[("a", DataType::Int32), ("b", DataType::Utf8)]),
                // `b` repeats with an identical type (deduped), `c` is new.
                schema(&[("b", DataType::Utf8), ("c", DataType::Float64)]),
            ])
            .unwrap();

        assert_eq!(names(&merged), vec!["a", "b", "c"]);
    }

    /// Two numeric types widen to the type that holds both. The schema order does
    /// not change the answer. Issue #377 was an answer that changed with it.
    #[test]
    fn two_numeric_types_widen_in_either_order() {
        let widening = ArrowTypeWidening::default_extension();
        let narrow = schema(&[("a", DataType::Int32)]);
        let wide = schema(&[("a", DataType::Int64)]);

        for order in [vec![narrow.clone(), wide.clone()], vec![wide, narrow]] {
            let merged = widening.merge_schemas(&order).unwrap();
            assert_eq!(
                merged.field_with_name("a").unwrap().data_type(),
                &DataType::Int64
            );
        }
    }

    /// A numeric type and a string have no common type. The merge reports the
    /// column and both types, in either order.
    #[test]
    fn a_number_and_a_string_are_refused_in_either_order() {
        let widening = ArrowTypeWidening::default_extension();
        let number = schema(&[("a", DataType::Int32)]);
        let text = schema(&[("a", DataType::Utf8)]);

        for order in [vec![number.clone(), text.clone()], vec![text, number]] {
            match widening.merge_schemas(&order).unwrap_err() {
                ArrowError::SchemaError(message) => {
                    assert!(
                        message.contains('a'),
                        "message should name the field: {message}"
                    );
                }
                other => panic!("expected SchemaError, got {other:?}"),
            }
        }
    }

    // ── the setting for a column that no type holds ────────────────────

    /// The merge rule that keeps the first type of a refused column.
    fn keeping_first() -> ArrowTypeWidening {
        ArrowTypeWidening::new(Arc::new(DefaultArrowTypeWidening::keeping_first_type()))
    }

    /// The column of a merge, by name.
    fn field_of<'a>(schema: &'a Schema, name: &str) -> &'a FieldRef {
        schema
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .unwrap_or_else(|| panic!("the merge holds '{name}'"))
    }

    /// `KeepFirst` reports the type of the first file instead of an error.
    #[test]
    fn the_setting_keeps_the_type_of_the_first_file() {
        let text = from_file("argo/a.nc", &[("depth", DataType::Utf8)]);
        let number = from_file("argo/b.nc", &[("depth", DataType::Float64)]);

        let merged = keeping_first()
            .merge_schemas(&[text, number])
            .expect("the setting settles the column");
        assert_eq!(field_of(&merged, "depth").data_type(), &DataType::Utf8);
    }

    /// The first file is the first in listing order, so the two orders report
    /// two types. `Fail` refuses both orders alike.
    #[test]
    fn the_setting_reads_the_order() {
        let text = from_file("argo/a.nc", &[("depth", DataType::Utf8)]);
        let number = from_file("argo/b.nc", &[("depth", DataType::Float64)]);

        let first = keeping_first()
            .merge_schemas(&[text.clone(), number.clone()])
            .expect("merge");
        let second = keeping_first().merge_schemas(&[number, text]).expect("merge");
        assert_eq!(field_of(&first, "depth").data_type(), &DataType::Utf8);
        assert_eq!(field_of(&second, "depth").data_type(), &DataType::Float64);
    }

    /// The merge marks the column, so the scan reads a value the type cannot
    /// hold as null. The column turns nullable for that reason.
    #[test]
    fn a_kept_column_is_marked_and_nullable() {
        let text = LabeledSchema::unlabeled(Arc::new(Schema::new(vec![Field::new(
            "depth",
            DataType::Utf8,
            false,
        )])));
        let number = LabeledSchema::unlabeled(Arc::new(Schema::new(vec![Field::new(
            "depth",
            DataType::Float64,
            false,
        )])));

        let merged = keeping_first().merge_schemas(&[text, number]).expect("merge");
        let field = field_of(&merged, "depth");
        assert!(is_type_conflict(field), "the merge marks the column");
        assert!(field.is_nullable(), "a null needs a nullable column");
    }

    /// A column that widens takes the rules of the module either way, and the
    /// merge leaves it unmarked.
    #[test]
    fn the_setting_changes_no_column_that_widens() {
        let narrow = schema(&[("v", DataType::Int32)]);
        let wide = schema(&[("v", DataType::Float64)]);

        let merged = keeping_first().merge_schemas(&[narrow, wide]).expect("merge");
        let field = field_of(&merged, "v");
        assert_eq!(field.data_type(), &DataType::Float64);
        assert!(
            !is_type_conflict(field),
            "a widened column needs no lenient cast"
        );
    }

    /// The mark outlives a later widening. `Utf8` still needs a lenient cast
    /// after `Int64` and `Float64` join above it.
    #[test]
    fn the_mark_survives_a_later_widening() {
        let schemas = [
            schema(&[("v", DataType::Int64)]),
            schema(&[("v", DataType::Utf8)]),
            schema(&[("v", DataType::Float64)]),
        ];

        let merged = keeping_first().merge_schemas(&schemas).expect("merge");
        let field = field_of(&merged, "v");
        assert_eq!(field.data_type(), &DataType::Float64);
        assert!(is_type_conflict(field), "the `Utf8` file still casts to null");
    }

    /// The default refuses the column, as before the setting existed.
    #[test]
    fn the_default_still_refuses_a_column_that_no_type_holds() {
        let widening = ArrowTypeWidening::default_extension();
        let text = from_file("argo/a.nc", &[("depth", DataType::Utf8)]);
        let number = from_file("argo/b.nc", &[("depth", DataType::Float64)]);

        assert!(widening.merge_schemas(&[text, number]).is_err());
    }

    /// `KeepFirst` reads the order, so it gets one fold over every schema.
    /// `Fail` is a semilattice join and keeps the threads.
    #[test]
    fn only_the_default_is_order_independent() {
        assert!(DefaultArrowTypeWidening::new().is_order_independent());
        assert!(!DefaultArrowTypeWidening::keeping_first_type().is_order_independent());
    }

    /// The names `BEACON_TYPE_WIDENING_ON_CONFLICT` takes.
    #[test]
    fn the_setting_parses_its_option_names() {
        for name in ["fail", "FAIL", " error ", ""] {
            assert_eq!(TypeConflict::parse(name), Ok(TypeConflict::Fail), "{name}");
        }
        for name in ["keep_first", "keep-first", "First"] {
            assert_eq!(
                TypeConflict::parse(name),
                Ok(TypeConflict::KeepFirst),
                "{name}"
            );
        }
        assert_eq!(TypeConflict::parse("widen"), Err("widen".to_string()));
    }

    // ── naming the source of a conflict ────────────────────────────────

    /// The error names the file of each type. A table over 10000 files is
    /// otherwise a search. See issue #424.
    #[test]
    fn a_refused_column_names_both_files() {
        let widening = ArrowTypeWidening::default_extension();
        let text = from_file("argo/a.nc", &[("depth", DataType::Utf8)]);
        let number = from_file("argo/b.nc", &[("depth", DataType::Float64)]);

        let message = message_of(widening.merge_schemas(&[text, number]).unwrap_err());
        assert_eq!(
            message,
            "Incompatible types for field 'depth': Utf8 in 'argo/a.nc' vs \
             Float64 in 'argo/b.nc'"
        );
    }

    /// A caller without a name gets the types alone, as before issue #424.
    #[test]
    fn a_schema_without_a_name_reports_its_type_alone() {
        let widening = ArrowTypeWidening::default_extension();
        let text = schema(&[("depth", DataType::Utf8)]);
        let number = from_file("argo/b.nc", &[("depth", DataType::Float64)]);

        assert_eq!(
            message_of(widening.merge_schemas(&[text, number]).unwrap_err()),
            "Incompatible types for field 'depth': Utf8 vs Float64 in 'argo/b.nc'"
        );
    }

    /// A widened column names the file that states the type it now holds, not the
    /// file that first held the column.
    #[test]
    fn a_widened_column_names_the_file_of_its_current_type() {
        let widening = ArrowTypeWidening::default_extension();
        let narrow = from_file("a.nc", &[("depth", DataType::Int32)]);
        let wide = from_file("b.nc", &[("depth", DataType::Int64)]);
        let text = from_file("c.nc", &[("depth", DataType::Utf8)]);

        assert_eq!(
            message_of(widening.merge_schemas(&[narrow, wide, text]).unwrap_err()),
            "Incompatible types for field 'depth': Int64 in 'b.nc' vs Utf8 in 'c.nc'"
        );
    }

    /// A join that equals neither operand has no file behind it. `Int64` beside
    /// `UInt64` gives `Float64`, and no file states `Float64`.
    #[test]
    fn a_join_of_two_files_names_neither() {
        let widening = ArrowTypeWidening::default_extension();
        let signed = from_file("a.nc", &[("depth", DataType::Int64)]);
        let unsigned = from_file("b.nc", &[("depth", DataType::UInt64)]);
        let text = from_file("c.nc", &[("depth", DataType::Utf8)]);

        assert_eq!(
            message_of(
                widening
                    .merge_schemas(&[signed, unsigned, text])
                    .unwrap_err()
            ),
            "Incompatible types for field 'depth': Float64 vs Utf8 in 'c.nc'"
        );
    }

    /// A repeated schema keeps the name of its first file. The merge drops the
    /// later copies, which state the same types.
    #[test]
    fn a_repeated_schema_keeps_the_name_of_its_first_file() {
        let widening = ArrowTypeWidening::default_extension();
        let first = from_file("a.nc", &[("depth", DataType::Utf8)]);
        let copy = from_file("z.nc", &[("depth", DataType::Utf8)]);
        let number = from_file("b.nc", &[("depth", DataType::Float64)]);

        assert_eq!(
            message_of(widening.merge_schemas(&[first, copy, number]).unwrap_err()),
            "Incompatible types for field 'depth': Utf8 in 'a.nc' vs Float64 in 'b.nc'"
        );
    }

    /// A merge that splits over threads still names both files. A chunk result
    /// names no file, so the failed merge folds the named schemas once more.
    #[test]
    fn a_split_merge_names_both_files() {
        // Enough distinct schemas to reach the threads, and a conflict in the
        // last chunk.
        let mut schemas: Vec<LabeledSchema> = (0..300)
            .map(|index| {
                from_file(
                    &format!("argo/{index}.nc"),
                    &[
                        ("depth", DataType::Utf8),
                        (&format!("own_{index}"), DataType::Int32),
                    ],
                )
            })
            .collect();
        schemas.push(from_file("argo/bad.nc", &[("depth", DataType::Float64)]));

        let message = message_of(
            ArrowTypeWidening::default_extension()
                .merge_schemas(&schemas)
                .unwrap_err(),
        );
        assert_eq!(
            message,
            "Incompatible types for field 'depth': Utf8 in 'argo/0.nc' vs \
             Float64 in 'argo/bad.nc'"
        );
    }

    fn message_of(error: ArrowError) -> String {
        match error {
            ArrowError::SchemaError(message) => message,
            other => panic!("expected SchemaError, got {other:?}"),
        }
    }

    /// A `Null` column holds no value, so it takes the type of the other file.
    #[test]
    fn a_null_column_takes_the_other_type() {
        let widening = ArrowTypeWidening::default_extension();
        let empty = schema(&[("a", DataType::Null)]);

        for other in [DataType::Int32, DataType::Utf8, DataType::Date32] {
            let merged = widening
                .merge_schemas(&[empty.clone(), schema(&[("a", other.clone())])])
                .unwrap();
            assert_eq!(merged.field_with_name("a").unwrap().data_type(), &other);
        }
    }

    /// A type with no rule merges with itself, and the merge keeps it as it is.
    ///
    /// Equality is the first check, so it covers every Arrow type. A geometry, a
    /// decimal, a list, a dictionary and a duration need no rule of their own.
    #[test]
    fn a_type_without_a_rule_merges_with_itself() {
        let geometry = |y: DataType| {
            DataType::Struct(Fields::from(vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", y, false),
            ]))
        };
        let widening = ArrowTypeWidening::default_extension();

        for data_type in [
            geometry(DataType::Float64),
            DataType::Decimal128(10, 2),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            DataType::FixedSizeBinary(16),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::Duration(TimeUnit::Second),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Boolean,
        ] {
            let one = schema(&[("a", data_type.clone())]);

            // Two files that state the type keep it.
            let merged = widening.merge_schemas(&[one.clone(), one.clone()]).unwrap();
            assert_eq!(
                merged.field_with_name("a").unwrap().data_type(),
                &data_type,
                "{data_type:?} with itself"
            );

            // A null column still takes it.
            let with_null = widening
                .merge_schemas(&[one.clone(), schema(&[("a", DataType::Null)])])
                .unwrap();
            assert_eq!(
                with_null.field_with_name("a").unwrap().data_type(),
                &data_type,
                "{data_type:?} with Null"
            );

            // Any other type is an error.
            assert!(
                widening
                    .merge_schemas(&[one, schema(&[("a", DataType::Int32)])])
                    .is_err(),
                "{data_type:?} and Int32"
            );
        }

        // Two geometries that differ in one child are two types, because type
        // equality reads the whole struct.
        assert!(
            widening
                .merge_schemas(&[
                    schema(&[("a", geometry(DataType::Float64))]),
                    schema(&[("a", geometry(DataType::Float32))]),
                ])
                .is_err(),
            "two point types with two coordinate widths"
        );
    }

    /// A type outside the numeric set widens with nothing but itself and `Null`.
    #[test]
    fn a_type_outside_the_numeric_set_is_refused() {
        let widening = ArrowTypeWidening::default_extension();
        for outsider in [DataType::Date32, DataType::Binary, DataType::Boolean] {
            let one = schema(&[("a", outsider.clone())]);
            assert_eq!(
                widening
                    .merge_schemas(&[one.clone(), one.clone()])
                    .unwrap()
                    .field_with_name("a")
                    .unwrap()
                    .data_type(),
                &outsider
            );
            assert!(
                widening
                    .merge_schemas(&[one, schema(&[("a", DataType::Int32)])])
                    .is_err(),
                "{outsider:?} and Int32"
            );
        }
    }

    // ── the numeric lattice ────────────────────────────────────────────
    //
    // Every rule comes from `NUMERIC_COVERS`. These tests check the derivation:
    // that the list is a lattice, that the indices run with the order, and that
    // the join therefore holds the three properties.

    /// `join` takes the lowest common index. That index is the least upper bound
    /// only while the indices run with the order.
    #[test]
    fn the_numeric_list_is_declared_in_order() {
        for (index, member) in NUMERICS.iter().enumerate() {
            assert_eq!(*member as usize, index, "{member:?} is not at its index");
        }
        for (lower, above) in NUMERIC_COVERS {
            assert!(
                (*lower as usize) < (*above as usize),
                "{lower:?} must come before {above:?}"
            );
        }
    }

    /// Every pair has one least upper bound. That property makes the order a
    /// lattice, and it makes the join associative.
    #[test]
    fn every_numeric_pair_has_one_least_upper_bound() {
        let above = |member: Numeric, other: Numeric| {
            NUMERIC_UPPER_SETS[other as usize] & (1 << member as usize) != 0
        };
        for left in NUMERICS {
            for right in NUMERICS {
                let bound = join(left, right);
                assert!(above(bound, left) && above(bound, right), "not a bound");

                let common = NUMERIC_UPPER_SETS[left as usize] & NUMERIC_UPPER_SETS[right as usize];
                for candidate in NUMERICS {
                    if common & (1 << candidate as usize) != 0 {
                        assert!(
                            above(candidate, bound),
                            "{left:?} v {right:?} = {bound:?}, and {candidate:?} \
                             is a common bound below it"
                        );
                    }
                }
            }
        }
    }

    /// Types for the property tests: every numeric type, every chain family, two
    /// timestamps, a second time zone, and `Null`.
    fn every_type() -> Vec<DataType> {
        NUMERICS
            .iter()
            .map(|member| member.data_type())
            .chain([
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
            ])
            .collect()
    }

    /// The operand order does not change the answer. That was the fault in issue
    /// #377.
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

    /// The group boundaries do not change the answer. This property lets the
    /// merge split its work across threads.
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

    /// A type widened with itself is itself. This property lets the merge drop a
    /// repeated schema.
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

    /// Two timestamps take the finer unit, which loses no value.
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

        // One time zone, two units: the zone stays.
        let utc = |unit: TimeUnit| DataType::Timestamp(unit, Some("UTC".into()));
        assert_eq!(
            super_type(&utc(TimeUnit::Second), &utc(TimeUnit::Millisecond)),
            Some(utc(TimeUnit::Millisecond))
        );
    }

    /// A time zone follows DataFusion. One zone wins over no zone, and two zones
    /// give UTC.
    #[test]
    fn a_time_zone_coerces_to_utc() {
        let stamp = |zone: Option<&str>| {
            DataType::Timestamp(TimeUnit::Second, zone.map(|zone| zone.into()))
        };

        // One file states a zone, and the tz-naive file reads as that zone.
        assert_eq!(
            super_type(&stamp(None), &stamp(Some("Europe/Berlin"))),
            Some(stamp(Some("Europe/Berlin")))
        );
        // Two zones give UTC, in either order.
        assert_eq!(
            super_type(&stamp(Some("Europe/Berlin")), &stamp(Some("America/Lima"))),
            Some(stamp(Some("UTC")))
        );
        assert_eq!(
            super_type(&stamp(Some("America/Lima")), &stamp(Some("Europe/Berlin"))),
            Some(stamp(Some("UTC")))
        );
        // "+00:00" names UTC, and the pair reaches UTC through the same rule.
        assert_eq!(
            super_type(&stamp(Some("+00:00")), &stamp(Some("UTC"))),
            Some(stamp(Some("UTC")))
        );
        // One zone, stated twice, stays as it is.
        assert_eq!(
            super_type(&stamp(Some("+00:00")), &stamp(Some("+00:00"))),
            Some(stamp(Some("+00:00")))
        );
        // Two files without a zone keep the column tz-naive.
        assert_eq!(super_type(&stamp(None), &stamp(None)), Some(stamp(None)));

        // A zone and a unit at once.
        assert_eq!(
            super_type(
                &DataType::Timestamp(TimeUnit::Second, Some("Europe/Berlin".into())),
                &DataType::Timestamp(TimeUnit::Nanosecond, Some("America/Lima".into()))
            ),
            Some(DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())))
        );

        // A timestamp is not a number.
        assert_eq!(super_type(&stamp(None), &DataType::Int64), None);
    }

    /// The other chains: a string, a binary, a date and a time.
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

        // Two families never widen.
        for (left, right) in [
            (DataType::Utf8, DataType::Binary),
            (DataType::Date32, DataType::Int32),
            (DataType::LargeUtf8, DataType::Float64),
            (DataType::Date64, DataType::Time64(TimeUnit::Nanosecond)),
        ] {
            assert_eq!(super_type(&left, &right), None, "{left:?} and {right:?}");
        }
    }

    /// The rules of the table in the module docs.
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
            (DataType::UInt8, DataType::UInt32, DataType::UInt32),
        ] {
            assert_eq!(
                super_type(&left, &right),
                Some(expected.clone()),
                "{left:?} v {right:?}"
            );
        }
    }

    /// A field keeps the metadata of the first file that states it. Formats depend
    /// on this. The GeoArrow keys of a geometry column live in field metadata.
    #[test]
    fn a_field_keeps_its_metadata() {
        let geometry = || {
            LabeledSchema::unlabeled(Arc::new(Schema::new(vec![
                Field::new("geometry", DataType::Float64, false).with_metadata(
                    [(
                        "ARROW:extension:name".to_string(),
                        "geoarrow.point".to_string(),
                    )]
                    .into(),
                ),
            ])))
        };

        let merged = ArrowTypeWidening::default_extension()
            .merge_schemas(&[geometry(), geometry()])
            .unwrap();

        let field = merged.field_with_name("geometry").unwrap();
        assert_eq!(
            field
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("geoarrow.point")
        );
        assert!(!field.is_nullable(), "every schema holds it, and requires it");
    }

    /// A column that some files lack must be nullable. The scan fills the column
    /// with nulls for those files. It refuses to fill a non-nullable column.
    #[test]
    fn a_column_some_files_lack_comes_out_nullable() {
        let required = |name: &str| {
            LabeledSchema::unlabeled(Arc::new(Schema::new(vec![Field::new(
                name,
                DataType::Float64,
                false,
            )])))
        };

        let merged = ArrowTypeWidening::default_extension()
            .merge_schemas(&[required("TEMP"), required("SALINITY")])
            .unwrap();
        assert!(merged.field_with_name("TEMP").unwrap().is_nullable());
        assert!(merged.field_with_name("SALINITY").unwrap().is_nullable());

        // A column that every file holds keeps the declared nullability. One file
        // that permits nulls makes the column nullable. No such file leaves the
        // column required.
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
            let merged = ArrowTypeWidening::default_extension()
                .merge_schemas(&order)
                .unwrap();
            assert!(!merged.field_with_name("TEMP").unwrap().is_nullable());
            assert!(merged.field_with_name("DEPTH").unwrap().is_nullable());
        }

        // One file alone keeps its own declaration.
        let alone = ArrowTypeWidening::default_extension()
            .merge_schemas(&[both_required])
            .unwrap();
        assert!(alone.fields().iter().all(|field| !field.is_nullable()));
    }

    // ── dropping repeats and splitting the work ────────────────────────

    #[test]
    fn repeats_are_dropped_in_first_seen_order() {
        let a = schema(&[("a", DataType::Int32)]);
        let b = schema(&[("b", DataType::Int32)]);
        // A second schema with the fields of `a` and a new allocation. The
        // fingerprint must catch it, because the pointers differ.
        let a_again = schema(&[("a", DataType::Int32)]);

        let listing = [a.clone(), a.clone(), b.clone(), a_again, b.clone()];
        let distinct = distinct_schemas(&listing);
        assert_eq!(distinct.len(), 2);
        assert_eq!(names(&distinct[0].schema), vec!["a"]);
        assert_eq!(names(&distinct[1].schema), vec!["b"]);

        // The input holds no repeat, so the function returns it unchanged.
        let no_repeats = [a, b];
        assert!(matches!(distinct_schemas(&no_repeats), Cow::Borrowed(_)));
    }

    /// Two schemas with one difference in nullability or in metadata are not
    /// equal. The fingerprint must show the difference. A merge would otherwise
    /// drop one schema and report the other.
    #[test]
    fn schemas_that_differ_in_more_than_types_stay_distinct() {
        let nullable = schema(&[("a", DataType::Int32)]);
        let required = LabeledSchema::unlabeled(Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Int32,
            false,
        )])));
        assert_eq!(distinct_schemas(&[nullable.clone(), required]).len(), 2);

        let with_metadata = LabeledSchema::unlabeled(Arc::new(
            Schema::new(vec![Field::new("a", DataType::Int32, true)])
                .with_metadata([("k".to_string(), "v".to_string())].into()),
        ));
        assert_eq!(distinct_schemas(&[nullable, with_metadata]).len(), 2);
    }

    /// The split must stay invisible. This case holds enough distinct schemas to
    /// reach the threads. The answer equals the answer of one fold.
    #[test]
    fn a_split_merge_gives_what_one_fold_gives() {
        // One shared numeric column, at a width that changes per schema, and one
        // own column. The union holds 301 fields, and every schema differs.
        let widths = [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::UInt8,
            DataType::Float32,
        ];
        let schemas: Vec<LabeledSchema> = (0..300)
            .map(|index| {
                schema(&[
                    ("shared", widths[index % widths.len()].clone()),
                    (&format!("own_{index}"), DataType::Utf8),
                ])
            })
            .collect();

        let merged = ArrowTypeWidening::default_extension()
            .merge_schemas(&schemas)
            .unwrap();
        let folded = DefaultArrowTypeWidening::new().merge_schemas(&schemas).unwrap();
        assert_eq!(merged, folded, "the split changed the answer");

        // Int32 v Float32 needs a Float64, and it reaches every chunk.
        assert_eq!(
            merged.field_with_name("shared").unwrap().data_type(),
            &DataType::Float64
        );
        assert_eq!(merged.fields().len(), 301);
        assert_eq!(names(&merged)[..2], ["shared", "own_0"]);
    }

    /// A conflict in one chunk fails the whole merge. The chunk position does not
    /// matter. A split that hides a conflict, or that reports one only from the
    /// first chunk, gives back the order dependence of issue #377.
    #[test]
    fn a_split_merge_reports_a_conflict_wherever_it_falls() {
        let schemas: Vec<LabeledSchema> = (0..300)
            .map(|index| {
                schema(&[
                    ("shared", DataType::Int32),
                    (&format!("own_{index}"), DataType::Utf8),
                ])
            })
            .collect();
        let widening = ArrowTypeWidening::default_extension();
        assert!(widening.merge_schemas(&schemas).is_ok());

        for position in [0, 150, 300] {
            let mut conflicting = schemas.clone();
            // Every other schema gives `shared` the type Int32.
            conflicting.insert(position, schema(&[("shared", DataType::Date32)]));
            assert!(
                widening.merge_schemas(&conflicting).is_err(),
                "a conflict at {position} was not reported"
            );
        }

        // A second copy of every schema does not change a clean merge.
        let doubled: Vec<LabeledSchema> = schemas.iter().chain(schemas.iter()).cloned().collect();
        assert_eq!(
            widening.merge_schemas(&doubled).unwrap(),
            widening.merge_schemas(&schemas).unwrap()
        );
    }

    /// An order-sensitive strategy gets one fold over its input. It sees every
    /// schema, in order, with the repeats.
    #[test]
    fn an_order_sensitive_strategy_sees_every_schema() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingStrategy {
            calls: AtomicUsize,
            seen: AtomicUsize,
        }

        impl ArrowTypeWideningStrategy for CountingStrategy {
            fn merge_schemas(&self, schemas: &[LabeledSchema]) -> Result<SchemaRef, ArrowError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.seen.fetch_add(schemas.len(), Ordering::SeqCst);
                DefaultArrowTypeWidening::new().merge_schemas(schemas)
            }

            fn is_order_independent(&self) -> bool {
                false
            }
        }

        let strategy = Arc::new(CountingStrategy {
            calls: AtomicUsize::new(0),
            seen: AtomicUsize::new(0),
        });
        let widening = ArrowTypeWidening::new(strategy.clone());
        let one = schema(&[("a", DataType::Int32)]);
        let repeated: Vec<LabeledSchema> = (0..200).map(|_| one.clone()).collect();

        widening.merge_schemas(&repeated).unwrap();
        assert_eq!(strategy.calls.load(Ordering::SeqCst), 1, "one fold");
        assert_eq!(strategy.seen.load(Ordering::SeqCst), 200, "every schema");
    }
}
