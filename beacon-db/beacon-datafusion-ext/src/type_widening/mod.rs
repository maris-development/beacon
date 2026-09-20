//! Merges the schemas of a table into the one schema a query plans against.
//!
//! [`session_widening`] gives the merge rule of a session, and
//! [`ArrowTypeWidening::merge_schemas`] applies it. Every strategy unions the
//! fields in first seen order and keeps the metadata of the first source. A
//! field turns nullable when a source permits nulls, lacks the column, or holds
//! a type the [`TypeConflict`] setting dropped. [`default`] and [`numpy`] hold
//! the two strategies, and `common` holds the field union they share.

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use arrow_schema::{ArrowError, DataType, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::physical_plan::FileOpener;
use object_store::ObjectMeta;

mod common;
pub mod default;
pub mod numpy;

pub use default::DefaultArrowTypeWidening;
pub use numpy::NumpyArrowTypeWidening;

use crate::scan_adapt::AdaptingOpener;

/// Below this count, one fold costs less than a split.
const SEQUENTIAL_MERGE_LIMIT: usize = 64;

/// The least work for one thread.
const MIN_SCHEMAS_PER_THREAD: usize = 32;

/// A schema and the name of the source it was read from. An error names both
/// sources of a refused column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabeledSchema {
    pub schema: SchemaRef,
    /// `None` for a caller without a name. The error then reports the type alone.
    pub label: Option<Arc<str>>,
}

impl LabeledSchema {
    pub fn new(schema: SchemaRef, label: impl Into<Arc<str>>) -> Self {
        Self {
            schema,
            label: Some(label.into()),
        }
    }

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

/// Name each schema after the object at the same index. A schema past the end
/// of `objects` gets no name.
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

/// The merge rule of a session.
pub struct ArrowTypeWidening {
    pub strategy: Arc<dyn ArrowTypeWideningStrategy>,
}

impl ArrowTypeWidening {
    pub fn new(strategy: Arc<dyn ArrowTypeWideningStrategy>) -> Self {
        Self { strategy }
    }

    /// The rule for a session that registers none.
    pub fn default_extension() -> Arc<Self> {
        Arc::new(Self::new(Arc::new(DefaultArrowTypeWidening::new())))
    }

    /// Merge `schemas` into one. An order-independent strategy merges the
    /// distinct schemas in threads. Any other strategy gets one fold, in order.
    pub fn merge_schemas(&self, schemas: &[LabeledSchema]) -> Result<SchemaRef, ArrowError> {
        let strategy = self.strategy.as_ref();
        if !strategy.is_order_independent() {
            return strategy.merge_schemas(schemas);
        }
        let distinct = distinct_schemas(schemas);
        match merge_distinct(strategy, &distinct) {
            Ok(schema) => Ok(schema),
            // The dropped repeats and the chunk results hide the source of a
            // field. One fold over the named schemas stops at the same pair.
            Err(error) => Err(strategy.merge_schemas(&distinct).err().unwrap_or(error)),
        }
    }

    /// `opener`, with every batch mapped onto `target`. See
    /// [`scan_adapt`](crate::scan_adapt).
    pub fn scan_adapter(
        &self,
        target: SchemaRef,
        opener: Arc<dyn FileOpener>,
    ) -> Arc<dyn FileOpener> {
        Arc::new(AdaptingOpener::new(
            opener,
            target,
            Arc::clone(&self.strategy),
        ))
    }
}

/// The merge rule of `session`, or [`ArrowTypeWidening::default_extension`]
/// when it registers none.
pub fn session_widening(session: &dyn Session) -> Arc<ArrowTypeWidening> {
    session
        .config()
        .get_extension::<ArrowTypeWidening>()
        .unwrap_or_else(ArrowTypeWidening::default_extension)
}

/// The rule a merge applies.
pub trait ArrowTypeWideningStrategy: std::fmt::Debug + Send + Sync {
    /// Merge `schemas` into one, in the order given. Report both sources for a
    /// column that two of them type in two families.
    fn merge_schemas(&self, schemas: &[LabeledSchema]) -> Result<SchemaRef, ArrowError>;

    /// Whether the merge is idempotent, commutative and associative. `true`
    /// lets [`ArrowTypeWidening::merge_schemas`] drop repeats and use threads.
    fn is_order_independent(&self) -> bool {
        true
    }

    /// The setting this strategy applies to a column that no type holds.
    fn on_conflict(&self) -> TypeConflict {
        TypeConflict::Fail
    }

    /// Whether a file column of `source` may read null where a table column of
    /// `target` cannot hold its value. `false` gives a strict cast, and such a
    /// value is an error.
    fn casts_leniently(&self, source: &DataType, target: &DataType) -> bool {
        let _ = (source, target);
        false
    }
}

/// What the merge does with a column that no type holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TypeConflict {
    /// Refuse the merge. The error names the column, both types and both
    /// sources.
    #[default]
    Fail,
    /// Keep the type the merge met first. The column turns nullable, and the
    /// scan reads a source of another family as null.
    KeepFirst,
}

impl TypeConflict {
    /// The setting `value` names, or `value` itself when it names none.
    /// `BEACON_TYPE_WIDENING_ON_CONFLICT` holds the value.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "fail" | "error" | "" => Ok(Self::Fail),
            "keep_first" | "keep-first" | "first" => Ok(Self::KeepFirst),
            other => Err(other.to_string()),
        }
    }
}

/// The schemas without a repeat, in first seen order. Two equal fingerprints
/// get a full compare.
fn distinct_schemas(schemas: &[LabeledSchema]) -> Cow<'_, [LabeledSchema]> {
    if schemas.len() < 2 {
        return Cow::Borrowed(schemas);
    }

    let mut kept: Vec<LabeledSchema> = Vec::new();
    let mut seen: HashMap<u64, Vec<usize>> = HashMap::new();
    for labeled in schemas {
        let candidates = seen.entry(fingerprint(&labeled.schema)).or_default();
        // The first of a repeat keeps its name. A later copy states the same types.
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
        return Cow::Borrowed(schemas);
    }
    Cow::Owned(kept)
}

/// A hash over every part that [`Schema`] equality reads. One process makes
/// and reads these values, so the hasher needs no stability across releases.
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

/// Merge in threads, one contiguous chunk each, and then merge the chunk
/// results the same way. The chunks combine in listing order.
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
            // A merge is pure, so a panic is a defect, not a schema error.
            .map(|handle| {
                handle
                    .join()
                    .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
            })
            .collect()
    });

    let mut partial = Vec::with_capacity(results.len());
    for result in results {
        // A chunk result names no single source. The caller folds the named
        // schemas again when the merge fails.
        partial.push(LabeledSchema::unlabeled(result?));
    }
    // `per_thread` is 32 or more, so each level shrinks the input and the
    // recursion ends.
    merge_distinct(strategy, &distinct_schemas(&partial))
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};

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

    fn names(schema: &Schema) -> Vec<&str> {
        schema.fields().iter().map(|f| f.name().as_str()).collect()
    }

    fn message_of(error: ArrowError) -> String {
        match error {
            ArrowError::SchemaError(message) => message,
            other => panic!("expected SchemaError, got {other:?}"),
        }
    }

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

    #[test]
    fn repeats_are_dropped_in_first_seen_order() {
        let a = schema(&[("a", DataType::Int32)]);
        let b = schema(&[("b", DataType::Int32)]);
        // The same fields in a new allocation, so the pointers differ.
        let a_again = schema(&[("a", DataType::Int32)]);

        let listing = [a.clone(), a.clone(), b.clone(), a_again, b.clone()];
        let distinct = distinct_schemas(&listing);
        assert_eq!(distinct.len(), 2);
        assert_eq!(names(&distinct[0].schema), vec!["a"]);
        assert_eq!(names(&distinct[1].schema), vec!["b"]);

        let no_repeats = [a, b];
        assert!(matches!(distinct_schemas(&no_repeats), Cow::Borrowed(_)));
    }

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

    #[test]
    fn a_split_merge_gives_what_one_fold_gives() {
        // 300 distinct schemas reach the threads, and every one widens `shared`.
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
        let folded = DefaultArrowTypeWidening::new()
            .merge_schemas(&schemas)
            .unwrap();
        assert_eq!(merged, folded, "the split changed the answer");

        assert_eq!(
            merged.field_with_name("shared").unwrap().data_type(),
            &DataType::Float64
        );
        assert_eq!(merged.fields().len(), 301);
        assert_eq!(names(&merged)[..2], ["shared", "own_0"]);
    }

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
            conflicting.insert(position, schema(&[("shared", DataType::Date32)]));
            assert!(
                widening.merge_schemas(&conflicting).is_err(),
                "a conflict at {position} was not reported"
            );
        }

        let doubled: Vec<LabeledSchema> = schemas.iter().chain(schemas.iter()).cloned().collect();
        assert_eq!(
            widening.merge_schemas(&doubled).unwrap(),
            widening.merge_schemas(&schemas).unwrap()
        );
    }

    #[test]
    fn a_split_merge_names_both_files() {
        // 300 distinct schemas reach the threads. The conflict sits in the last chunk.
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

    #[test]
    fn an_order_sensitive_strategy_sees_every_schema() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug)]
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
