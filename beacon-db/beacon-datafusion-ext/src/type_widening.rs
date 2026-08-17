//! The single place that merges the schemas of a table.
//!
//! A table over files holds one schema per file. A query plans against one
//! schema. Each format merged its own schemas before this module. Five formats
//! read those schemas in disk answer order, so the merged schema changed between
//! runs. See issue #377.
//!
//! Each format now calls [`session_widening`]. The table does the same. Both then
//! merge with [`ArrowTypeWidening::merge_schemas`].
//!
//! One entry point gives two results:
//!
//! - **A deployment sets the rule once.** [`RuntimeBuilder::with_type_widening`]
//!   registers a strategy on the session. Every merge in the process uses it.
//! - **The merge gets faster in one place.** Read the next section.
//!
//! # How the merge does less work
//!
//! A strategy answers [`is_order_independent`]. `true` is a promise: the merge is
//! a semilattice join. Such a join is idempotent, commutative and associative.
//! The schema order, the group boundaries and the repeat count do not change the
//! result. They do not change a failure either. [`DefaultArrowTypeWidening`]
//! keeps that promise.
//!
//! The merge then drops work:
//!
//! - **It drops each repeated schema.** A collection of 100000 netCDF files from
//!   one instrument holds few distinct schemas. One pass finds them. The merge
//!   reads what remains.
//! - **It splits the rest across threads.** Each thread merges one contiguous
//!   chunk. The merge then combines the chunk results in the same way.
//!
//! The chunks stay contiguous and stay in listing order. Column order follows the
//! listing, because `SELECT *` shows it.
//!
//! A strategy that answers `false` gets one fold over every schema it was given.
//! The repeats stay.
//!
//! [`is_order_independent`]: ArrowTypeWideningStrategy::is_order_independent
//! [`RuntimeBuilder::with_type_widening`]: ../../beacon_core/runtime_builder/struct.RuntimeBuilder.html#method.with_type_widening

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use arrow_schema::{ArrowError, FieldRef, Schema, SchemaRef};
use datafusion::catalog::Session;

/// Below this count, one fold costs less than a split.
///
/// A merge walks field names, so one schema is little work. A thread must earn
/// its start cost. A `read_*` over a few files stays below this count. A
/// collection goes far above it.
const SEQUENTIAL_MERGE_LIMIT: usize = 64;

/// The least work for one thread.
const MIN_SCHEMAS_PER_THREAD: usize = 32;

pub struct ArrowTypeWidening {
    pub strategy: Arc<dyn ArrowTypeWideningStrategy>,
}

impl ArrowTypeWidening {
    pub fn new(strategy: Arc<dyn ArrowTypeWideningStrategy>) -> Self {
        Self { strategy }
    }

    /// The strategy for a session that registers none.
    ///
    /// Every merge in the process reads this extension. Each format merges the
    /// files behind one URL. `FastObjectTable` merges the URLs behind one table.
    /// `RuntimeBuilder` registers the extension for a server. A test or an
    /// embedded use must register it too. [`session_widening`] falls back to this
    /// value, so a session that forgets gets the default rule and no error.
    pub fn default_extension() -> Arc<Self> {
        Arc::new(Self::new(Arc::new(DefaultArrowTypeWidening)))
    }

    /// Merge `schema_refs` into the schema a query plans against.
    ///
    /// The strategy decides the result for a column that two files describe
    /// differently. This method decides the cost. An order-independent strategy
    /// loses its repeats, and threads merge the rest. See the
    /// [module docs](self).
    pub fn merge_schemas(&self, schema_refs: &[SchemaRef]) -> Result<SchemaRef, ArrowError> {
        let strategy = self.strategy.as_ref();
        if !strategy.is_order_independent() {
            return strategy.merge_schemas(schema_refs);
        }
        merge_distinct(strategy, &distinct_schemas(schema_refs))
    }
}

/// The merge rule of the session, or the default rule when the session has none.
///
/// This function is the entry point. A format calls it in `infer_schema` to merge
/// the files it got. `FastObjectTable` calls it to merge the URLs behind one
/// table. Both then get the rule that a deployment registered through
/// `RuntimeBuilder::with_type_widening`.
///
/// A session without the extension gets
/// [`ArrowTypeWidening::default_extension`]. `RuntimeBuilder` registers the same
/// rule. The fallback keeps a hand-built session correct, and it keeps that
/// session equal to a server.
pub fn session_widening(session: &dyn Session) -> Arc<ArrowTypeWidening> {
    session
        .config()
        .get_extension::<ArrowTypeWidening>()
        .unwrap_or_else(ArrowTypeWidening::default_extension)
}

pub trait ArrowTypeWideningStrategy: Send + Sync {
    /// Merge these schemas into one, in the order given.
    fn merge_schemas(&self, schema_refs: &[SchemaRef]) -> Result<SchemaRef, ArrowError>;

    /// State whether this merge is a semilattice join.
    ///
    /// Such a join is **idempotent, commutative and associative**. The schema
    /// order, the group boundaries and the repeat count do not change the result.
    /// They do not change a failure either.
    ///
    /// `true` is the default. It lets [`ArrowTypeWidening::merge_schemas`] drop a
    /// repeated schema and give each contiguous chunk to a thread.
    /// [`DefaultArrowTypeWidening`] qualifies. It unions the fields that agree and
    /// refuses the rest. The refusal does not depend on the position of the
    /// conflict.
    ///
    /// Answer `false` for a rule that reads the order. One example keeps the first
    /// type of a column. Such a merge gets one fold over every schema, first to
    /// last.
    fn is_order_independent(&self) -> bool {
        true
    }
}

/// Merge the schemas of a table. Union the fields that agree.
///
/// A table reports one type per column. Two files that give a column two types
/// are an error, not a promotion. A guess about the type that holds both values
/// made the merge depend on the file order. See issue #377.
///
/// The fields keep first seen order, which is the order `SELECT *` shows. Each
/// field keeps the metadata of the first file that states it.
///
/// A field is nullable unless every schema holds it and every schema requires it.
/// The scan fills a missing column with nulls. A non-nullable column holds no
/// nulls, so the query fails with "Non-nullable column X is missing from the
/// physical schema". Presence and nullability belong to the whole set of schemas.
/// The order, the group boundaries and the repeats do not change them.
pub struct DefaultArrowTypeWidening;

impl ArrowTypeWideningStrategy for DefaultArrowTypeWidening {
    fn merge_schemas(&self, schema_refs: &[SchemaRef]) -> Result<SchemaRef, ArrowError> {
        if schema_refs.is_empty() {
            return Err(ArrowError::SchemaError(
                "No schemas provided for merging".to_string(),
            ));
        }

        let mut merged_fields: Vec<FieldRef> = Vec::new();
        // The position of each field in `merged_fields`, and its schema count.
        let mut field_map: HashMap<String, (usize, usize)> = HashMap::new();

        for schema_ref in schema_refs {
            for field in schema_ref.fields() {
                let field_name = field.name().clone();
                if let Some((at, held_by)) = field_map.get_mut(&field_name) {
                    *held_by += 1;
                    let existing_field = &merged_fields[*at];
                    // If the field already exists, we need to check if the types are compatible
                    if existing_field.data_type() != field.data_type() {
                        return Err(ArrowError::SchemaError(format!(
                            "Incompatible types for field '{}': {:?} vs {:?}",
                            field_name,
                            existing_field.data_type(),
                            field.data_type()
                        )));
                    }
                    // One file that permits nulls makes the column nullable.
                    if field.is_nullable() && !existing_field.is_nullable() {
                        merged_fields[*at] =
                            Arc::new(existing_field.as_ref().clone().with_nullable(true));
                    }
                } else {
                    // If the field does not exist, add it to the map and merged fields
                    field_map.insert(field_name.clone(), (merged_fields.len(), 1));
                    merged_fields.push(field.clone());
                }
            }
        }

        // The scan fills a missing column with nulls. A column that some schemas
        // lack must therefore be nullable.
        for field in merged_fields.iter_mut() {
            let (_, held_by) = field_map[field.name()];
            if held_by < schema_refs.len() && !field.is_nullable() {
                *field = Arc::new(field.as_ref().clone().with_nullable(true));
            }
        }

        Ok(Arc::new(arrow_schema::Schema::new(merged_fields)))
    }
}

/// The schemas without a repeat, in first seen order.
///
/// An order-independent join is idempotent, so a repeat cannot change its
/// answer. A collection holds far fewer distinct schemas than files. This
/// function turns a merge over 100000 files into a merge over a few.
///
/// The check uses a fingerprint, not a pairwise compare. One pass reads the
/// fields. A full compare runs only for two equal fingerprints.
fn distinct_schemas(schemas: &[SchemaRef]) -> Cow<'_, [SchemaRef]> {
    if schemas.len() < 2 {
        return Cow::Borrowed(schemas);
    }

    let mut kept: Vec<SchemaRef> = Vec::new();
    let mut seen: HashMap<u64, Vec<usize>> = HashMap::new();
    for schema in schemas {
        let candidates = seen.entry(fingerprint(schema)).or_default();
        if candidates
            .iter()
            .any(|index| Arc::ptr_eq(&kept[*index], schema) || kept[*index] == *schema)
        {
            continue;
        }
        candidates.push(kept.len());
        kept.push(Arc::clone(schema));
    }

    if kept.len() == schemas.len() {
        // Nothing repeated. Hand back what we were given rather than the copy.
        return Cow::Borrowed(schemas);
    }
    Cow::Owned(kept)
}

/// A hash over every part that [`Schema`] equality reads.
///
/// One process makes and reads these values. The hash algorithm therefore needs
/// no stability across releases. The schema cache stores its keys, so
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

/// Merge with threads. Each thread takes one contiguous chunk. The chunk results
/// then merge in the same way.
///
/// The chunks stay contiguous and combine in order. The columns therefore keep
/// the order of the listing. Column order is the last result that the order
/// decides, and the split keeps it.
fn merge_distinct(
    strategy: &dyn ArrowTypeWideningStrategy,
    schemas: &[SchemaRef],
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
        partial.push(result?);
    }
    // `per_thread` is 32 or more. This call therefore gets at most one part in 32
    // of the input, and the recursion stops.
    merge_distinct(strategy, &distinct_schemas(&partial))
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
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

    /// Two files that give a column two types are an error. The schema order does
    /// not change that. Issue #377 was an answer that changed with the order.
    #[test]
    fn a_field_with_two_types_is_refused_in_either_order() {
        let widening = ArrowTypeWidening::default_extension();
        let narrow = schema(&[("a", DataType::Int32)]);
        let wide = schema(&[("a", DataType::Int64)]);

        for order in [
            vec![narrow.clone(), wide.clone()],
            vec![wide.clone(), narrow.clone()],
        ] {
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

    /// A field keeps the metadata of the first file that states it. Formats depend
    /// on this. The GeoArrow keys of a geometry column live in field metadata.
    #[test]
    fn a_field_keeps_its_metadata() {
        let geometry = || {
            Arc::new(Schema::new(vec![
                Field::new("geometry", DataType::Float64, false).with_metadata(
                    [(
                        "ARROW:extension:name".to_string(),
                        "geoarrow.point".to_string(),
                    )]
                    .into(),
                ),
            ])) as SchemaRef
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
            Arc::new(Schema::new(vec![Field::new(
                name,
                DataType::Float64,
                false,
            )])) as SchemaRef
        };

        let merged = ArrowTypeWidening::default_extension()
            .merge_schemas(&[required("TEMP"), required("SALINITY")])
            .unwrap();
        assert!(merged.field_with_name("TEMP").unwrap().is_nullable());
        assert!(merged.field_with_name("SALINITY").unwrap().is_nullable());

        // A column that every file holds keeps the declared nullability. One file
        // that permits nulls makes the column nullable. No such file leaves the
        // column required.
        let both_required = Arc::new(Schema::new(vec![
            Field::new("TEMP", DataType::Float64, false),
            Field::new("DEPTH", DataType::Int64, false),
        ])) as SchemaRef;
        let depth_optional = Arc::new(Schema::new(vec![
            Field::new("TEMP", DataType::Float64, false),
            Field::new("DEPTH", DataType::Int64, true),
        ])) as SchemaRef;

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

        let listing = [
            Arc::clone(&a),
            Arc::clone(&a),
            Arc::clone(&b),
            a_again,
            Arc::clone(&b),
        ];
        let distinct = distinct_schemas(&listing);
        assert_eq!(distinct.len(), 2);
        assert_eq!(names(&distinct[0]), vec!["a"]);
        assert_eq!(names(&distinct[1]), vec!["b"]);

        // The input holds no repeat, so the function returns it unchanged.
        let no_repeats = [a, b];
        assert!(matches!(distinct_schemas(&no_repeats), Cow::Borrowed(_)));
    }

    /// Two schemas with one difference in nullability or in metadata are not
    /// equal. The fingerprint must show the difference. A merge would otherwise
    /// drop one schema and report the other.
    #[test]
    fn schemas_that_differ_in_more_than_types_stay_distinct() {
        let nullable = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let required = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        assert_eq!(distinct_schemas(&[nullable.clone(), required]).len(), 2);

        let with_metadata: SchemaRef = Arc::new(
            Schema::new(vec![Field::new("a", DataType::Int32, true)])
                .with_metadata([("k".to_string(), "v".to_string())].into()),
        );
        assert_eq!(distinct_schemas(&[nullable, with_metadata]).len(), 2);
    }

    /// The split must stay invisible. This case holds enough distinct schemas to
    /// reach the threads. The answer equals the answer of one fold.
    #[test]
    fn a_split_merge_gives_what_one_fold_gives() {
        let schemas: Vec<SchemaRef> = (0..300)
            .map(|index| {
                schema(&[
                    // One shared column and one own column per schema. The union
                    // holds 301 fields, and every schema differs.
                    ("shared", DataType::Float64),
                    (&format!("own_{index}"), DataType::Utf8),
                ])
            })
            .collect();

        let merged = ArrowTypeWidening::default_extension()
            .merge_schemas(&schemas)
            .unwrap();
        let folded = DefaultArrowTypeWidening.merge_schemas(&schemas).unwrap();
        assert_eq!(merged, folded, "the split changed the answer");

        assert_eq!(merged.fields().len(), 301);
        assert_eq!(names(&merged)[..2], ["shared", "own_0"]);
    }

    /// A conflict in one chunk fails the whole merge. The chunk position does not
    /// matter. A split that hides a conflict, or that reports one only from the
    /// first chunk, gives back the order dependence of issue #377.
    #[test]
    fn a_split_merge_reports_a_conflict_wherever_it_falls() {
        let schemas: Vec<SchemaRef> = (0..300)
            .map(|index| schema(&[("shared", DataType::Int32), (&format!("own_{index}"), DataType::Utf8)]))
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
        let doubled: Vec<SchemaRef> = schemas.iter().chain(schemas.iter()).cloned().collect();
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
            fn merge_schemas(&self, schema_refs: &[SchemaRef]) -> Result<SchemaRef, ArrowError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.seen.fetch_add(schema_refs.len(), Ordering::SeqCst);
                DefaultArrowTypeWidening.merge_schemas(schema_refs)
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
        let repeated: Vec<SchemaRef> = (0..200).map(|_| Arc::clone(&one)).collect();

        widening.merge_schemas(&repeated).unwrap();
        assert_eq!(strategy.calls.load(Ordering::SeqCst), 1, "one fold");
        assert_eq!(strategy.seen.load(Ordering::SeqCst), 200, "every schema");
    }
}
