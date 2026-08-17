//! The schema of a table, from the cache where the cache can answer.
//!
//! # What this module replaces
//!
//! `ListingOptions::infer_schema` lists a URL and gives the whole listing to the
//! format. The format then opens every object. Nothing kept the result, so a
//! second query over 100000 netCDF files opened the same 100000 files. That work
//! measured 9.2s of a 10.4s query.
//!
//! This module runs the same listing. It then asks the schema cache about each
//! object, and it opens only the objects without an answer. A fully analysed
//! collection costs one lookup per file and one merge. A collection with 1000 new
//! files costs 1000 opens, not 100000.
//!
//! # Why a cached schema may stand in for an inferred one
//!
//! Every Beacon format reads one schema per object. It then merges them with
//! [`ArrowTypeWidening`], which unions the fields that agree. The merge refuses a
//! column that two files give two types. That merge is a semilattice join. It is
//! idempotent, commutative and associative, so a part merge plus the rest equals
//! one flat merge. The group boundaries do not matter. This property lets the
//! cache answer for part of a listing, and lets inference cover the rest.
//!
//! # Why the module keeps the listing order
//!
//! Not for the types. The merge answer no longer depends on the order. See issue
//! #377. The order counts for the columns. A user sees field order through
//! `SELECT *`, and that order follows first sight of a column. This module
//! therefore keeps the units in listing order, and the merge combines them in
//! that order.
//!
//! The cache removes the file opens, which held the 9.2s. The merge removes the
//! repeats. A collection of 100000 files from one instrument holds few distinct
//! schemas, and the merge drops the rest. See
//! [`ArrowTypeWidening::merge_schemas`].
//!
//! # Fail open
//!
//! Four cases send the module back to the format: no cache, no format
//! fingerprint, an unreadable table and a stale entry. The cache may make a query
//! faster. It may never change the answer of a query.

use std::sync::Arc;

use beacon_file_stats::{FileKey, Lookup, SchemaCache, Stamp, try_file_stats_from_session};
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    common::exec_datafusion_err,
    datasource::{
        file_format::FileFormat,
        listing::{ListingOptions, ListingTableUrl},
    },
    error::DataFusionError,
};
use futures::{StreamExt, TryStreamExt, future};
use object_store::{ObjectMeta, ObjectStore};

use crate::format_ext::{FileFormatFactoryExt, SchemaUnit, try_file_format_factory_ext};
use crate::type_widening::{ArrowTypeWidening, session_widening};

/// One schema per URL. The cache answers where it can.
///
/// The caller merges the schemas. `FastObjectTable` applies the widening rule of
/// the session. An external table has one URL and takes that schema as it is.
/// This function therefore returns one schema per URL. The merge of each caller
/// stays as it is.
pub async fn infer_url_schemas(
    state: &dyn Session,
    options: &ListingOptions,
    urls: &[ListingTableUrl],
) -> Result<Vec<SchemaRef>, DataFusionError> {
    let Some(cached) = CachedInference::for_session(state, &options.format) else {
        return infer_uncached(state, options, urls).await;
    };

    let mut schemas = Vec::with_capacity(urls.len());
    for url in urls {
        schemas.push(cached.url_schema(state, options, url).await?);
    }
    Ok(schemas)
}

/// What `ListingOptions::infer_schema` does, per URL.
///
/// A format outside the cache takes this path. Every failure falls back to it.
async fn infer_uncached(
    state: &dyn Session,
    options: &ListingOptions,
    urls: &[ListingTableUrl],
) -> Result<Vec<SchemaRef>, DataFusionError> {
    let mut schemas = Vec::with_capacity(urls.len());
    for url in urls {
        tracing::debug!("Infer schema for table/file url: {}", url);
        schemas.push(options.infer_schema(state, url).await?);
    }
    Ok(schemas)
}

/// The cache, and the format identity of its entries.
struct CachedInference {
    cache: Arc<SchemaCache>,
    factory: Arc<dyn FileFormatFactoryExt>,
    /// The option fingerprint of the format. Two option sets that read a file
    /// differently never share a key.
    fingerprint: u64,
}

impl CachedInference {
    /// `None` when this session or this format cannot use the cache. Three cases
    /// give `None`: no file statistics store, no factory for the format, and a
    /// format outside the cache. Each case is a reason to read the files. None of
    /// them is a reason to fail.
    fn for_session(state: &dyn Session, format: &Arc<dyn FileFormat>) -> Option<Self> {
        let store = try_file_stats_from_session(state)?;
        // The factory answers to the extension of the format, because the
        // registration used that extension. This lookup is the only route back to
        // the `Ext` trait. DataFusion returns `Arc<dyn FileFormatFactory>`, and
        // Rust has no upcast from it.
        let factory = try_file_format_factory_ext(state, &format.get_ext())?;
        let fingerprint = factory.schema_options_fingerprint(format.as_ref())?;
        Some(Self {
            cache: Arc::clone(store.schema_cache()),
            factory,
            fingerprint,
        })
    }

    /// The schema of one URL. List the URL. Read the cache. Then open the rest.
    async fn url_schema(
        &self,
        state: &dyn Session,
        options: &ListingOptions,
        url: &ListingTableUrl,
    ) -> Result<SchemaRef, DataFusionError> {
        let store = state.runtime_env().object_store(url)?;
        let objects = list_objects(state, options, url, store.as_ref()).await?;

        let units = self.factory.schema_units(&objects);
        if units.is_empty() {
            // The listing holds no key. The format states the result. netCDF and
            // Atlas answer with an empty schema. Zarr answers with an error.
            return options.format.infer_schema(state, &store, &objects).await;
        }

        let lookups = self.lookups(url, &objects, &units);
        let mut schemas = self.cache.file_schemas(&lookups);

        let missing: Vec<usize> = schemas
            .iter()
            .enumerate()
            .filter(|(_, schema)| schema.is_none())
            .map(|(index, _)| index)
            .collect();
        tracing::debug!(
            url = %url,
            objects = objects.len(),
            units = units.len(),
            inferred = missing.len(),
            "resolving a table's schema"
        );

        for (index, schema) in missing.iter().zip(
            self.infer_missing(state, options, &store, &objects, &units, &missing)
                .await?,
        ) {
            schemas[*index] = Some(schema);
        }

        let resolved: Vec<SchemaRef> = schemas
            .into_iter()
            .map(|schema| schema.expect("every miss was just inferred"))
            .collect();
        merge_in_listing_order(&session_widening(state), &resolved)
    }

    /// The cache request. It holds one entry per unit, in listing order.
    fn lookups(
        &self,
        url: &ListingTableUrl,
        objects: &[ObjectMeta],
        units: &[SchemaUnit],
    ) -> Vec<Lookup> {
        let store_url = url.object_store();
        units
            .iter()
            .map(|unit| Lookup {
                key: FileKey::new(
                    store_url.as_str(),
                    objects[unit.source].location.as_ref(),
                    self.fingerprint,
                ),
                stamp: stamp_unit(objects, unit),
            })
            .collect()
    }

    /// The schemas of the units without a cache answer.
    ///
    /// The width is the `meta_fetch_concurrency` of the session. The formats use
    /// the same width. A serial loop here would make a cold collection slower than
    /// no cache.
    async fn infer_missing(
        &self,
        state: &dyn Session,
        options: &ListingOptions,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
        units: &[SchemaUnit],
        missing: &[usize],
    ) -> Result<Vec<SchemaRef>, DataFusionError> {
        if missing.is_empty() {
            return Ok(Vec::new());
        }
        let width = state
            .config_options()
            .execution
            .meta_fetch_concurrency
            .max(1);
        futures::stream::iter(missing.iter().copied())
            .map(|index| {
                let object = objects[units[index].source].clone();
                let store = Arc::clone(store);
                let format = Arc::clone(&options.format);
                async move {
                    format
                        .infer_schema(state, &store, std::slice::from_ref(&object))
                        .await
                }
            })
            .buffered(width)
            .try_collect()
            .await
    }
}

/// Every object at `url`, as `ListingOptions::infer_schema` collects them.
///
/// The function drops an empty object. Such an object changes no schema, and a
/// format can fail on it.
async fn list_objects(
    state: &dyn Session,
    options: &ListingOptions,
    url: &ListingTableUrl,
    store: &dyn ObjectStore,
) -> Result<Vec<ObjectMeta>, DataFusionError> {
    url.list_all_files(state, store, &options.file_extension)
        .await?
        .try_filter(|object| future::ready(object.size > 0))
        .try_collect()
        .await
}

/// The content of one cache entry. It holds the source object first. It then
/// holds every other object of the unit, in listing order.
///
/// A plain file depends on itself. A Zarr store and an Atlas store depend on the
/// whole directory. A new array under an unchanged marker therefore retires the
/// entry.
fn stamp_unit(objects: &[ObjectMeta], unit: &SchemaUnit) -> Stamp {
    beacon_file_stats::stamp_objects(
        std::iter::once(unit.source)
            .chain(unit.dependents.iter().copied())
            .map(|index| {
                let object = &objects[index];
                (
                    object.size,
                    object.last_modified.timestamp_millis(),
                    object.e_tag.as_deref(),
                )
            }),
    )
}

/// Merge the schema of every unit, in listing order.
///
/// The sequence equals the sequence of the format before this cache, so the
/// answer stays the same. The merge decides what it skips and what it splits. The
/// order still sets the column order. See the [module docs](self).
fn merge_in_listing_order(
    widening: &ArrowTypeWidening,
    schemas: &[SchemaRef],
) -> Result<SchemaRef, DataFusionError> {
    widening
        .merge_schemas(schemas)
        .map_err(|e| exec_datafusion_err!("Failed to merge the schemas of a table: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, kind)| Field::new(*name, kind.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    fn names(schema: &Schema) -> Vec<&str> {
        schema.fields().iter().map(|f| f.name().as_str()).collect()
    }

    /// The rule for a session without an extension. A server registers the same
    /// rule.
    fn widening() -> Arc<ArrowTypeWidening> {
        ArrowTypeWidening::default_extension()
    }

    /// One flat fold over every schema, in order. It skips and splits nothing.
    /// The tests below compare the merge against this result.
    fn flat_merge(listing: &[SchemaRef]) -> SchemaRef {
        use crate::type_widening::{ArrowTypeWideningStrategy, DefaultArrowTypeWidening};
        DefaultArrowTypeWidening.merge_schemas(listing).unwrap()
    }

    /// The merge equals the earlier fold of the format, over the same sequence.
    #[test]
    fn the_merge_unions_over_the_listing() {
        let a = schema(&[("TEMP", DataType::Float64), ("DEPTH", DataType::Int32)]);
        let b = schema(&[("PSAL", DataType::Float64)]);
        let c = schema(&[("DEPTH", DataType::Int32), ("PRES", DataType::Float64)]);

        // A collection with repeats, as a real collection has.
        let listing = vec![
            Arc::clone(&a),
            Arc::clone(&a),
            Arc::clone(&b),
            Arc::clone(&a),
            Arc::clone(&c),
            Arc::clone(&b),
        ];

        let merged = merge_in_listing_order(&widening(), &listing).unwrap();
        assert_eq!(merged, flat_merge(&listing));
        assert_eq!(names(&merged), vec!["TEMP", "DEPTH", "PSAL", "PRES"]);
    }

    /// A repeated schema cannot change the answer. The merge therefore drops it.
    ///
    /// This test held the opposite claim before. Widening was a table of pairs. It
    /// gave `(Int32, Float32) -> Float32` and `(Float32, Int32) -> Float64`. A
    /// second sight of a schema could then widen a column again. See issue #377.
    /// The merge now unions what agrees and refuses the rest.
    #[test]
    fn a_repeat_cannot_move_the_answer() {
        let one = schema(&[("TEMP", DataType::Int32)]);
        let two = schema(&[("PSAL", DataType::Float64)]);

        let once =
            merge_in_listing_order(&widening(), &[Arc::clone(&one), Arc::clone(&two)]).unwrap();
        let repeated = merge_in_listing_order(
            &widening(),
            &[
                Arc::clone(&one),
                two,
                Arc::clone(&one),
                schema(&[("TEMP", DataType::Int32)]),
            ],
        )
        .unwrap();

        assert_eq!(once, repeated);
        assert_eq!(names(&once), vec!["TEMP", "PSAL"]);
    }

    /// Field order follows the listing, because `SELECT *` shows it. A reversed
    /// listing gives reversed columns, as a flat merge does.
    #[test]
    fn field_order_follows_the_listing() {
        let a = schema(&[("A", DataType::Int32)]);
        let b = schema(&[("B", DataType::Int32)]);

        let forward =
            merge_in_listing_order(&widening(), &[Arc::clone(&a), Arc::clone(&b)]).unwrap();
        let backward = merge_in_listing_order(&widening(), &[b, a]).unwrap();
        assert_eq!(names(&forward), vec!["A", "B"]);
        assert_eq!(names(&backward), vec!["B", "A"]);
    }

    /// Two files that give a column two types are an error, not a guess. The
    /// listing order does not change that.
    #[test]
    fn a_column_with_two_types_is_still_an_error() {
        let dates = schema(&[("WHEN", DataType::Date32)]);
        let counts = schema(&[("WHEN", DataType::Int32)]);
        assert!(merge_in_listing_order(&widening(), &[dates.clone(), counts.clone()]).is_err());
        assert!(merge_in_listing_order(&widening(), &[counts, dates]).is_err());
    }

    // ── the properties of the design ───────────────────────────────────
    //
    // These tests use random cases, because the interesting cases are
    // combinations. The cases stay deterministic. The generator gives a fixed
    // sequence, so a failure repeats exactly.

    /// A small linear congruential generator. These tests need no cryptographic
    /// random values. A fixed sequence makes a failed case repeat, and it needs no
    /// seed from a log.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self, bound: usize) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            ((self.0 >> 33) as usize) % bound.max(1)
        }
    }

    /// One type per column name. A merge over any mixture of these schemas then
    /// succeeds, and each test below reads the result, not an error.
    fn column_types() -> Vec<(&'static str, DataType)> {
        vec![
            ("TEMP", DataType::Float64),
            ("PSAL", DataType::Float32),
            ("DEPTH", DataType::Int32),
            ("TIME", DataType::Int64),
            ("PLATFORM", DataType::Utf8),
        ]
    }

    /// A pool of schemas over one set of column names. The merges then get fields
    /// to union and fields to skip.
    fn schema_pool(rng: &mut Rng) -> Vec<SchemaRef> {
        let columns = column_types();
        (0..6)
            .map(|_| {
                let width = 1 + rng.next(columns.len());
                let fields: Vec<Field> = (0..width)
                    .map(|_| {
                        let (name, kind) = &columns[rng.next(columns.len())];
                        Field::new(*name, kind.clone(), true)
                    })
                    // A schema cannot hold one name twice.
                    .fold(Vec::new(), |mut kept: Vec<Field>, field| {
                        if !kept.iter().any(|k| k.name() == field.name()) {
                            kept.push(field);
                        }
                        kept
                    });
                Arc::new(Schema::new(fields))
            })
            .collect()
    }

    /// Over random listings, the merge of this module equals the earlier merge of
    /// the format.
    ///
    /// The cache may change the source of a schema. It may not change the report
    /// of the table. This test guards against a later change that reorders or
    /// thins the sequence.
    #[test]
    fn the_cached_merge_matches_a_flat_merge_over_random_listings() {
        let mut rng = Rng(0x5EED);
        for case in 0..500 {
            let pool = schema_pool(&mut rng);
            let length = 1 + rng.next(40);
            let listing: Vec<SchemaRef> = (0..length)
                .map(|_| Arc::clone(&pool[rng.next(pool.len())]))
                .collect();

            assert_eq!(
                merge_in_listing_order(&widening(), &listing).unwrap(),
                flat_merge(&listing),
                "case {case}: the merge changed the answer for {listing:?}"
            );
        }
    }

    /// A part merge, combined in listing order, equals one flat merge.
    ///
    /// This property makes a cached schema valid. One entry holds the schema of
    /// one file, and the plan merges those part answers. The property holds only
    /// while the merge is associative.
    #[test]
    fn a_partial_merge_combined_in_order_matches_a_flat_merge() {
        let mut rng = Rng(0xC0FFEE);
        for case in 0..500 {
            let pool = schema_pool(&mut rng);
            let length = 2 + rng.next(20);
            let listing: Vec<SchemaRef> = (0..length)
                .map(|_| Arc::clone(&pool[rng.next(pool.len())]))
                .collect();

            let split = 1 + rng.next(listing.len() - 1);
            let combined: Vec<SchemaRef> = std::iter::once(flat_merge(&listing[..split]))
                .chain(listing[split..].iter().cloned())
                .collect();

            assert_eq!(
                flat_merge(&combined),
                flat_merge(&listing),
                "case {case}: splitting at {split} changed the answer"
            );
        }
    }

    /// The merge answer does not depend on the schema order or the group
    /// boundaries.
    ///
    /// Everything above rests on this property. It lets a cached part merge stand
    /// for the files it covers. It lets the merge drop a schema it has seen. It
    /// lets the merge split the work across threads. The property was false
    /// before. Widening was a table of pairs that gave
    /// `(Int32, Float32) -> Float32` and `(Float32, Int32) -> Float64`, so one
    /// listing could merge two ways. See issue #377.
    #[test]
    fn the_merge_does_not_depend_on_the_order_of_its_schemas() {
        let temp = schema(&[("TEMP", DataType::Int32)]);
        let psal = schema(&[("PSAL", DataType::Float32)]);
        let both = schema(&[("TEMP", DataType::Int32), ("PSAL", DataType::Float32)]);

        // Every order, every group boundary and every repeat count gives the same
        // fields with the same types.
        for listing in [
            vec![Arc::clone(&temp), Arc::clone(&psal)],
            vec![Arc::clone(&psal), Arc::clone(&temp)],
            vec![Arc::clone(&both), Arc::clone(&temp), Arc::clone(&psal)],
            vec![flat_merge(&[Arc::clone(&temp)]), Arc::clone(&psal)],
        ] {
            let merged = merge_in_listing_order(&widening(), &listing).unwrap();
            assert_eq!(merged.fields().len(), 2);
            assert_eq!(
                merged.field_with_name("TEMP").unwrap().data_type(),
                &DataType::Int32
            );
            assert_eq!(
                merged.field_with_name("PSAL").unwrap().data_type(),
                &DataType::Float32
            );
        }

        // The merge refuses a conflict from either end of the listing.
        let conflicting = schema(&[("TEMP", DataType::Float32)]);
        assert!(merge_in_listing_order(&widening(), &[temp.clone(), conflicting.clone()]).is_err());
        assert!(merge_in_listing_order(&widening(), &[conflicting, temp]).is_err());
    }

    /// A stamp covers the source and every dependent, so a Zarr chunk that
    /// changed retires the store's entry.
    #[test]
    fn a_unit_stamps_everything_it_depends_on() {
        let object = |size: u64| ObjectMeta {
            location: object_store::path::Path::from("a/zarr.json"),
            last_modified: chrono::DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
            size,
            e_tag: None,
            version: None,
        };
        let objects = vec![object(64), object(4096)];
        let store = SchemaUnit {
            source: 0,
            dependents: vec![1],
        };
        let marker_only = SchemaUnit::from_file(0);

        assert_ne!(
            stamp_unit(&objects, &store),
            stamp_unit(&objects, &marker_only)
        );

        // The chunk grows; the marker does not. The store's stamp still moves.
        let grown = vec![object(64), object(8192)];
        assert_ne!(stamp_unit(&objects, &store), stamp_unit(&grown, &store));
        assert_eq!(
            stamp_unit(&objects, &marker_only),
            stamp_unit(&grown, &marker_only),
            "a file's own stamp does not see its neighbours"
        );
    }
}
