//! Inferring a table's schema, from the cache where it can.
//!
//! # What this replaces
//!
//! `ListingOptions::infer_schema` lists a URL and hands the whole listing to the
//! format, which opens every object. Nothing kept the result, so the second
//! query over a hundred thousand netCDF files opened the same hundred thousand
//! files as the first. Measured, that is 9.2s of a 10.4s query.
//!
//! This does the same listing, then asks the schema cache about each object
//! first, and opens only the ones it has no answer for. A fully analysed
//! collection costs one lookup per file and one merge. A collection that gained
//! a thousand files costs a thousand opens, not a hundred thousand.
//!
//! # Why a cached schema may stand in for an inferred one
//!
//! Every Beacon format infers one schema per object and folds them with
//! [`super_type_schema`], which unions fields into an `IndexMap` and widens a
//! type two files disagree on. That is a left fold, and the schema it produces
//! captures the whole fold state: the field names in insertion order, each with
//! its type so far. So a partial merge, folded back in at the position it came
//! from, gives what one flat merge gives. That is what licenses answering part
//! of a listing from the cache and inferring the rest.
//!
//! # Why the listing order is kept exactly
//!
//! Widening is **not** commutative, and so not associative. `super_type_arrow`
//! is a hand-written table, and it holds `(Int32, Float32) -> Float32` beside
//! `(Float32, Int32) -> Float64`. A merge that reordered the listing, or that
//! dropped a repeated schema, could therefore land on a different type — a
//! property test caught exactly that.
//!
//! So this folds every unit's schema, in listing order, however often a schema
//! repeats. It is the same fold the format did before, over the same sequence,
//! and it costs the same. What the cache removes is the file opens, which is
//! where the 9.2s was.
//!
//! Field order is user-visible through `SELECT *`, and it follows the same
//! sequence, so it does not move either.
//!
//! # Fail open
//!
//! No cache, no format fingerprint, an unreadable table, a stale entry: each one
//! falls back to inferring. This may only ever make a query faster. It may never
//! change its answer.

use std::sync::Arc;

use beacon_common::super_typing::super_type_schema;
use beacon_file_stats::{FileKey, Lookup, SchemaCache, Stamp, try_file_stats_from_session};
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
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

/// One schema per URL, cached where the cache can answer.
///
/// The caller merges them. `FastObjectTable` applies the session's widening
/// rule, and an external table has one URL and takes it as it is — so the split
/// stays here rather than being folded into one schema, and neither caller's
/// merge changes.
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

/// What `ListingOptions::infer_schema` does, per URL. The path every format
/// that has not opted in still takes, and the one every failure falls back to.
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

/// The cache, and the format identity its entries are keyed under.
struct CachedInference {
    cache: Arc<SchemaCache>,
    factory: Arc<dyn FileFormatFactoryExt>,
    /// The format's options fingerprint. Two option sets that read a file
    /// differently never share a key.
    fingerprint: u64,
}

impl CachedInference {
    /// `None` when this session or this format cannot use the cache: no
    /// file-statistics store, no factory behind the format, or a format that
    /// has not opted in. Each is a reason to infer, not a reason to fail.
    fn for_session(state: &dyn Session, format: &Arc<dyn FileFormat>) -> Option<Self> {
        let store = try_file_stats_from_session(state)?;
        // The factory answers to the format's own extension, which is how it was
        // registered. It is the only route back to the `Ext` trait: DataFusion
        // hands out `Arc<dyn FileFormatFactory>` and there is no upcast.
        let factory = try_file_format_factory_ext(state, &format.get_ext())?;
        let fingerprint = factory.schema_options_fingerprint(format.as_ref())?;
        Some(Self {
            cache: Arc::clone(store.schema_cache()),
            factory,
            fingerprint,
        })
    }

    /// One URL's schema: list it, answer what the cache can, infer the rest.
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
            // Nothing to key on. Let the format say what that means: netCDF and
            // Atlas answer with an empty schema, Zarr with an error.
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
        merge_in_listing_order(&resolved)
    }

    /// What to ask the cache, one entry per unit, in listing order.
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

    /// The schemas of the units the cache had no answer for.
    ///
    /// Driven at the session's `meta_fetch_concurrency`, which is the width the
    /// formats use for this themselves. A serial loop here would be slower than
    /// no cache at all on a cold collection.
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

/// Everything at `url`, the way `ListingOptions::infer_schema` gathers it.
///
/// Empty objects are dropped: they cannot affect a schema, and a format asked to
/// read one may well throw.
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

/// What one cached entry describes: its source object, then everything else it
/// depends on, in listing order.
///
/// A plain file depends on itself alone. A Zarr or Atlas store depends on its
/// whole directory, so an array added under an unchanged marker still retires
/// the entry.
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

/// Fold every unit's schema, in listing order.
///
/// The same sequence the format folded before this cache existed, so the answer
/// is the same answer. Repeats are folded again rather than skipped: widening is
/// not commutative, so a repeat that arrives after a wider type can still move
/// the result, and dropping it does not. See the [module docs](self).
fn merge_in_listing_order(schemas: &[SchemaRef]) -> Result<SchemaRef, DataFusionError> {
    super_type_schema(schemas)
        .map(Arc::new)
        .map_err(|e| exec_datafusion_err!("Failed to merge the schemas of a table: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field};

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

    /// The merge is the fold the format did before, over the same sequence.
    #[test]
    fn the_merge_unions_and_widens_over_the_listing() {
        let a = schema(&[("TEMP", DataType::Float64), ("DEPTH", DataType::Int32)]);
        let b = schema(&[("PSAL", DataType::Float64)]);
        let c = schema(&[("DEPTH", DataType::Int64)]);

        // A collection that repeats itself, the way a real one does.
        let listing = vec![
            Arc::clone(&a),
            Arc::clone(&a),
            Arc::clone(&b),
            Arc::clone(&a),
            Arc::clone(&c),
            Arc::clone(&b),
        ];

        let merged = merge_in_listing_order(&listing).unwrap();
        assert_eq!(merged.as_ref(), &super_type_schema(&listing).unwrap());
        assert_eq!(names(&merged), vec!["TEMP", "DEPTH", "PSAL"]);
        // And the widening still happened: Int32 beside Int64 is Int64.
        assert_eq!(
            merged.field_with_name("DEPTH").unwrap().data_type(),
            &DataType::Int64
        );
    }

    /// A repeated schema is folded again, not skipped.
    ///
    /// Skipping looks safe and is not: widening is not commutative, so a type
    /// that arrives again *after* a wider one can still move the answer. This
    /// pins the case a property test found.
    #[test]
    fn a_repeat_after_a_wider_type_still_counts() {
        let narrow = schema(&[("TEMP", DataType::Int32)]);
        let wide = schema(&[("TEMP", DataType::Float32)]);

        // Int32, then Float32, then Int32 again. The last one matters: the table
        // holds (Int32, Float32) -> Float32 but (Float32, Int32) -> Float64.
        let merged =
            merge_in_listing_order(&[Arc::clone(&narrow), Arc::clone(&wide), Arc::clone(&narrow)])
                .unwrap();
        assert_eq!(
            merged.field_with_name("TEMP").unwrap().data_type(),
            &DataType::Float64,
            "dropping the repeat would have stopped at Float32"
        );
    }

    /// Field order follows the listing, because `SELECT *` shows it. Reversing
    /// the listing must reverse the columns, exactly as a flat merge does.
    #[test]
    fn field_order_follows_the_listing() {
        let a = schema(&[("A", DataType::Int32)]);
        let b = schema(&[("B", DataType::Int32)]);

        let forward = merge_in_listing_order(&[Arc::clone(&a), Arc::clone(&b)]).unwrap();
        let backward = merge_in_listing_order(&[b, a]).unwrap();
        assert_eq!(names(&forward), vec!["A", "B"]);
        assert_eq!(names(&backward), vec!["B", "A"]);
    }

    /// A type pair with no common representation is an error, not a guess. The
    /// deduped merge must refuse exactly what a flat merge refuses.
    #[test]
    fn an_irreconcilable_pair_is_still_an_error() {
        let dates = schema(&[("WHEN", DataType::Date32)]);
        let counts = schema(&[("WHEN", DataType::Int32)]);
        assert!(merge_in_listing_order(&[dates.clone(), counts.clone()]).is_err());
        assert!(super_type_schema(&[dates, counts]).is_err());
    }

    // ── the properties the design rests on ─────────────────────────────
    //
    // Randomised rather than enumerated, because the interesting cases are
    // combinations. Deterministic all the same: the generator is a fixed
    // sequence, so a failure here reproduces exactly.

    /// A tiny linear congruential generator. Nothing here needs randomness in
    /// the cryptographic sense, and a fixed sequence is what makes a failing
    /// case reproducible without a seed to hunt for.
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

    /// Types that always have a common super type, so a merge over any mixture
    /// of them succeeds and the property under test is the *result*, not the
    /// error.
    fn lattice() -> Vec<DataType> {
        vec![
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
        ]
    }

    /// A pool of schemas over a shared column namespace, so the merges have
    /// something to widen and something to union.
    fn schema_pool(rng: &mut Rng) -> Vec<SchemaRef> {
        let columns = ["TEMP", "PSAL", "DEPTH", "TIME", "PLATFORM"];
        let types = lattice();
        (0..6)
            .map(|_| {
                let width = 1 + rng.next(columns.len());
                let fields: Vec<Field> = (0..width)
                    .map(|_| {
                        Field::new(
                            columns[rng.next(columns.len())],
                            types[rng.next(types.len())].clone(),
                            true,
                        )
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

    /// Over random listings: the merge this module applies is the merge the
    /// format applied before it, exactly.
    ///
    /// The cache is allowed to change where a schema *came from*, never what the
    /// table reports. This is the guard against a future optimization quietly
    /// reordering or thinning the sequence — dropping repeated schemas looked
    /// safe, and this test is what caught it.
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
                merge_in_listing_order(&listing).unwrap().as_ref(),
                &super_type_schema(&listing).unwrap(),
                "case {case}: the merge changed the answer for {listing:?}"
            );
        }
    }

    /// A partial merge, combined in listing order, is the same answer as one
    /// flat merge.
    ///
    /// This is what makes a cached schema legitimate at all: an entry holds one
    /// file's schema, and the plan folds those partial answers together. It is
    /// only sound while folding is associative in listing order.
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
            let head: SchemaRef = Arc::new(super_type_schema(&listing[..split]).unwrap());
            let combined: Vec<SchemaRef> = std::iter::once(head)
                .chain(listing[split..].iter().cloned())
                .collect();

            assert_eq!(
                super_type_schema(&combined).unwrap(),
                super_type_schema(&listing).unwrap(),
                "case {case}: splitting at {split} changed the answer"
            );
        }
    }

    /// Widening is **not** commutative, and so not associative.
    ///
    /// `super_type_arrow` is a hand-written table, and its two halves disagree:
    /// `(Int32, Float32)` gives `Float32` while `(Float32, Int32)` gives
    /// `Float64`. This pins that, because everything above depends on it: it is
    /// the reason the merge keeps the listing order exactly, and the reason a
    /// repeated schema is folded again rather than skipped.
    ///
    /// Change this and the merge above has to be re-examined, not just retuned.
    #[test]
    fn widening_depends_on_the_order_of_its_operands() {
        use beacon_common::super_typing::super_type_arrow;

        assert_eq!(
            super_type_arrow(&DataType::Int32, &DataType::Float32),
            Some(DataType::Float32)
        );
        assert_eq!(
            super_type_arrow(&DataType::Float32, &DataType::Int32),
            Some(DataType::Float64),
            "the two halves of the table disagree"
        );

        // Which makes grouping visible: the same three types widen two ways.
        let left_first = super_type_arrow(&DataType::Float32, &DataType::Int32)
            .and_then(|pair| super_type_arrow(&pair, &DataType::Float32));
        let right_first = super_type_arrow(&DataType::Int32, &DataType::Float32)
            .and_then(|pair| super_type_arrow(&DataType::Float32, &pair));
        assert_eq!(left_first, Some(DataType::Float64));
        assert_eq!(right_first, Some(DataType::Float32));
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
