# Beacon: caching inferred schemas

Stop re-deriving a table's schema from every file on every query. Intern each
file's schema once, memoize the merged result per listing, and let the
statistics collector fill both as a side effect of the pass it already runs.

Status: design / plan. Tracked as issue #367. Prerequisite work landed on
`features/custom-listing-table`.

---

## 1. The problem, measured

`FastObjectTable::try_new` infers a schema by opening every file behind the
table. Nothing caches the result, so every query pays it again. At 100 000 files
this is the whole cost of the query.

From `beacon-core/tests/nd_format_scale.rs` and
`beacon-datafusion-ext/tests/fast_object_scale.rs`, 12-core M-series, release:

| 100 000 files | netCDF | HDF5 | Parquet |
|---|---|---|---|
| schema inference | 9.16s | 9.77s | 3.47s |
| per file | ~92 µs | ~98 µs | ~35 µs |
| prune 100 000 files | 0.04s | ~0s | 0.12s |
| read 25 000 files | 1.52s | 2.01s | 0.79s |
| **re-plan, same query** | **9.26s / 9.27s** | **9.61s / 9.67s** | 0.54s |

The re-plan rows are the finding. The second and third identical query cost what
the first cost. The same 100 000 files are opened and their schemas re-derived,
forever.

Inference is **83–87%** of a netCDF or HDF5 query that reads a quarter of the
store. Pruning is 1.2 µs per file, three orders of magnitude cheaper. Nothing
else in the profile is worth touching first.

Parquet is cheaper per file (a footer read, not an HDF5 metadata walk) and its
listing table is built once for an external table, so it shows the cost as a
one-off 3.47s rather than per query. The structural problem is the same.

**Target: take the plan from ~9.2s to the cost of listing plus one lookup,
roughly 0.5s.** A netCDF query that reads a quarter of the store goes from 10.4s
to about 2s.

## 2. The fact that makes this cheap

`beacon-core/src/file_stats.rs:174`, inside the collector's per-file analysis:

```rust
let schema = format
    .infer_schema(&state, &store, std::slice::from_ref(&object))
    .await?;

let statistics = format
    .infer_stats(&state, &store, schema.clone(), &object)
    .await?;
```

The collector **already computes each file's schema**, uses it to position the
statistics, and discards it. The open is already paid for. Persisting the schema
alongside the statistics costs one encode and one write per file, and no I/O
that is not already happening.

So the cache is not a new subsystem. It is a second output of a pass that
already runs.

## 3. The property the design rests on

Both merge strategies fold field-by-field over a sequence of schemas:

- `DefaultArrowTypeWidening` unions fields by name and errors on a type conflict.
- `SuperTypeWidening` (`beacon_common::super_typing::super_type_schema`) unions
  by name into an `IndexMap` and widens a conflict through `super_type_arrow`.

That gives two different properties, and conflating them is the main way this
design could go wrong:

- **Content is order-independent.** Which fields appear, and what type each ends
  up with, does not depend on the order the schemas are merged in (assuming
  `super_type_arrow` is associative, see §8).
- **Field order is order-dependent.** `IndexMap` preserves first-seen order, so
  `merge(A, B)` and `merge(B, A)` can differ in column order. `SELECT *` shows
  that order, so it is user-visible and must not drift.

Therefore a cached partial merge is sound **only if partial results are combined
in the same order a flat merge would have visited them**. Concretely: merge files
within a prefix in path order, and prefixes in path order. That reproduces
merging the sorted listing exactly.

This is what makes a directory-level merged schema legitimate rather than a
gamble, and it is a constraint a property test must hold us to.

## 4. Design: two levels

### Level 1 — intern each file's schema

```
fs_schemas:     schema_hash (16B blake3)      -> Arrow IPC schema bytes
fs_file_schema: (file_id, options_hash)       -> schema_hash
```

A collection of a million files typically has a handful of distinct schemas, so
`fs_schemas` holds a handful of blobs however large the collection. Content
addressing gives that deduplication for free.

Level 1 makes re-inference proportional to **changed** files rather than all
files. A directory that gains one file re-infers one file.

### Level 2 — memoize the merged result

```
fs_merged: merge_key -> (listing_fingerprint, schema_hash)
```

One lookup answers a repeat query. `merge_key` identifies *what* is being merged;
`listing_fingerprint` decides whether the answer is still valid (§6).

Level 2 is what turns 9.2s into a lookup. Level 1 is what makes Level 2 cheap to
rebuild when it misses, and what stops a single new file from costing 9.2s.

### Why not per-directory merged schemas as the primary unit

The original suggestion was to store a merged schema per directory. §3 shows
that is sound if the combine order is fixed. But it is strictly weaker than the
pair above:

- A directory-level merge still invalidates wholly when one file changes. With
  Level 1 underneath, that costs one file's inference, not the directory's.
- The steady-state win (a repeat query) needs a whole-table answer, which is
  Level 2, not a per-directory one.

Per-directory merges are worth adding later as a middle tier if collections turn
out to have many directories that change independently. They are not needed to
get the win, so they are out of Phase 1.

## 5. Where it lives, and why not in the obvious places

**Not in segments.** Statistics live in object-store segments because they are
large: a row per (file, column), 10 segments for 100 000 files. Schemas dedup to
a handful of kilobytes total. Putting them in segments would buy the cost of a
manifest and a range read to fetch something that fits in a redb page. redb is
the right home.

**Not as a field on `FileRecord`.** `FileRecord` is `bincode::serialize`d with no
version marker (`registry.rs:593`). Adding a field makes every existing
`beacon.db` fail to decode. This was recorded as the blocker for schema interning
when the work started.

**New redb tables dissolve that blocker.** redb creates a table on first open, so
an existing `beacon.db` simply has empty new tables and re-populates them on the
next collector pass. No migration, no version key, no decode path. `FileRecord`
is not touched.

**Arrow IPC for the schema bytes**, not bincode or serde. It is Arrow's own
versioned encoding, forward-compatible across arrow-rs releases, and it already
round-trips every type Beacon can produce.

## 6. Validity

Two independent mechanisms, because they fail differently.

### Level 1: the file's identity

`FileRecord` already carries `size`, `last_modified_millis` and `e_tag`, and
`intern_files` already marks a file `Stale` when they change. A schema keyed by
`file_id` is valid exactly while the record is not `Stale`. This is the same
mechanism pruning already trusts, and it needs nothing new.

### Level 2: a fingerprint of the listing

This is the hash from the original suggestion, made precise. Over the listing,
in path order:

```
fingerprint = blake3(for each object: path, size, last_modified_millis, e_tag)
```

Include `size` and `e_tag`, not just the date. A file rewritten within the
filesystem's timestamp granularity changes size or etag far more often than it
changes neither, and on S3 the etag is the only reliable signal.

**The listing is not saved by this, and does not need to be.** The plan must list
regardless, to know which files to scan. Listing 100 000 objects costs ~0.5s;
hashing what was listed is microseconds. The fingerprint converts a 9.2s
inference into a hash of data already in hand.

### Fail-open, always

Any doubt infers. A missing entry, a `Stale` record, a decode failure, a
fingerprint mismatch, an unreadable table: all fall back to inferring, exactly as
pruning falls back to keeping a file it knows nothing about. The cache may only
ever make a query faster, never change its answer.

## 7. The keys

A schema is not a function of the file alone.

| Key component | Why |
|---|---|
| `file_id` | which file |
| `options_hash` | netCDF's `read_dimensions` changes which variables appear, so the same file has different schemas under different options. Hash the format's options struct. |
| widening strategy id | `read_*` uses the session's `ArrowTypeWidening` (default: strict union); the JSON query API uses `SuperTypeWidening`. They can produce different merged schemas from the same files, so this belongs in `merge_key`, not in the per-file key. |
| table URLs | the set of `ListingTableUrl`s, in order, including globs |

`merge_key = blake3(urls, options_hash, widening_id, format_name)`.

Getting `options_hash` wrong is the one way this returns a wrong schema rather
than a slow one, so the options type should gain a derived `Hash` and a test
that two different option sets produce different keys.

## 8. Risks

| Risk | Handling |
|---|---|
| Field order drifts when partial merges are combined | §3's fixed combine order; a property test asserting cached-merge == flat-merge over random schema sets and orders |
| `super_type_arrow` is not associative for some pair | Property test over the type lattice. If it fails for a pair, that pair is already producing order-dependent results today, so the test is worth having regardless of this feature |
| A format's schema depends on something not in the key | Fail-open covers correctness only if the key is complete. Start with formats whose inference is a pure function of (bytes, options): Parquet, netCDF, HDF5. Audit before extending |
| Cache grows unbounded | `fs_schemas` is content-addressed and tiny. `fs_file_schema` is one 16-byte value per file, ~1.6 MB per million files. `fs_merged` is one row per distinct query shape; evict by count |
| The collector has not run yet | Level 2 misses, Level 1 misses, everything infers. Identical to today's behaviour |

## 9. Phases

**Phase 1 — Level 1, populated by the collector.**
New redb tables, IPC encoding, `Registry::intern_schema` / `schema_for`. The
collector writes the schema it already computes. `FastObjectTable::try_new`
consults the cache per file and infers only the misses. No behaviour change when
the subsystem is off.

Expected: a fully analysed 100 000-file netCDF table goes from 9.2s to the cost
of 100 000 redb point lookups plus one merge, well under a second. A table with
1 000 new files pays 1 000 inferences, not 100 000.

**Phase 2 — Level 2, the merged memo.**
Fingerprint the listing, memoize `merge_key -> schema`. Turns the repeat query
into one lookup and removes the per-file lookups of Phase 1 from the hot path.

**Phase 3 — observability.**
`beacon.system.schema_cache`: rows, distinct schemas, hit rate, bytes. Without
it, a cache that silently stops hitting looks exactly like one that works.

**Phase 4 (conditional) — per-prefix merges.**
Only if measurement shows collections with many independently-changing
directories where Phase 2 thrashes. Deliberately not in the first pass.

## 10. What this does not fix

Listing still costs ~0.5s per query at 100 000 files and scales linearly, because
`list_files_for_scan` materialises a `PartitionedFile` per object before pruning
sees it (~331 B/file, 0.92 GiB at 3M files). That is the other half of issue #361
and is untouched here. After this work it becomes the dominant per-query cost,
which is the right time to look at it.

Opening files during the scan (131 µs/file netCDF, 155 µs/file HDF5) is also
untouched. That is real work against files the query actually reads, and pruning
is what reduces it.

## 11. Open decisions

1. **Does the object store guarantee path-ordered listings?** §3's combine order
   assumes it. Local and S3 both list lexicographically, but this should be
   asserted rather than assumed, and sorted explicitly if it is not free.
2. **Where does `options_hash` come from?** Each format's options type needs a
   stable hash. Derived `Hash` is fine if no field is a float or a map with
   non-deterministic iteration order. Needs an audit per format.
3. **Should the collector record a schema for a file it fails to analyse?**
   Inference can succeed where statistics generation fails. Recording the schema
   anyway is more useful, but means the two outputs can disagree about which
   files are known.
4. **Eviction policy for `fs_merged`.** One row per distinct query shape is
   small, but a workload with many generated globs could grow it. Count-bounded
   LRU is probably enough; confirm against real query logs.
