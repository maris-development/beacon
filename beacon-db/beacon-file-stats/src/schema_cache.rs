//! Each file's inferred schema, kept so a query never derives it twice.
//!
//! # The problem
//!
//! A table's schema is derived from every file behind it, on every query.
//! Nothing kept the result, so the second query opened the same 100 000 files
//! and read the same metadata as the first. At that size it is 83% to 87% of a
//! netCDF or HDF5 query, and pruning — the other half of the plan — costs three
//! orders of magnitude less per file.
//!
//! # Why it is nearly free to fix
//!
//! The statistics collector already infers each file's schema, uses it to
//! position the statistics, and drops it. The open is already paid for. Keeping
//! the schema costs one encode and one write per file, and no I/O that was not
//! happening anyway. So this is a second output of a pass that already runs, not
//! a new subsystem.
//!
//! # Two tables
//!
//! ```text
//! fs_schemas:     schema_hash                 -> Arrow IPC schema bytes
//! fs_file_schema: path_hash + options_hash     -> stamp + schema_hash
//! ```
//!
//! [`fs_schemas`](SCHEMAS) is content addressed, so a million files that share
//! one schema store one blob. A collection typically has a handful of distinct
//! schemas however large it is, so this table stays kilobytes.
//!
//! Both tables are new. redb creates a table on first open, so an existing
//! `beacon.db` starts with empty ones and fills them on the next collector pass.
//! [`FileRecord`](crate::FileRecord) is not touched, which matters: it is
//! bincode-encoded with no version marker, so a new field on it would break
//! every database already out there.
//!
//! # Validity
//!
//! The value carries a [`Stamp`] over the objects the entry covers — their size,
//! last-modified time and etag. One lookup therefore answers both "which schema"
//! and "is this still the same file", with no second read and no dependence on
//! [`FileState`](crate::FileState).
//!
//! Size and etag are in the stamp, not just the date. A file rewritten inside
//! the filesystem's timestamp granularity changes one of those far more often
//! than it changes neither, and on S3 the etag is the only reliable signal.
//!
//! # Fail open, always
//!
//! A missing entry, a stamp mismatch, a decode failure, an unreadable table: all
//! read as "not cached", and the caller infers exactly as it did before. This
//! cache may only ever make a query faster. It may never change its answer.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::{Schema, SchemaRef};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::error::Result;

/// Content-addressed schema blobs. See the [module docs](self).
const SCHEMAS: TableDefinition<'static, &'static [u8], &'static [u8]> =
    TableDefinition::new("fs_schemas");
/// One row per (file, format options). See the [module docs](self).
const FILE_SCHEMA: TableDefinition<'static, &'static [u8], &'static [u8]> =
    TableDefinition::new("fs_file_schema");

/// A 128-bit blake3 digest.
///
/// Truncated from 256 bits because these are cache keys, not signatures: 128
/// bits puts a collision beyond reach for any collection that could exist, and
/// halves what every row costs.
pub type Digest = [u8; 16];

/// What a cached entry describes, so a changed file reads as a miss.
///
/// Built from the objects an entry covers — one file for most formats, a marker
/// and its store for Zarr and Atlas.
pub type Stamp = Digest;

/// Which file, under which format options.
///
/// `path` is hashed with the object store's URL, so two stores holding the same
/// relative path never collide. `options` distinguishes the same file read two
/// ways: netCDF's `read_dimensions` changes which variables appear, so the same
/// bytes have more than one schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileKey {
    pub path: Digest,
    pub options: u64,
}

impl FileKey {
    /// Hash `path` under `store_url` into a key, with the format's fingerprint.
    pub fn new(store_url: &str, path: &str, options: u64) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(store_url.as_bytes());
        hasher.update(&[0]); // a separator, so ("ab", "c") and ("a", "bc") differ
        hasher.update(path.as_bytes());
        Self {
            path: truncate(hasher.finalize().as_bytes()),
            options,
        }
    }

    /// The redb key: the path digest then the options, big-endian.
    fn encode(&self) -> [u8; 24] {
        let mut key = [0u8; 24];
        key[..16].copy_from_slice(&self.path);
        key[16..].copy_from_slice(&self.options.to_be_bytes());
        key
    }
}

/// A stamp over one object's identity.
pub fn stamp_object(size: u64, last_modified_millis: i64, e_tag: Option<&str>) -> Stamp {
    stamp_objects([(size, last_modified_millis, e_tag)])
}

/// A stamp over every object an entry covers, in the caller's order.
///
/// Zarr and Atlas derive one schema from a whole store, so their entry covers
/// the marker and everything the listing reported under it. A chunk that changes
/// then invalidates the entry, which a stamp over the marker alone would miss.
pub fn stamp_objects<'a>(objects: impl IntoIterator<Item = (u64, i64, Option<&'a str>)>) -> Stamp {
    let mut hasher = blake3::Hasher::new();
    for (size, last_modified_millis, e_tag) in objects {
        hasher.update(&size.to_be_bytes());
        hasher.update(&last_modified_millis.to_be_bytes());
        match e_tag {
            Some(tag) => {
                hasher.update(&[1]);
                hasher.update(tag.as_bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        }
        hasher.update(&[0xff]); // a separator between objects
    }
    truncate(hasher.finalize().as_bytes())
}

/// What one lookup asks about.
#[derive(Debug, Clone, Copy)]
pub struct Lookup {
    pub key: FileKey,
    /// The stamp the caller just computed from the listing. An entry whose own
    /// stamp differs describes content that is gone.
    pub stamp: Stamp,
}

/// What the cache has done since it was opened.
///
/// Read by tests and by anyone asking why a node still infers. A cache that
/// silently stops hitting looks exactly like one that works.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counters {
    /// Lookups answered from the cache.
    pub hits: u64,
    /// Lookups the caller had to infer: absent, stale, or undecodable.
    pub misses: u64,
    /// Blobs decoded from IPC. Far below `hits`, because a collection has a
    /// handful of distinct schemas however many files it holds.
    pub decoded: u64,
    /// Entries written.
    pub written: u64,
}

/// Interned per-file schemas, backed by redb.
#[derive(Debug)]
pub struct SchemaCache {
    db: Arc<Database>,
    hits: AtomicU64,
    misses: AtomicU64,
    decoded: AtomicU64,
    written: AtomicU64,
}

impl SchemaCache {
    /// Open (or create) the cache tables on an existing database.
    ///
    /// Both tables are created up front, so a read transaction never meets a
    /// missing one and has to special-case it.
    pub fn from_database(db: Arc<Database>) -> Result<Self> {
        let write = db.begin_write()?;
        {
            write.open_table(SCHEMAS)?;
            write.open_table(FILE_SCHEMA)?;
        }
        write.commit()?;
        Ok(Self {
            db,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            decoded: AtomicU64::new(0),
            written: AtomicU64::new(0),
        })
    }

    /// The cached schema of each lookup, in the caller's order.
    ///
    /// One read transaction and two open tables for the whole batch. Doing this
    /// a file at a time costs a transaction per file, which at a hundred
    /// thousand files is the overhead this cache exists to remove.
    ///
    /// Each distinct blob is decoded once and handed out as clones, so a
    /// collection with three schemas pays three decodes however many files ask.
    ///
    /// A failure anywhere answers `None` for every lookup rather than an error:
    /// the caller's only response to either is to infer.
    pub fn file_schemas(&self, lookups: &[Lookup]) -> Vec<Option<SchemaRef>> {
        match self.try_file_schemas(lookups) {
            Ok(found) => found,
            Err(error) => {
                tracing::debug!(%error, "the schema cache is unreadable; inferring instead");
                self.misses
                    .fetch_add(lookups.len() as u64, Ordering::Relaxed);
                vec![None; lookups.len()]
            }
        }
    }

    fn try_file_schemas(&self, lookups: &[Lookup]) -> Result<Vec<Option<SchemaRef>>> {
        let read = self.db.begin_read()?;
        let entries = read.open_table(FILE_SCHEMA)?;
        let blobs = read.open_table(SCHEMAS)?;

        let mut decoded: HashMap<Digest, Option<SchemaRef>> = HashMap::new();
        let mut out = Vec::with_capacity(lookups.len());
        let (mut hits, mut misses) = (0u64, 0u64);

        for lookup in lookups {
            let schema = match entries.get(lookup.key.encode().as_slice())? {
                Some(value) => match read_entry(value.value()) {
                    // A stamp that disagrees describes content that is gone.
                    Some((stamp, _)) if stamp != lookup.stamp => None,
                    Some((_, schema_hash)) => match decoded.entry(schema_hash) {
                        std::collections::hash_map::Entry::Occupied(found) => found.get().clone(),
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            let schema = blobs
                                .get(schema_hash.as_slice())?
                                .and_then(|bytes| decode_schema(bytes.value()));
                            if schema.is_some() {
                                self.decoded.fetch_add(1, Ordering::Relaxed);
                            }
                            slot.insert(schema).clone()
                        }
                    },
                    None => None,
                },
                None => None,
            };
            if schema.is_some() {
                hits += 1;
            } else {
                misses += 1;
            }
            out.push(schema);
        }

        self.hits.fetch_add(hits, Ordering::Relaxed);
        self.misses.fetch_add(misses, Ordering::Relaxed);
        Ok(out)
    }

    /// Record a batch of schemas in one transaction.
    ///
    /// A redb commit is an fsync, so writing per file would cap the collector at
    /// a few hundred files a second however fast the analysis is. At a million
    /// files that is the difference between minutes and hours.
    ///
    /// The blob is written only when its hash is new, so the common case — every
    /// file of a batch sharing one schema — writes one blob and `n` small rows.
    pub fn put_file_schemas(&self, entries: &[(FileKey, Stamp, SchemaRef)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        // Encode outside the transaction. The write lock is exclusive across
        // every tenant of the database, including the collector's own registry
        // writes, so it holds only the redb work.
        let mut encoded = Vec::with_capacity(entries.len());
        for (key, stamp, schema) in entries {
            let bytes = encode_schema(schema);
            encoded.push((*key, *stamp, digest(&bytes), bytes));
        }

        let write = self.db.begin_write()?;
        {
            let mut blobs = write.open_table(SCHEMAS)?;
            let mut rows = write.open_table(FILE_SCHEMA)?;
            for (key, stamp, schema_hash, bytes) in &encoded {
                if blobs.get(schema_hash.as_slice())?.is_none() {
                    blobs.insert(schema_hash.as_slice(), bytes.as_slice())?;
                }
                let mut value = [0u8; 32];
                value[..16].copy_from_slice(stamp);
                value[16..].copy_from_slice(schema_hash);
                rows.insert(key.encode().as_slice(), value.as_slice())?;
            }
        }
        write.commit()?;
        self.written
            .fetch_add(entries.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    pub fn counters(&self) -> Counters {
        Counters {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            decoded: self.decoded.load(Ordering::Relaxed),
            written: self.written.load(Ordering::Relaxed),
        }
    }

    /// Distinct schemas held. A diagnostic, not a hot path.
    pub fn num_schemas(&self) -> Result<u64> {
        use redb::ReadableTableMetadata;
        let read = self.db.begin_read()?;
        Ok(read.open_table(SCHEMAS)?.len()?)
    }

    /// Rows held, one per (file, format options). A diagnostic, not a hot path.
    pub fn num_entries(&self) -> Result<u64> {
        use redb::ReadableTableMetadata;
        let read = self.db.begin_read()?;
        Ok(read.open_table(FILE_SCHEMA)?.len()?)
    }
}

/// Split a 32-byte value into its stamp and its schema hash.
///
/// A value of any other width is a row this build does not understand, and reads
/// as absent rather than as an error.
fn read_entry(bytes: &[u8]) -> Option<(Stamp, Digest)> {
    let value: &[u8; 32] = bytes.try_into().ok()?;
    let mut stamp = [0u8; 16];
    let mut schema = [0u8; 16];
    stamp.copy_from_slice(&value[..16]);
    schema.copy_from_slice(&value[16..]);
    Some((stamp, schema))
}

fn truncate(digest: &[u8; 32]) -> Digest {
    let mut short = [0u8; 16];
    short.copy_from_slice(&digest[..16]);
    short
}

fn digest(bytes: &[u8]) -> Digest {
    truncate(blake3::hash(bytes).as_bytes())
}

/// A schema as Arrow IPC flatbuffer bytes.
///
/// Arrow's own encoding, versioned by Arrow and forward-compatible across
/// arrow-rs releases. bincode or serde would tie a stored schema to the layout
/// of one build's types.
fn encode_schema(schema: &Schema) -> Vec<u8> {
    arrow_ipc::convert::IpcSchemaEncoder::new()
        .schema_to_fb(schema)
        .finished_data()
        .to_vec()
}

/// The reverse. `None` for bytes this build cannot read, which reads as a miss.
fn decode_schema(bytes: &[u8]) -> Option<SchemaRef> {
    let root = arrow_ipc::root_as_schema(bytes).ok()?;
    Some(Arc::new(arrow_ipc::convert::fb_to_schema(root)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    fn cache() -> (SchemaCache, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path().join("cache.redb")).unwrap());
        (SchemaCache::from_database(db).unwrap(), dir)
    }

    fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, kind)| Field::new(*name, kind.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    fn key(path: &str) -> FileKey {
        FileKey::new("file:///data/", path, 7)
    }

    fn lookup(path: &str, stamp: Stamp) -> Lookup {
        Lookup {
            key: key(path),
            stamp,
        }
    }

    /// A schema survives the round trip through IPC and redb, types and all.
    #[test]
    fn a_schema_round_trips() {
        let (cache, _dir) = cache();
        let original = schema(&[
            ("TEMP", DataType::Float64),
            ("PLATFORM", DataType::Utf8),
            (
                "TIME",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            ),
        ]);
        let stamp = stamp_object(4096, 1_700_000_000_000, Some("abc"));

        cache
            .put_file_schemas(&[(key("a.nc"), stamp, original.clone())])
            .unwrap();

        let found = cache.file_schemas(&[lookup("a.nc", stamp)]);
        assert_eq!(found[0].as_deref(), Some(original.as_ref()));
        assert_eq!(cache.counters().hits, 1);
    }

    /// Content addressing is the whole reason this stays small: a million files
    /// sharing one schema must store one blob.
    #[test]
    fn files_that_share_a_schema_share_a_blob() {
        let (cache, _dir) = cache();
        let shared = schema(&[("TEMP", DataType::Float64)]);
        let other = schema(&[("PSAL", DataType::Float64)]);
        let stamp = stamp_object(1, 1, None);

        cache
            .put_file_schemas(&[
                (key("a.nc"), stamp, shared.clone()),
                (key("b.nc"), stamp, shared.clone()),
                (key("c.nc"), stamp, other),
            ])
            .unwrap();

        assert_eq!(cache.num_entries().unwrap(), 3);
        assert_eq!(cache.num_schemas().unwrap(), 2, "two distinct schemas");

        // And the read side decodes each distinct blob once, however many files
        // ask for it.
        let found = cache.file_schemas(&[
            lookup("a.nc", stamp),
            lookup("b.nc", stamp),
            lookup("c.nc", stamp),
        ]);
        assert!(found.iter().all(Option::is_some));
        assert_eq!(cache.counters().decoded, 2);
    }

    /// The stamp is the validity check. A file that changed must read as a miss,
    /// because serving its old schema is the one way this returns a wrong answer
    /// rather than a slow one.
    #[test]
    fn a_changed_file_reads_as_a_miss() {
        let (cache, _dir) = cache();
        let before = stamp_object(1024, 1_700_000_000_000, None);
        cache
            .put_file_schemas(&[(key("a.nc"), before, schema(&[("TEMP", DataType::Float64)]))])
            .unwrap();

        // Same path, one byte longer.
        let after = stamp_object(1025, 1_700_000_000_000, None);
        assert!(cache.file_schemas(&[lookup("a.nc", after)])[0].is_none());

        // An etag settles it even when size and date do not.
        let tagged = stamp_object(1024, 1_700_000_000_000, Some("v2"));
        assert!(cache.file_schemas(&[lookup("a.nc", tagged)])[0].is_none());

        // The unchanged stamp still hits.
        assert!(cache.file_schemas(&[lookup("a.nc", before)])[0].is_some());
    }

    /// The same bytes read two ways have two schemas. netCDF's `read_dimensions`
    /// is the case: it changes which variables appear.
    #[test]
    fn the_options_are_part_of_the_key() {
        let (cache, _dir) = cache();
        let stamp = stamp_object(1, 1, None);
        let narrow = schema(&[("TEMP", DataType::Float64)]);
        let wide = schema(&[("TEMP", DataType::Float64), ("PSAL", DataType::Float64)]);

        let with_options = |options| FileKey::new("file:///data/", "a.nc", options);
        cache
            .put_file_schemas(&[
                (with_options(1), stamp, narrow.clone()),
                (with_options(2), stamp, wide.clone()),
            ])
            .unwrap();

        let found = cache.file_schemas(&[
            Lookup {
                key: with_options(1),
                stamp,
            },
            Lookup {
                key: with_options(2),
                stamp,
            },
            // An options set nobody has recorded is a miss, not a wrong answer.
            Lookup {
                key: with_options(3),
                stamp,
            },
        ]);
        assert_eq!(found[0].as_deref(), Some(narrow.as_ref()));
        assert_eq!(found[1].as_deref(), Some(wide.as_ref()));
        assert!(found[2].is_none());
    }

    /// The store URL is in the key, so two stores holding the same relative path
    /// never answer for each other.
    #[test]
    fn the_store_url_separates_identical_paths() {
        let (cache, _dir) = cache();
        let stamp = stamp_object(1, 1, None);
        let local = FileKey::new("file:///data/", "a.nc", 0);
        let remote = FileKey::new("s3://bucket/", "a.nc", 0);
        assert_ne!(local.path, remote.path);

        cache
            .put_file_schemas(&[(local, stamp, schema(&[("TEMP", DataType::Float64)]))])
            .unwrap();
        assert!(cache.file_schemas(&[Lookup { key: remote, stamp }])[0].is_none());
    }

    /// A batch answers in the caller's order, with a hole where an entry is
    /// missing. The caller pairs the answers back to its listing by position.
    #[test]
    fn a_batch_answers_in_order() {
        let (cache, _dir) = cache();
        let stamp = stamp_object(1, 1, None);
        let temp = schema(&[("TEMP", DataType::Float64)]);
        let psal = schema(&[("PSAL", DataType::Float64)]);
        cache
            .put_file_schemas(&[
                (key("a.nc"), stamp, temp.clone()),
                (key("c.nc"), stamp, psal.clone()),
            ])
            .unwrap();

        let found = cache.file_schemas(&[
            lookup("a.nc", stamp),
            lookup("b.nc", stamp),
            lookup("c.nc", stamp),
        ]);
        assert_eq!(found[0].as_deref(), Some(temp.as_ref()));
        assert!(found[1].is_none());
        assert_eq!(found[2].as_deref(), Some(psal.as_ref()));

        let counters = cache.counters();
        assert_eq!((counters.hits, counters.misses), (2, 1));
        assert!(cache.file_schemas(&[]).is_empty());
    }

    /// A re-analysis replaces the row rather than adding one, so a file that
    /// changes repeatedly does not grow the table.
    #[test]
    fn a_re_analysis_replaces_the_row() {
        let (cache, _dir) = cache();
        let before = stamp_object(1, 1, None);
        let after = stamp_object(2, 2, None);
        let widened = schema(&[("TEMP", DataType::Float64), ("PSAL", DataType::Float64)]);

        cache
            .put_file_schemas(&[(key("a.nc"), before, schema(&[("TEMP", DataType::Float64)]))])
            .unwrap();
        cache
            .put_file_schemas(&[(key("a.nc"), after, widened.clone())])
            .unwrap();

        assert_eq!(cache.num_entries().unwrap(), 1);
        assert_eq!(
            cache.file_schemas(&[lookup("a.nc", after)])[0].as_deref(),
            Some(widened.as_ref())
        );
        // The old stamp no longer describes anything.
        assert!(cache.file_schemas(&[lookup("a.nc", before)])[0].is_none());
    }

    /// A stamp covers every object it is given, in order, so a Zarr store whose
    /// chunk changed invalidates even though its marker did not.
    #[test]
    fn a_stamp_covers_every_object_it_is_given() {
        let marker = (64u64, 1_700_000_000_000i64, None);
        let chunk = (4096u64, 1_700_000_000_000i64, None);
        let bigger_chunk = (8192u64, 1_700_000_000_000i64, None);

        assert_eq!(
            stamp_objects([marker, chunk]),
            stamp_objects([marker, chunk])
        );
        assert_ne!(stamp_objects([marker, chunk]), stamp_objects([marker]));
        assert_ne!(
            stamp_objects([marker, chunk]),
            stamp_objects([marker, bigger_chunk])
        );
        // Order is part of it: the listing order is what the caller hashed.
        assert_ne!(
            stamp_objects([marker, chunk]),
            stamp_objects([chunk, marker])
        );
    }

    /// Bytes this build cannot read must be a miss, not a panic and not an
    /// error. Every doubt infers.
    #[test]
    fn an_unreadable_row_is_a_miss() {
        assert!(read_entry(&[0u8; 8]).is_none(), "a short value");
        assert!(read_entry(&[0u8; 48]).is_none(), "a long value");
        assert!(decode_schema(b"not a flatbuffer").is_none());
    }

    /// The tables survive the handle. A collector fills them, the process
    /// restarts, and the next query still hits.
    #[test]
    fn the_entries_outlive_the_handle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.redb");
        let stamp = stamp_object(1, 1, None);
        let recorded = schema(&[("TEMP", DataType::Float64)]);

        {
            let db = Arc::new(Database::create(&path).unwrap());
            let cache = SchemaCache::from_database(db).unwrap();
            cache
                .put_file_schemas(&[(key("a.nc"), stamp, recorded.clone())])
                .unwrap();
        }

        let db = Arc::new(Database::create(&path).unwrap());
        let cache = SchemaCache::from_database(db).unwrap();
        assert_eq!(
            cache.file_schemas(&[lookup("a.nc", stamp)])[0].as_deref(),
            Some(recorded.as_ref())
        );
    }
}
