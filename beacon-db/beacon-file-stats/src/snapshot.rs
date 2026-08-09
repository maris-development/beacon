//! One consistent view of the registry, for the life of a query.
//!
//! # Why a snapshot at all
//!
//! A scan that plans its file list eagerly gets consistency for free: the list
//! is a point-in-time copy. A scan whose partitions *walk* the registry during
//! execution does not — a discovery pass committing mid-query could add, revive
//! or tombstone files under a partition's feet, and two partitions could
//! disagree about what the table holds.
//!
//! redb read transactions are MVCC: one opened at plan time keeps showing the
//! state it opened on, however many writes commit afterwards. So the fix is to
//! open exactly one, hand it to every partition, and drop it when the query
//! ends. [`ReadTransaction`] is `Send + Sync` and its iterators carry their own
//! guard, so an `Arc` of this is all a partition needs.
//!
//! Holding a read transaction pins the pages it can see, so a long query keeps
//! a little more of the database file alive than a short one. That is the cost
//! of a consistent answer, and it is bounded by the query's own lifetime.
//!
//! # Sharding without enumerating
//!
//! [`shard_prefix`](RegistrySnapshot::shard_prefix) divides the files under a
//! prefix into disjoint, contiguous path ranges of roughly equal *bytes*. It
//! walks the path index, which holds `(id, size)` — so it decodes no file
//! record, allocates no path, and builds no list. What comes back is one small
//! [`PathShard`] per partition: two path bounds and two counts. A partition is
//! then a query — "the files between here and there" — rather than a vector of
//! file identities.
//!
//! Byte balance is the reason the size lives in the path index. Splitting by
//! count lets one partition draw every large file, which is the straggler
//! `FileGroupPartitioner` exists to prevent on the listing path.

use std::sync::Arc;

use redb::{ReadOnlyTable, ReadTransaction};

use crate::error::{FileStatsError, Result};
use crate::registry::{FILES_BY_ID, FILES_BY_PATH, read_path_entry, read_record};
use crate::types::{FileId, FileRecord};

/// One partition's worth of work: the files whose paths fall in `[start, end)`.
///
/// The bounds are path bytes, so a shard is a range of the same B-tree the
/// walk reads — no id arithmetic, no assumption that a prefix's ids are
/// contiguous. `end` is `None` on the last shard, which runs to the end of the
/// prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathShard {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
    /// Files seen in this range when it was cut. An estimate by execution
    /// time, because the snapshot is fixed but pruning is not applied here.
    pub files: u64,
    /// Bytes seen in this range. Zero when no file in it recorded a size.
    pub bytes: u64,
}

/// What a prefix holds, and how it was divided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixShards {
    pub shards: Vec<PathShard>,
    pub files: u64,
    pub bytes: u64,
}

/// A fixed view of the registry.
pub struct RegistrySnapshot {
    txn: ReadTransaction,
}

impl std::fmt::Debug for RegistrySnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RegistrySnapshot")
    }
}

impl RegistrySnapshot {
    pub(crate) fn new(txn: ReadTransaction) -> Self {
        Self { txn }
    }

    fn by_path(&self) -> Result<ReadOnlyTable<&'static [u8], &'static [u8]>> {
        Ok(self.txn.open_table(FILES_BY_PATH)?)
    }

    fn by_id(&self) -> Result<ReadOnlyTable<&'static [u8], &'static [u8]>> {
        Ok(self.txn.open_table(FILES_BY_ID)?)
    }

    /// Divide the files under `prefix` into at most `parts` disjoint path
    /// ranges of roughly equal bytes.
    ///
    /// One walk of the path index, reading only its keys and 16-byte values:
    /// no record is decoded and no path is allocated, so this stays a B-tree
    /// scan rather than a deserialization pass over the collection.
    ///
    /// Shards close on **either** their share of the bytes or their share of
    /// the files. Bytes alone is not enough: one large file among many small
    /// ones takes its own shard and leaves every remaining file in the next,
    /// which is a partition that reads 84 files while eleven others idle.
    /// Whichever limit a shard reaches first closes it, so a skewed collection
    /// still spreads across the partition budget.
    ///
    /// A collection whose entries predate sizes in the index reports zero
    /// bytes and is divided by count alone, which is what the listing path
    /// would have done anyway.
    pub fn shard_prefix(&self, prefix: &str, parts: usize) -> Result<PrefixShards> {
        let parts = parts.max(1);
        let table = self.by_path()?;

        // First pass: totals. Keys and values only.
        let (mut files, mut bytes) = (0u64, 0u64);
        for entry in table.range(prefix.as_bytes()..)? {
            let (key, value) = entry?;
            if !key.value().starts_with(prefix.as_bytes()) {
                break;
            }
            files += 1;
            bytes += read_path_entry(value.value())?.1.unwrap_or(0);
        }
        if files == 0 {
            return Ok(PrefixShards {
                shards: Vec::new(),
                files: 0,
                bytes: 0,
            });
        }

        // Second pass: cut where the running total crosses each share. Two
        // walks rather than one because the shares cannot be known before the
        // totals are, and a walk is far cheaper than holding the keys.
        let share_bytes = (bytes > 0).then(|| bytes.div_ceil(parts as u64));
        let share_files = files.div_ceil(parts as u64);

        let mut shards: Vec<PathShard> = Vec::with_capacity(parts);
        let mut start = prefix.as_bytes().to_vec();
        let (mut run_files, mut run_bytes) = (0u64, 0u64);
        for entry in table.range(prefix.as_bytes()..)? {
            let (key, value) = entry?;
            let key = key.value();
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            let size = read_path_entry(value.value())?.1.unwrap_or(0);
            run_files += 1;
            run_bytes += size;

            let full = run_files >= share_files
                || share_bytes.is_some_and(|share| run_bytes >= share);
            // Never cut after the last file: that would leave a trailing shard
            // covering nothing.
            if full && shards.len() + 1 < parts {
                // The bound is exclusive and this file belongs to the shard
                // being closed, so the next shard starts just past this key.
                let mut end = key.to_vec();
                end.push(0);
                shards.push(PathShard {
                    start: std::mem::replace(&mut start, end.clone()),
                    end: Some(end),
                    files: run_files,
                    bytes: run_bytes,
                });
                run_files = 0;
                run_bytes = 0;
            }
        }
        if run_files > 0 || shards.is_empty() {
            shards.push(PathShard {
                start,
                end: None,
                files: run_files,
                bytes: run_bytes,
            });
        }

        Ok(PrefixShards {
            shards,
            files,
            bytes,
        })
    }

    /// Walk one shard, visiting every live file in it, in path order.
    ///
    /// This is the execute-time half: records are decoded here, one at a time,
    /// for the files of this partition only. The visitor decides what to keep,
    /// and returning `false` stops the walk — which is how a limit stops
    /// reading rather than walking to the end of the shard.
    ///
    /// Deleted files are skipped, as everywhere else: a tombstone says the
    /// file is not there.
    pub fn for_each_in_shard(
        &self,
        prefix: &str,
        shard: &PathShard,
        mut visit: impl FnMut(FileId, FileRecord) -> bool,
    ) -> Result<()> {
        let by_path = self.by_path()?;
        let by_id = self.by_id()?;

        for entry in by_path.range(shard.start.as_slice()..)? {
            let (key, value) = entry?;
            let key = key.value();
            if !key.starts_with(prefix.as_bytes()) {
                break; // past the prefix
            }
            if let Some(end) = &shard.end
                && key >= end.as_slice()
            {
                break; // past this shard; the next one owns the rest
            }
            let id = read_path_entry(value.value())?.0;
            let Some(record) = read_record(&by_id, id)? else {
                return Err(FileStatsError::Registry(format!(
                    "file {id} has a path entry but no record"
                )));
            };
            if record.state != crate::types::FileState::Deleted && !visit(id, record) {
                break;
            }
        }
        Ok(())
    }

    /// The ids and sizes of the live files under `prefix`, in path order.
    ///
    /// The pruning path's input. It enumerates, because
    /// [`prune_files`](crate::prune_files) evaluates a predicate over a row per
    /// candidate and there is no way to know which files survive without
    /// naming them. What it enumerates is 16 bytes a file, not a record: no
    /// path, no allocation.
    ///
    /// Deleted files are skipped. The path index carries no state, so this
    /// reads each candidate's record — the cost the streaming path avoids and
    /// this one cannot.
    pub fn candidates_under_prefix(&self, prefix: &str) -> Result<Vec<(FileId, u64)>> {
        let by_path = self.by_path()?;
        let by_id = self.by_id()?;

        let mut out = Vec::new();
        for entry in by_path.range(prefix.as_bytes()..)? {
            let (key, value) = entry?;
            if !key.value().starts_with(prefix.as_bytes()) {
                break;
            }
            let (id, size) = read_path_entry(value.value())?;
            let Some(record) = read_record(&by_id, id)? else {
                return Err(FileStatsError::Registry(format!(
                    "file {id} has a path entry but no record"
                )));
            };
            if record.state != crate::types::FileState::Deleted {
                out.push((id, size.unwrap_or(record.size)));
            }
        }
        Ok(out)
    }

    /// Whether the registry has ever interned any of `names` as a column.
    ///
    /// One B-tree lookup per name, and no walk. A scan planner asks this
    /// before deciding to enumerate: if a predicate names no column the
    /// registry has heard of, pruning cannot drop a single file, and
    /// enumerating candidates to discover that would be the whole cost for
    /// none of the benefit.
    pub fn knows_any_column(&self, names: &[&str]) -> Result<bool> {
        let table = self.txn.open_table(crate::registry::COLUMNS_BY_NAME)?;
        for name in names {
            if table.get(name.as_bytes())?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Fetch records by id, in the caller's order, `None` where unknown.
    pub fn records_for_ids(&self, ids: &[FileId]) -> Result<Vec<Option<FileRecord>>> {
        let by_id = self.by_id()?;
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            out.push(read_record(&by_id, *id)?);
        }
        Ok(out)
    }

    /// One file's record, by path. `None` when the path is unknown or
    /// tombstoned.
    pub fn record_by_path(&self, path: &str) -> Result<Option<(FileId, FileRecord)>> {
        let by_path = self.by_path()?;
        let Some(value) = by_path.get(path.as_bytes())? else {
            return Ok(None);
        };
        let id = read_path_entry(value.value())?.0;
        let by_id = self.by_id()?;
        match read_record(&by_id, id)? {
            Some(record) if record.state != crate::types::FileState::Deleted => {
                Ok(Some((id, record)))
            }
            _ => Ok(None),
        }
    }
}

/// A snapshot is shared by every partition of a query.
pub type SharedSnapshot = Arc<RegistrySnapshot>;

#[cfg(test)]
mod tests {
    use crate::registry::Registry;
    use crate::types::{FileState, ObservedFile};

    fn registry() -> (Registry, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry::open(dir.path().join("registry.redb")).unwrap();
        (registry, dir)
    }

    fn observed(path: &str, size: u64) -> ObservedFile {
        ObservedFile::new(path, size, 1_700_000_000_000)
    }

    /// Every file under the prefix lands in exactly one shard, and the shards
    /// tile the prefix in path order. That is the correctness bar: a scan
    /// reads each surviving file once.
    #[test]
    fn shards_are_disjoint_and_cover_the_prefix() {
        let (registry, _dir) = registry();
        let files: Vec<ObservedFile> = (0..20)
            .map(|i| observed(&format!("obs/{i:03}.parquet"), 100))
            .collect();
        registry.intern_files(&files).unwrap();
        // A file outside the prefix must never appear in a shard.
        registry.intern_files(&[observed("other/x.parquet", 100)]).unwrap();

        let snapshot = registry.snapshot().unwrap();
        let sharded = snapshot.shard_prefix("obs/", 4).unwrap();
        assert_eq!(sharded.files, 20);
        assert_eq!(sharded.bytes, 2000);
        assert!(sharded.shards.len() <= 4, "{:?}", sharded.shards.len());

        let mut seen = Vec::new();
        for shard in &sharded.shards {
            snapshot
                .for_each_in_shard("obs/", shard, |_, record| {
                    seen.push(record.path);
                    true
                })
                .unwrap();
        }
        assert_eq!(seen.len(), 20, "every file exactly once");
        let mut sorted = seen.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 20, "no file in two shards");
        assert_eq!(seen, sorted, "and the walk stays in path order");
    }

    /// A shard closes on whichever share it reaches first, bytes or files.
    ///
    /// Bytes alone would put the one large file in its own shard and every
    /// other file in the next — a partition that reads three while two idle.
    /// The file share splits those out; the byte share still keeps the large
    /// one alone.
    #[test]
    fn shards_close_on_bytes_or_on_count() {
        let (registry, _dir) = registry();
        registry
            .intern_files(&[
                observed("obs/a", 1000),
                observed("obs/b", 10),
                observed("obs/c", 10),
                observed("obs/d", 10),
            ])
            .unwrap();

        let snapshot = registry.snapshot().unwrap();
        let sharded = snapshot.shard_prefix("obs/", 4).unwrap();
        assert_eq!(sharded.shards.len(), 4, "four files fill four partitions");
        assert_eq!(sharded.shards[0].files, 1, "the large file stands alone");
        assert_eq!(sharded.shards[0].bytes, 1000);

        // With a partition budget below the file count, the file share is what
        // bounds a shard: 4 files over 2 partitions is 2 each, and the large
        // file's own share still closes the first early.
        let sharded = snapshot.shard_prefix("obs/", 2).unwrap();
        assert_eq!(sharded.shards.len(), 2);
        assert_eq!(sharded.shards[0].files, 1);
        assert_eq!(sharded.shards[1].files, 3);
    }

    /// A skewed collection must still spread across the partition budget: the
    /// regression this guards is one huge file leaving 84 of 100 in a single
    /// partition.
    #[test]
    fn one_huge_file_does_not_strand_the_rest_in_one_shard() {
        let (registry, _dir) = registry();
        let mut files = vec![observed("obs/00000", 900_000)];
        files.extend((1..100).map(|i| observed(&format!("obs/{i:05}"), 1_000)));
        registry.intern_files(&files).unwrap();

        let snapshot = registry.snapshot().unwrap();
        let sharded = snapshot.shard_prefix("obs/", 12).unwrap();
        assert_eq!(sharded.shards.len(), 12, "every partition gets work");
        let largest = sharded.shards.iter().map(|s| s.files).max().unwrap();
        assert!(
            largest <= 100 / 12 + 1,
            "no shard may hoard the small files, got {largest}"
        );
    }

    /// A collection whose index predates recorded sizes still shards, by
    /// count. Nothing needs converting for a query to work.
    #[test]
    fn a_sizeless_collection_shards_by_count() {
        let (registry, _dir) = registry();
        let files: Vec<ObservedFile> = (0..8)
            .map(|i| observed(&format!("obs/{i}"), 0))
            .collect();
        registry.intern_files(&files).unwrap();

        let snapshot = registry.snapshot().unwrap();
        let sharded = snapshot.shard_prefix("obs/", 4).unwrap();
        assert_eq!(sharded.bytes, 0);
        assert_eq!(sharded.shards.len(), 4);
        assert!(sharded.shards.iter().all(|shard| shard.files == 2));
    }

    /// An empty prefix yields no shards, so a caller can tell "nothing here"
    /// from "one empty partition".
    #[test]
    fn an_unknown_prefix_yields_no_shards() {
        let (registry, _dir) = registry();
        registry.intern_files(&[observed("obs/a", 1)]).unwrap();
        let snapshot = registry.snapshot().unwrap();
        let sharded = snapshot.shard_prefix("nothing/", 4).unwrap();
        assert!(sharded.shards.is_empty());
        assert_eq!(sharded.files, 0);
    }

    /// The point of a snapshot: a write committed after it opened is invisible
    /// to it, so every partition of a running query sees one state.
    #[test]
    fn a_snapshot_does_not_see_later_writes() {
        let (registry, _dir) = registry();
        registry.intern_files(&[observed("obs/a", 10)]).unwrap();

        let snapshot = registry.snapshot().unwrap();
        assert_eq!(snapshot.shard_prefix("obs/", 4).unwrap().files, 1);

        // A discovery pass lands mid-query.
        registry.intern_files(&[observed("obs/b", 10)]).unwrap();
        assert_eq!(
            snapshot.shard_prefix("obs/", 4).unwrap().files,
            1,
            "the snapshot still shows the state it opened on"
        );
        // And a fresh one sees the new file.
        assert_eq!(registry.snapshot().unwrap().shard_prefix("obs/", 4).unwrap().files, 2);
    }

    /// A tombstone drops out of the walk, and the visitor can stop it early —
    /// which is how a limit stops reading instead of walking to the end.
    #[test]
    fn the_walk_skips_tombstones_and_stops_when_asked() {
        let (registry, _dir) = registry();
        registry
            .intern_files(&[observed("obs/a", 1), observed("obs/b", 1), observed("obs/c", 1)])
            .unwrap();
        registry
            .reconcile_prefix("obs/", &[observed("obs/a", 1), observed("obs/c", 1)])
            .unwrap();

        let snapshot = registry.snapshot().unwrap();
        let sharded = snapshot.shard_prefix("obs/", 1).unwrap();

        let mut seen = Vec::new();
        snapshot
            .for_each_in_shard("obs/", &sharded.shards[0], |_, record| {
                seen.push(record.path);
                true
            })
            .unwrap();
        assert_eq!(seen, vec!["obs/a", "obs/c"], "the tombstone is gone");

        let mut first = Vec::new();
        snapshot
            .for_each_in_shard("obs/", &sharded.shards[0], |_, record| {
                first.push(record.path);
                false
            })
            .unwrap();
        assert_eq!(first.len(), 1, "the visitor stopped the walk");
    }

    /// The pruning path's input: ids and sizes, tombstones excluded.
    #[test]
    fn candidates_carry_ids_and_sizes_without_tombstones() {
        let (registry, _dir) = registry();
        registry
            .intern_files(&[observed("obs/a", 10), observed("obs/b", 20)])
            .unwrap();
        registry
            .reconcile_prefix("obs/", &[observed("obs/a", 10)])
            .unwrap();

        let snapshot = registry.snapshot().unwrap();
        assert_eq!(snapshot.candidates_under_prefix("obs/").unwrap(), vec![(0, 10)]);

        // And a single path resolves, unless it is tombstoned.
        assert!(snapshot.record_by_path("obs/a").unwrap().is_some());
        assert!(snapshot.record_by_path("obs/b").unwrap().is_none());
        assert!(snapshot.record_by_path("obs/missing").unwrap().is_none());
        assert_eq!(
            snapshot.record_by_path("obs/a").unwrap().unwrap().1.state,
            FileState::Pending
        );
    }
}
