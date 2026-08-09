//! The identity layer: paths and column names to dense ordinals, and back.
//!
//! Segments reference files by [`FileId`] and columns by [`ColumnId`] rather
//! than by name. A 200-byte path repeated across 50M cells would cost 10 GB; the
//! ids cost 400 MB. Ids are also stable, so a segment written months ago still
//! means what it said: a delete sets [`FileState::Deleted`] and keeps the slot.
//!
//! Nothing here loads the whole registry. A scan touches the ids of its own
//! files, and redb pages in only what the B-tree walk reaches.

use std::path::Path as FsPath;
use std::sync::Arc;

use redb::{
    Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
};

use crate::error::{FileStatsError, Result};
use crate::types::{ColumnId, FileId, FileRecord, FileState, ObservedFile};

// Every table is `<&[u8], &[u8]>`, and every name is prefixed. Both are the
// tenant contract `RedbStore::database` sets out: a vacuum rewrites the whole
// file and has to copy tables whose types it cannot know, so bytes is the only
// signature it can open them with. Integer keys are big-endian, because redb
// orders byte keys lexicographically and the collector's queue depends on
// ascending file ids.
type ByteTable = TableDefinition<'static, &'static [u8], &'static [u8]>;

const FILES_BY_PATH: ByteTable = TableDefinition::new("fs_files_by_path");
const FILES_BY_ID: ByteTable = TableDefinition::new("fs_files_by_id");
const COLUMNS_BY_NAME: ByteTable = TableDefinition::new("fs_columns_by_name");
const COLUMNS_BY_ID: ByteTable = TableDefinition::new("fs_columns_by_id");
/// Files awaiting analysis. A dedicated table keeps the collector's next batch
/// an O(batch) range scan instead of a full scan over every known file.
const PENDING: ByteTable = TableDefinition::new("fs_pending");
/// Files that have stored statistics which must not be trusted.
///
/// Membership is `stats_epoch > 0 && state != Analyzed`. Both halves matter. The
/// second is the danger: between a listing noticing a change and the collector
/// re-analyzing, the segments still describe content that is gone, and pruning
/// on that range drops files the new content would have matched. The first keeps
/// the set small: a file that was never analyzed has no rows anywhere, so there
/// is nothing to distrust, and during a first ingest of a million files this
/// table stays empty rather than holding all of them.
///
/// A dedicated table makes "is this file trustworthy" one range scan over the
/// churn in flight, instead of a record lookup per candidate.
const SUPPRESSED: ByteTable = TableDefinition::new("fs_suppressed");
const STATE: ByteTable = TableDefinition::new("fs_state");

const NEXT_FILE_ID: &[u8] = b"next_file_id";
const NEXT_COLUMN_ID: &[u8] = b"next_column_id";

/// What [`Registry::reconcile_prefix`] changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Files the listing reported, registered or refreshed.
    pub present: usize,
    /// Files the registry held that the listing no longer reports.
    pub deleted: usize,
    /// Files the registry held under the prefix before this call.
    pub known_before: usize,
}

/// One file's analysis outcome, for [`Registry::mark_analyzed_batch`].
#[derive(Debug, Clone, Copy)]
pub struct AnalyzedFile<'a> {
    pub id: FileId,
    pub format: &'a str,
    pub num_rows: Option<u64>,
    pub total_byte_size: Option<u64>,
    /// Columns this file contributed. Zero means the format produced no ranges.
    pub column_count: u32,
}

/// File and column identity, backed by redb.
pub struct Registry {
    db: Arc<Database>,
}

impl Registry {
    /// Open (or create) a registry in its own file.
    pub fn open(path: impl AsRef<FsPath>) -> Result<Self> {
        Self::from_database(Arc::new(Database::create(path)?))
    }

    /// Build on an existing redb database.
    ///
    /// The table names are prefixed, so the registry can share one file with
    /// other tenants. That is the path to holding it inside `beacon.db` itself:
    /// `RedbStore` takes an exclusive lock, so a second opener cannot, and
    /// sharing needs the store to hand out its `Database`.
    pub fn from_database(db: Arc<Database>) -> Result<Self> {
        // Create every table up front so read transactions never meet a missing
        // one and have to special-case it.
        let write = db.begin_write()?;
        {
            write.open_table(FILES_BY_PATH)?;
            write.open_table(FILES_BY_ID)?;
            write.open_table(COLUMNS_BY_NAME)?;
            write.open_table(COLUMNS_BY_ID)?;
            write.open_table(PENDING)?;
            write.open_table(SUPPRESSED)?;
            write.open_table(STATE)?;
        }
        write.commit()?;
        Ok(Self { db })
    }

    /// Assign ids to `observed`, in order.
    ///
    /// A path already known keeps its id. When the file changed underneath, the
    /// record is refreshed and marked [`FileState::Stale`], which reads as "no
    /// statistics" until the collector catches up. One write transaction covers
    /// the whole batch.
    pub fn intern_files(&self, observed: &[ObservedFile]) -> Result<Vec<FileId>> {
        let write = self.db.begin_write()?;
        let mut ids = Vec::with_capacity(observed.len());
        {
            let mut by_path = write.open_table(FILES_BY_PATH)?;
            let mut by_id = write.open_table(FILES_BY_ID)?;
            let mut pending = write.open_table(PENDING)?;
            let mut suppressed = write.open_table(SUPPRESSED)?;
            let mut state = write.open_table(STATE)?;

            let mut next = match state.get(NEXT_FILE_ID)? {
                Some(value) => read_u64(value.value())?,
                None => 0,
            };

            for file in observed {
                let existing = match by_path.get(file.path.as_bytes())? {
                    Some(value) => Some(read_u64(value.value())?),
                    None => None,
                };
                match existing {
                    Some(id) => {
                        let mut record = read_record(&by_id, id)?.ok_or_else(|| {
                            FileStatsError::Registry(format!(
                                "file {id} has a path entry but no record"
                            ))
                        })?;
                        // A tombstoned path that turns up again must come back,
                        // even byte-identical: its record says Deleted, which
                        // suppresses its statistics and keeps it off the queue.
                        // Re-analyzing an unchanged revival wastes one read; not
                        // reviving it loses the file until someone notices.
                        let revived = record.state == FileState::Deleted;
                        if revived || !record.matches(file) {
                            let had_statistics = record.stats_epoch > 0;
                            record.size = file.size;
                            record.last_modified_millis = file.last_modified_millis;
                            record.e_tag = file.e_tag.clone();
                            record.state = FileState::Stale;
                            by_id.insert(
                                file_key(id).as_slice(),
                                encode_record(&record)?.as_slice(),
                            )?;
                            pending.insert(file_key(id).as_slice(), [].as_slice())?;
                            if had_statistics {
                                suppressed.insert(file_key(id).as_slice(), [].as_slice())?;
                            }
                        }
                        ids.push(id);
                    }
                    None => {
                        let id = next;
                        next += 1;
                        let mut record = FileRecord::pending(
                            file.path.clone(),
                            file.size,
                            file.last_modified_millis,
                        );
                        record.e_tag = file.e_tag.clone();
                        by_path.insert(file.path.as_bytes(), file_key(id).as_slice())?;
                        by_id.insert(file_key(id).as_slice(), encode_record(&record)?.as_slice())?;
                        pending.insert(file_key(id).as_slice(), [].as_slice())?;
                        ids.push(id);
                    }
                }
            }
            state.insert(NEXT_FILE_ID, next.to_be_bytes().as_slice())?;
        }
        write.commit()?;
        Ok(ids)
    }

    /// Assign ids to column names, in order. Known names keep their id.
    pub fn intern_columns(&self, names: &[&str]) -> Result<Vec<ColumnId>> {
        let write = self.db.begin_write()?;
        let mut ids = Vec::with_capacity(names.len());
        {
            let mut by_name = write.open_table(COLUMNS_BY_NAME)?;
            let mut by_id = write.open_table(COLUMNS_BY_ID)?;
            let mut state = write.open_table(STATE)?;

            let mut next = match state.get(NEXT_COLUMN_ID)? {
                Some(value) => read_u32(value.value())?,
                None => 0,
            };

            for name in names {
                let existing = match by_name.get(name.as_bytes())? {
                    Some(value) => Some(read_u32(value.value())?),
                    None => None,
                };
                match existing {
                    Some(id) => ids.push(id),
                    None => {
                        let id = next;
                        next += 1;
                        by_name.insert(name.as_bytes(), column_key(id).as_slice())?;
                        by_id.insert(column_key(id).as_slice(), name.as_bytes())?;
                        ids.push(id);
                    }
                }
            }
            state.insert(NEXT_COLUMN_ID, next.to_be_bytes().as_slice())?;
        }
        write.commit()?;
        Ok(ids)
    }

    pub fn file_id(&self, path: &str) -> Result<Option<FileId>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(FILES_BY_PATH)?;
        match table.get(path.as_bytes())? {
            Some(value) => Ok(Some(read_u64(value.value())?)),
            None => Ok(None),
        }
    }

    /// Resolve many paths at once, in order, `None` where the path is unknown.
    ///
    /// One read transaction and one open table for the whole batch. A scan holds
    /// paths and the store holds ids, so this conversion sits between a query and
    /// any pruning at all: doing it a path at a time costs a transaction and two
    /// table opens per file, which at a million files is seconds of pure
    /// overhead.
    pub fn file_ids(&self, paths: &[&str]) -> Result<Vec<Option<FileId>>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(FILES_BY_PATH)?;
        let mut out = Vec::with_capacity(paths.len());
        for path in paths {
            out.push(match table.get(path.as_bytes())? {
                Some(value) => Some(read_u64(value.value())?),
                None => None,
            });
        }
        Ok(out)
    }

    pub fn record(&self, id: FileId) -> Result<Option<FileRecord>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(FILES_BY_ID)?;
        read_record(&table, id)
    }

    pub fn column_id(&self, name: &str) -> Result<Option<ColumnId>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(COLUMNS_BY_NAME)?;
        match table.get(name.as_bytes())? {
            Some(value) => Ok(Some(read_u32(value.value())?)),
            None => Ok(None),
        }
    }

    pub fn column_name(&self, id: ColumnId) -> Result<Option<String>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(COLUMNS_BY_ID)?;
        match table.get(column_key(id).as_slice())? {
            Some(value) => Ok(Some(
                String::from_utf8(value.value().to_vec())
                    .map_err(|e| FileStatsError::Registry(format!("column {id} name: {e}")))?,
            )),
            None => Ok(None),
        }
    }

    /// The next files awaiting analysis, ascending by id.
    ///
    /// Ascending matters twice: it is the order
    /// [`SegmentBuilder::push_file`](crate::segment::SegmentBuilder::push_file)
    /// requires, and it keeps a batch's file ids contiguous, which sharpens the
    /// manifest's range test.
    pub fn next_pending(&self, limit: usize) -> Result<Vec<(FileId, FileRecord)>> {
        let read = self.db.begin_read()?;
        let pending = read.open_table(PENDING)?;
        let by_id = read.open_table(FILES_BY_ID)?;

        let mut out = Vec::with_capacity(limit);
        for entry in pending.iter()?.take(limit) {
            let (key, _) = entry?;
            let id = read_u64(key.value())?;
            if let Some(record) = read_record(&by_id, id)? {
                out.push((id, record));
            }
        }
        Ok(out)
    }

    /// Record a successful analysis and take the file off the queue.
    ///
    /// Prefer [`mark_analyzed_batch`](Self::mark_analyzed_batch) for more than a
    /// handful: every call here is one redb transaction, and a transaction is one
    /// fsync.
    pub fn mark_analyzed(
        &self,
        id: FileId,
        format: &str,
        num_rows: Option<u64>,
        total_byte_size: Option<u64>,
        column_count: u32,
    ) -> Result<()> {
        self.mark_analyzed_batch(&[AnalyzedFile {
            id,
            format,
            num_rows,
            total_byte_size,
            column_count,
        }])
    }

    /// Record a whole batch's analyses in one transaction.
    ///
    /// A transaction costs an fsync, so doing this per file caps the collector at
    /// a few hundred files a second however fast the analysis is. At a million
    /// files that is the difference between minutes and hours.
    pub fn mark_analyzed_batch(&self, files: &[AnalyzedFile<'_>]) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }
        let write = self.db.begin_write()?;
        {
            let mut by_id = write.open_table(FILES_BY_ID)?;
            let mut pending = write.open_table(PENDING)?;
            let mut suppressed = write.open_table(SUPPRESSED)?;
            for file in files {
                let Some(mut record) = read_record(&by_id, file.id)? else {
                    return Err(FileStatsError::Registry(format!("unknown file {}", file.id)));
                };
                record.format = file.format.to_string();
                record.num_rows = file.num_rows;
                record.total_byte_size = file.total_byte_size;
                record.column_count = file.column_count;
                record.state = FileState::Analyzed;
                record.stats_epoch += 1;
                by_id.insert(
                    file_key(file.id).as_slice(),
                    encode_record(&record)?.as_slice(),
                )?;
                pending.remove(file_key(file.id).as_slice())?;
                suppressed.remove(file_key(file.id).as_slice())?;
            }
        }
        write.commit()?;
        Ok(())
    }

    /// Move a batch of files to one state, in a single transaction.
    pub fn set_state_batch(&self, ids: &[FileId], new_state: FileState) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let write = self.db.begin_write()?;
        {
            let mut by_id = write.open_table(FILES_BY_ID)?;
            let mut pending = write.open_table(PENDING)?;
            let mut suppressed = write.open_table(SUPPRESSED)?;
            for id in ids {
                let Some(mut record) = read_record(&by_id, *id)? else {
                    return Err(FileStatsError::Registry(format!("unknown file {id}")));
                };
                record.state = new_state;
                by_id.insert(file_key(*id).as_slice(), encode_record(&record)?.as_slice())?;
                match new_state {
                    FileState::Pending | FileState::Stale => {
                        pending.insert(file_key(*id).as_slice(), [].as_slice())?;
                    }
                    _ => {
                        pending.remove(file_key(*id).as_slice())?;
                    }
                }
                if new_state != FileState::Analyzed && record.stats_epoch > 0 {
                    suppressed.insert(file_key(*id).as_slice(), [].as_slice())?;
                } else {
                    suppressed.remove(file_key(*id).as_slice())?;
                }
            }
        }
        write.commit()?;
        Ok(())
    }

    /// Move a file to a terminal or waiting state.
    ///
    /// [`FileState::Pending`] and [`FileState::Stale`] put it back on the queue.
    /// Everything else takes it off.
    pub fn set_state(&self, id: FileId, new_state: FileState) -> Result<()> {
        self.set_state_batch(&[id], new_state)
    }

    /// Files known to the registry, tombstones included.
    pub fn num_files(&self) -> Result<u64> {
        let read = self.db.begin_read()?;
        Ok(read.open_table(FILES_BY_ID)?.len()?)
    }

    /// Distinct column names ever seen.
    pub fn num_columns(&self) -> Result<u64> {
        let read = self.db.begin_read()?;
        Ok(read.open_table(COLUMNS_BY_NAME)?.len()?)
    }

    /// The files in `range` whose stored statistics must not be trusted,
    /// ascending.
    ///
    /// Read once per query, and only after the reader knows it has statistics to
    /// prune on. In steady state this is empty, and it is only ever as large as
    /// the churn currently in flight.
    pub fn suppressed_in_range(&self, range: (FileId, FileId)) -> Result<Vec<FileId>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(SUPPRESSED)?;
        let (low, high) = (file_key(range.0), file_key(range.1));
        let mut out = Vec::new();
        for entry in table.range(low.as_slice()..=high.as_slice())? {
            let (key, _) = entry?;
            out.push(read_u64(key.value())?);
        }
        Ok(out)
    }

    /// Reconcile a prefix against what a listing just saw.
    ///
    /// Registering files can only ever add or update, because a listing reports
    /// what is there, never what is gone. Deletion needs this comparison: every
    /// path the registry holds under `prefix` that the listing did not report is
    /// marked [`FileState::Deleted`].
    ///
    /// The path table is a B-tree keyed by path, so "everything under this
    /// prefix" is one range scan, not a walk over every file in the store.
    ///
    /// `observed` must be the *complete* listing for `prefix`. A partial one
    /// would delete the files it happened to leave out.
    pub fn reconcile_prefix(
        &self,
        prefix: &str,
        observed: &[ObservedFile],
    ) -> Result<ReconcileReport> {
        let seen: std::collections::HashSet<&str> =
            observed.iter().map(|file| file.path.as_str()).collect();

        let known: Vec<(String, FileId)> = {
            let read = self.db.begin_read()?;
            let table = read.open_table(FILES_BY_PATH)?;
            let mut out = Vec::new();
            for entry in table.range(prefix.as_bytes()..)? {
                let (key, value) = entry?;
                let path = String::from_utf8_lossy(key.value()).into_owned();
                if !path.starts_with(prefix) {
                    break; // the scan has walked past the prefix
                }
                out.push((path, read_u64(value.value())?));
            }
            out
        };

        let gone: Vec<FileId> = known
            .iter()
            .filter(|(path, _)| !seen.contains(path.as_str()))
            .map(|(_, id)| *id)
            .collect();

        let before = known.len();
        let ids = self.intern_files(observed)?;
        self.set_state_batch(&gone, FileState::Deleted)?;

        Ok(ReconcileReport {
            present: ids.len(),
            deleted: gone.len(),
            known_before: before,
        })
    }

    /// Walk the live files under a path prefix, ascending by path, without
    /// materialising them.
    ///
    /// This is the walk a registry-backed scan plans from: the path table is a
    /// B-tree keyed by path bytes, so "everything under this prefix" is one
    /// range scan, and the records arrive already in the order a listing would
    /// have produced. The visitor keeps only what it needs — at a million
    /// files, holding every record alive is exactly the memory this crate
    /// exists to avoid spending.
    ///
    /// Tombstoned files are skipped, because a scan wants what is there and a
    /// [`FileState::Deleted`] record says the file is not. Every other state is
    /// visited, analyzed or not: a pending or stale file exists and must be
    /// read, it simply carries no trustworthy statistics yet.
    ///
    /// One read transaction covers the whole walk, so the visitor sees a
    /// consistent snapshot even while discovery is writing.
    pub fn for_each_under_prefix(
        &self,
        prefix: &str,
        mut visit: impl FnMut(FileId, FileRecord),
    ) -> Result<()> {
        let read = self.db.begin_read()?;
        let by_path = read.open_table(FILES_BY_PATH)?;
        let by_id = read.open_table(FILES_BY_ID)?;

        for entry in by_path.range(prefix.as_bytes()..)? {
            let (key, value) = entry?;
            if !key.value().starts_with(prefix.as_bytes()) {
                break; // the scan has walked past the prefix
            }
            let id = read_u64(value.value())?;
            let Some(record) = read_record(&by_id, id)? else {
                return Err(FileStatsError::Registry(format!(
                    "file {id} has a path entry but no record"
                )));
            };
            if record.state != FileState::Deleted {
                visit(id, record);
            }
        }
        Ok(())
    }

    /// The live files under a path prefix, ascending by path, materialised.
    ///
    /// [`for_each_under_prefix`](Self::for_each_under_prefix) with a `Vec` at
    /// the end. For collections small enough to hold whole; a scan planner
    /// walks instead.
    pub fn records_under_prefix(&self, prefix: &str) -> Result<Vec<(FileId, FileRecord)>> {
        let mut out = Vec::new();
        self.for_each_under_prefix(prefix, |id, record| out.push((id, record)))?;
        Ok(out)
    }

    /// Fetch many records by id under one read transaction, in the caller's
    /// order, `None` where the id is unknown.
    ///
    /// The execute-time counterpart of the planner's walk: a scan partition
    /// holds file ids and turns a chunk of them back into records right before
    /// opening the files, so the file list never exists as records all at
    /// once.
    pub fn records_for_ids(&self, ids: &[FileId]) -> Result<Vec<Option<FileRecord>>> {
        let read = self.db.begin_read()?;
        let by_id = read.open_table(FILES_BY_ID)?;
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            out.push(read_record(&by_id, *id)?);
        }
        Ok(out)
    }

    /// Every file the registry knows, ascending by id.
    ///
    /// Materializes the lot, so it is a diagnostic path rather than a hot one:
    /// at a million files this is a million records. Nothing on the query or
    /// collection paths calls it.
    pub fn scan_records(&self) -> Result<Vec<(FileId, FileRecord)>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(FILES_BY_ID)?;
        let mut out = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            let id = read_u64(key.value())?;
            let record: FileRecord = bincode::deserialize(value.value())
                .map_err(|e| FileStatsError::Registry(format!("file record {id}: {e}")))?;
            out.push((id, record));
        }
        Ok(out)
    }

    /// Put already-analyzed files back on the queue.
    ///
    /// Nothing else does this. A file whose content has not changed is never
    /// re-queued, which is right until the *reader* changes: turn on netCDF's
    /// Rust reader and every file is `Analyzed` with no columns, correctly
    /// recorded and permanently useless. This is the way out.
    ///
    /// Their statistics are suppressed until the re-analysis lands, so pruning
    /// pauses over the affected files rather than trusting rows that are about
    /// to be replaced. Safe, and briefly slower.
    ///
    /// `prefix` restricts it; `None` takes everything.
    pub fn requeue(&self, prefix: Option<&str>) -> Result<usize> {
        let ids: Vec<FileId> = {
            let read = self.db.begin_read()?;
            let by_path = read.open_table(FILES_BY_PATH)?;
            let by_id = read.open_table(FILES_BY_ID)?;

            let mut ids = Vec::new();
            let start = prefix.unwrap_or("");
            for entry in by_path.range(start.as_bytes()..)? {
                let (key, value) = entry?;
                let path = String::from_utf8_lossy(key.value()).into_owned();
                if let Some(prefix) = prefix
                    && !path.starts_with(prefix)
                {
                    break; // the scan has walked past the prefix
                }
                let id = read_u64(value.value())?;
                if let Some(record) = read_record(&by_id, id)?
                    && record.state == FileState::Analyzed
                {
                    ids.push(id);
                }
            }
            ids
        };
        self.set_state_batch(&ids, FileState::Pending)?;
        Ok(ids.len())
    }

    pub fn num_pending(&self) -> Result<u64> {
        let read = self.db.begin_read()?;
        Ok(read.open_table(PENDING)?.len()?)
    }
}

/// Big-endian, so redb's lexicographic byte ordering is numeric ordering. The
/// collector's queue reads ascending file ids straight out of that.
fn file_key(id: FileId) -> [u8; 8] {
    id.to_be_bytes()
}

fn column_key(id: ColumnId) -> [u8; 4] {
    id.to_be_bytes()
}

fn read_u64(bytes: &[u8]) -> Result<u64> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| FileStatsError::Registry(format!("expected an 8-byte value, got {}", bytes.len())))
}

fn read_u32(bytes: &[u8]) -> Result<u32> {
    bytes
        .try_into()
        .map(u32::from_be_bytes)
        .map_err(|_| FileStatsError::Registry(format!("expected a 4-byte value, got {}", bytes.len())))
}

fn encode_record(record: &FileRecord) -> Result<Vec<u8>> {
    bincode::serialize(record).map_err(|e| FileStatsError::Registry(format!("file record: {e}")))
}

fn read_record<T>(table: &T, id: FileId) -> Result<Option<FileRecord>>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    match table.get(file_key(id).as_slice())? {
        Some(bytes) => bincode::deserialize(bytes.value())
            .map(Some)
            .map_err(|e| FileStatsError::Registry(format!("file record {id}: {e}"))),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> (Registry, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry::open(dir.path().join("registry.redb")).unwrap();
        (registry, dir)
    }

    fn observed(path: &str, size: u64) -> ObservedFile {
        ObservedFile::new(path, size, 1_700_000_000_000)
    }

    #[test]
    fn ids_are_dense_and_stable_across_calls() {
        let (registry, _dir) = registry();

        let first = registry
            .intern_files(&[observed("a.nc", 1), observed("b.nc", 2)])
            .unwrap();
        assert_eq!(first, vec![0, 1]);

        // A second sighting of the same paths returns the same ids, and a new
        // path continues the sequence.
        let second = registry
            .intern_files(&[observed("b.nc", 2), observed("c.nc", 3)])
            .unwrap();
        assert_eq!(second, vec![1, 2]);
        assert_eq!(registry.num_files().unwrap(), 3);
    }

    /// The batch form answers the same as the single form, and says `None` for
    /// a path it has never seen rather than failing the whole lookup.
    #[test]
    fn batched_lookups_match_the_single_form() {
        let (registry, _dir) = registry();
        registry
            .intern_files(&[observed("a.nc", 1), observed("b.nc", 1)])
            .unwrap();

        let batched = registry.file_ids(&["b.nc", "missing.nc", "a.nc"]).unwrap();
        assert_eq!(batched, vec![Some(1), None, Some(0)]);
        assert_eq!(batched[0], registry.file_id("b.nc").unwrap());
        assert!(registry.file_ids(&[]).unwrap().is_empty());
    }

    #[test]
    fn column_ids_behave_the_same_way() {
        let (registry, _dir) = registry();
        assert_eq!(registry.intern_columns(&["TEMP", "PSAL"]).unwrap(), vec![0, 1]);
        assert_eq!(registry.intern_columns(&["PSAL", "TIME"]).unwrap(), vec![1, 2]);
        assert_eq!(registry.column_name(2).unwrap().as_deref(), Some("TIME"));
        assert_eq!(registry.column_id("TEMP").unwrap(), Some(0));
        assert_eq!(registry.num_columns().unwrap(), 3);
    }

    /// A changed file must not keep serving its old statistics. The record goes
    /// stale and back on the queue, and the id stays put so old segments stay
    /// meaningful.
    #[test]
    fn a_changed_file_goes_stale_and_keeps_its_id() {
        let (registry, _dir) = registry();
        let id = registry.intern_files(&[observed("a.nc", 1)]).unwrap()[0];
        registry.mark_analyzed(id, "netcdf", Some(10), Some(100), 2).unwrap();
        assert_eq!(registry.record(id).unwrap().unwrap().state, FileState::Analyzed);
        assert_eq!(registry.num_pending().unwrap(), 0);

        let again = registry.intern_files(&[observed("a.nc", 999)]).unwrap()[0];
        assert_eq!(again, id, "the id survives a content change");
        let record = registry.record(id).unwrap().unwrap();
        assert_eq!(record.state, FileState::Stale);
        assert_eq!(record.size, 999);
        assert_eq!(registry.num_pending().unwrap(), 1);
    }

    /// An etag settles the question when both sides have one, and disagreeing
    /// size or mtime settles it otherwise.
    #[test]
    fn an_unchanged_file_is_not_re_queued() {
        let (registry, _dir) = registry();
        let id = registry.intern_files(&[observed("a.nc", 1)]).unwrap()[0];
        registry.mark_analyzed(id, "netcdf", None, None, 0).unwrap();

        registry.intern_files(&[observed("a.nc", 1)]).unwrap();
        assert_eq!(registry.record(id).unwrap().unwrap().state, FileState::Analyzed);
        assert_eq!(registry.num_pending().unwrap(), 0);
    }

    #[test]
    fn the_queue_hands_out_ascending_ids_and_drains() {
        let (registry, _dir) = registry();
        registry
            .intern_files(&[observed("a", 1), observed("b", 1), observed("c", 1)])
            .unwrap();

        let batch = registry.next_pending(2).unwrap();
        assert_eq!(batch.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![0, 1]);

        for (id, _) in &batch {
            registry.mark_analyzed(*id, "csv", Some(1), Some(1), 1).unwrap();
        }
        assert_eq!(registry.num_pending().unwrap(), 1);
        assert_eq!(registry.next_pending(10).unwrap()[0].0, 2);
    }

    #[test]
    fn a_failed_file_leaves_the_queue_and_a_retry_puts_it_back() {
        let (registry, _dir) = registry();
        let id = registry.intern_files(&[observed("a", 1)]).unwrap()[0];

        registry.set_state(id, FileState::Failed).unwrap();
        assert_eq!(registry.num_pending().unwrap(), 0);

        registry.set_state(id, FileState::Pending).unwrap();
        assert_eq!(registry.num_pending().unwrap(), 1);
    }


    /// Registering can only add or update: a listing reports what is there, never
    /// what is gone. Deletion needs the comparison `reconcile_prefix` does.
    #[test]
    fn reconcile_tombstones_the_paths_a_listing_no_longer_reports() {
        let (registry, _dir) = registry();
        registry
            .intern_files(&[
                observed("argo/a.nc", 1),
                observed("argo/b.nc", 1),
                observed("ctd/c.nc", 1),
            ])
            .unwrap();
        for id in 0..3 {
            registry.mark_analyzed(id, "netcdf", None, None, 0).unwrap();
        }

        // The listing for `argo/` now reports only a.nc.
        let report = registry
            .reconcile_prefix("argo/", &[observed("argo/a.nc", 1)])
            .unwrap();
        assert_eq!(report.known_before, 2);
        assert_eq!(report.present, 1);
        assert_eq!(report.deleted, 1);

        assert_eq!(registry.record(0).unwrap().unwrap().state, FileState::Analyzed);
        assert_eq!(registry.record(1).unwrap().unwrap().state, FileState::Deleted);
        // A different prefix is untouched, however sure the listing was.
        assert_eq!(registry.record(2).unwrap().unwrap().state, FileState::Analyzed);
    }

    /// The slot survives the tombstone, so a segment written before the delete
    /// still means what it said.
    #[test]
    fn a_deleted_file_keeps_its_id() {
        let (registry, _dir) = registry();
        let ids = registry.intern_files(&[observed("argo/a.nc", 1)]).unwrap();
        registry.mark_analyzed(ids[0], "netcdf", None, None, 0).unwrap();

        registry.reconcile_prefix("argo/", &[]).unwrap();
        assert_eq!(registry.record(ids[0]).unwrap().unwrap().state, FileState::Deleted);

        // A new path continues the sequence rather than reusing the slot.
        let next = registry.intern_files(&[observed("argo/b.nc", 1)]).unwrap();
        assert_eq!(next, vec![1]);
    }

    /// A tombstoned path that reappears must come back, even byte-identical.
    /// Its record still says Deleted, which suppresses its statistics and keeps
    /// it off the queue.
    #[test]
    fn a_reappearing_file_is_revived() {
        let (registry, _dir) = registry();
        let ids = registry.intern_files(&[observed("argo/a.nc", 1)]).unwrap();
        registry.mark_analyzed(ids[0], "netcdf", None, None, 0).unwrap();
        registry.reconcile_prefix("argo/", &[]).unwrap();
        assert_eq!(registry.num_pending().unwrap(), 0);

        // Same size, same mtime: nothing about the file changed, only its
        // presence.
        let again = registry.intern_files(&[observed("argo/a.nc", 1)]).unwrap();
        assert_eq!(again, ids, "the id is stable across a delete and a return");
        assert_eq!(registry.record(ids[0]).unwrap().unwrap().state, FileState::Stale);
        assert_eq!(registry.num_pending().unwrap(), 1);
    }

    /// The prefix scan is the file list a registry-backed scan plans from: it
    /// must honour the prefix boundary, come back in path order, and hold every
    /// live state while skipping tombstones.
    #[test]
    fn a_prefix_scan_returns_live_records_in_path_order() {
        let (registry, _dir) = registry();
        registry
            .intern_files(&[
                observed("argo/b.nc", 2),
                observed("argo/a.nc", 1),
                observed("argonaut/x.nc", 3),
                observed("ctd/c.nc", 4),
            ])
            .unwrap();

        // Interned out of order; the path B-tree hands them back sorted. A path
        // that merely shares the string prefix without the separator still
        // matches, the way a raw range scan must; the caller's glob draws the
        // directory boundary.
        let under = registry.records_under_prefix("argo").unwrap();
        let paths: Vec<&str> = under.iter().map(|(_, r)| r.path.as_str()).collect();
        assert_eq!(paths, vec!["argo/a.nc", "argo/b.nc", "argonaut/x.nc"]);

        let under = registry.records_under_prefix("argo/").unwrap();
        let paths: Vec<&str> = under.iter().map(|(_, r)| r.path.as_str()).collect();
        assert_eq!(paths, vec!["argo/a.nc", "argo/b.nc"]);

        // A pending file is a live file: it exists and must be read, it simply
        // has no statistics yet.
        assert!(under.iter().all(|(_, r)| r.state == FileState::Pending));

        // The empty prefix is the whole store.
        assert_eq!(registry.records_under_prefix("").unwrap().len(), 4);
        assert!(registry.records_under_prefix("zzz").unwrap().is_empty());
    }

    /// The by-id batch is the execute-time counterpart of the prefix walk: it
    /// answers in the caller's order and says `None` for an unknown id rather
    /// than failing the chunk.
    #[test]
    fn records_come_back_by_id_in_batch_order() {
        let (registry, _dir) = registry();
        registry
            .intern_files(&[observed("a.nc", 1), observed("b.nc", 2)])
            .unwrap();

        let records = registry.records_for_ids(&[1, 99, 0]).unwrap();
        assert_eq!(records[0].as_ref().unwrap().path, "b.nc");
        assert!(records[1].is_none());
        assert_eq!(records[2].as_ref().unwrap().path, "a.nc");
    }

    /// A tombstone says the file is gone, so the file list a scan builds from
    /// this must not hold it.
    #[test]
    fn a_prefix_scan_skips_tombstones() {
        let (registry, _dir) = registry();
        registry
            .intern_files(&[observed("argo/a.nc", 1), observed("argo/b.nc", 2)])
            .unwrap();
        registry
            .reconcile_prefix("argo/", &[observed("argo/b.nc", 2)])
            .unwrap();

        let under = registry.records_under_prefix("argo/").unwrap();
        let paths: Vec<&str> = under.iter().map(|(_, r)| r.path.as_str()).collect();
        assert_eq!(paths, vec!["argo/b.nc"]);
    }

    /// Untrusted files are exactly the ones not in `Analyzed`, and the range scan
    /// returns them ascending.
    #[test]
    fn suppression_tracks_the_file_state() {
        let (registry, _dir) = registry();
        let ids = registry
            .intern_files(&[observed("a", 1), observed("b", 1), observed("c", 1)])
            .unwrap();

        // Nothing is analyzed yet, so nothing has statistics to distrust. This
        // is what keeps the table empty through a first ingest.
        assert!(
            registry.suppressed_in_range((0, 2)).unwrap().is_empty(),
            "a file that was never analyzed has no rows to suppress"
        );

        for id in &ids {
            registry.mark_analyzed(*id, "csv", None, None, 0).unwrap();
        }
        assert!(registry.suppressed_in_range((0, 2)).unwrap().is_empty());

        // Now they have statistics, so a change to one suppresses that one.
        registry.intern_files(&[observed("b", 999)]).unwrap();
        assert_eq!(registry.suppressed_in_range((0, 2)).unwrap(), vec![1]);

        // Re-analysis restores trust.
        registry.mark_analyzed(ids[1], "csv", None, None, 0).unwrap();
        assert!(registry.suppressed_in_range((0, 2)).unwrap().is_empty());

        // And the range scan honours its bounds.
        assert!(registry.suppressed_in_range((2, 2)).unwrap().is_empty());
    }

    #[test]
    fn analysis_records_the_summary_the_scan_layer_asks_for() {
        let (registry, _dir) = registry();
        let id = registry.intern_files(&[observed("a", 1)]).unwrap()[0];
        registry.mark_analyzed(id, "parquet", Some(42), Some(4096), 7).unwrap();

        let record = registry.record(id).unwrap().unwrap();
        assert_eq!(record.num_rows, Some(42));
        assert_eq!(record.total_byte_size, Some(4096));
        assert_eq!(record.format, "parquet");
        assert_eq!(record.column_count, 7);
        assert_eq!(record.stats_epoch, 1);
    }
}
