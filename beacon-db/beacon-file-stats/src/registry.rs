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
const STATE: ByteTable = TableDefinition::new("fs_state");

const NEXT_FILE_ID: &[u8] = b"next_file_id";
const NEXT_COLUMN_ID: &[u8] = b"next_column_id";

/// One file's analysis outcome, for [`Registry::mark_analyzed_batch`].
#[derive(Debug, Clone, Copy)]
pub struct AnalyzedFile<'a> {
    pub id: FileId,
    pub format: &'a str,
    pub num_rows: Option<u64>,
    pub total_byte_size: Option<u64>,
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
                        if !record.matches(file) {
                            record.size = file.size;
                            record.last_modified_millis = file.last_modified_millis;
                            record.e_tag = file.e_tag.clone();
                            record.state = FileState::Stale;
                            by_id.insert(
                                file_key(id).as_slice(),
                                encode_record(&record)?.as_slice(),
                            )?;
                            pending.insert(file_key(id).as_slice(), [].as_slice())?;
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
    ) -> Result<()> {
        self.mark_analyzed_batch(&[AnalyzedFile {
            id,
            format,
            num_rows,
            total_byte_size,
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
            for file in files {
                let Some(mut record) = read_record(&by_id, file.id)? else {
                    return Err(FileStatsError::Registry(format!("unknown file {}", file.id)));
                };
                record.format = file.format.to_string();
                record.num_rows = file.num_rows;
                record.total_byte_size = file.total_byte_size;
                record.state = FileState::Analyzed;
                record.stats_epoch += 1;
                by_id.insert(
                    file_key(file.id).as_slice(),
                    encode_record(&record)?.as_slice(),
                )?;
                pending.remove(file_key(file.id).as_slice())?;
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
        registry.mark_analyzed(id, "netcdf", Some(10), Some(100)).unwrap();
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
        registry.mark_analyzed(id, "netcdf", None, None).unwrap();

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
            registry.mark_analyzed(*id, "csv", Some(1), Some(1)).unwrap();
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

    #[test]
    fn analysis_records_the_summary_the_scan_layer_asks_for() {
        let (registry, _dir) = registry();
        let id = registry.intern_files(&[observed("a", 1)]).unwrap()[0];
        registry.mark_analyzed(id, "parquet", Some(42), Some(4096)).unwrap();

        let record = registry.record(id).unwrap().unwrap();
        assert_eq!(record.num_rows, Some(42));
        assert_eq!(record.total_byte_size, Some(4096));
        assert_eq!(record.format, "parquet");
        assert_eq!(record.stats_epoch, 1);
    }
}
