//! Reading one column out of a segment, and nothing else.
//!
//! A lookup costs three ranged reads at worst: the footer, one column index
//! chunk, and the block. With the footer cached it costs two. The count does not
//! grow with the number of columns in the segment, which is the property the
//! whole layout exists to provide.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use bytes::Bytes;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, path::Path};

use crate::error::{FileStatsError, Result};
use crate::types::ColumnId;

use super::format::{
    INDEX_ENTRY_LEN, MAGIC, SegmentFooter, StatBlockMeta, TRAILER_LEN, VERSION, decode_index_entry,
};
use super::values::{decode_counts, decode_u64s, decode_values};

/// How much of a segment's tail the first read grabs. Large enough that a
/// typical footer arrives whole, small enough to be cheap when it does not.
const TAIL_READ: u64 = 64 * 1024;

/// One column's statistics, as read back from a segment.
///
/// Every vector and array has the same length, and `file_ids` is ascending. Row
/// `i` describes file `file_ids[i]`.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub data_type: DataType,
    pub file_ids: Vec<u64>,
    pub min: ArrayRef,
    pub max: ArrayRef,
    /// Nullable: an entry is null where the format reported no count.
    pub null_count: ArrayRef,
    /// Nullable, and null for every netCDF file, which reports no row count.
    pub row_count: ArrayRef,
}

impl ColumnStats {
    pub fn len(&self) -> usize {
        self.file_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.file_ids.is_empty()
    }
}

/// A reader over one segment object.
pub struct SegmentReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    footer: SegmentFooter,
    type_table: Vec<DataType>,
}

impl SegmentReader {
    /// Read and validate the segment tail.
    pub async fn open(store: Arc<dyn ObjectStore>, path: Path) -> Result<Self> {
        let size = store.head(&path).await?.size;
        if size < (MAGIC.len() + TRAILER_LEN) as u64 {
            return Err(FileStatsError::Format(format!("{path} is too short to be a segment")));
        }

        let tail_len = TAIL_READ.min(size);
        let tail = get_range(&store, &path, size - tail_len, tail_len).await?;

        if !tail.ends_with(MAGIC) {
            return Err(FileStatsError::Format(format!("{path} has no segment trailer")));
        }
        let footer_len_at = tail.len() - TRAILER_LEN;
        let footer_len =
            u32::from_le_bytes(tail[footer_len_at..footer_len_at + 4].try_into().unwrap()) as usize;

        // The footer normally lands inside the tail read. A segment with tens of
        // thousands of columns has a larger sparse index, so fall back to a
        // second, exact read rather than growing the first one for everybody.
        let footer_bytes = if footer_len + TRAILER_LEN <= tail.len() {
            tail.slice(footer_len_at - footer_len..footer_len_at)
        } else {
            let start = size - (TRAILER_LEN + footer_len) as u64;
            get_range(&store, &path, start, footer_len as u64).await?
        };

        let footer: SegmentFooter = from_aligned_bytes(&footer_bytes)
            .map_err(|e| FileStatsError::Format(format!("{path} footer: {e}")))?;

        if footer.version != VERSION {
            return Err(FileStatsError::Format(format!(
                "{path} is segment version {}, this build reads {VERSION}",
                footer.version
            )));
        }

        let type_table: Vec<DataType> = serde_json::from_slice(&footer.type_table)
            .map_err(|e| FileStatsError::Format(format!("{path} type table: {e}")))?;

        Ok(Self {
            store,
            path,
            footer,
            type_table,
        })
    }

    /// The lowest and highest file id this segment covers.
    pub fn file_id_range(&self) -> (u64, u64) {
        (self.footer.min_file_id, self.footer.max_file_id)
    }

    pub fn num_files(&self) -> u64 {
        self.footer.num_files
    }

    /// Columns held by this segment.
    pub fn num_columns(&self) -> u32 {
        self.footer.index_len
    }

    /// Read one column's statistics, or `None` when this segment holds none.
    ///
    /// The absent case is not an error. A reader treats a missing statistic as
    /// unknown and keeps every file, which is the only direction pruning may
    /// err in.
    pub async fn column(&self, column_id: ColumnId) -> Result<Option<ColumnStats>> {
        let Some((block_offset, block_len)) = self.locate(column_id).await? else {
            return Ok(None);
        };

        let block = get_range(&self.store, &self.path, block_offset, block_len as u64).await?;
        if block.len() < 4 {
            return Err(FileStatsError::Format(format!(
                "{}: column {column_id} block is truncated",
                self.path
            )));
        }

        let meta_len_at = block.len() - 4;
        let meta_len =
            u32::from_le_bytes(block[meta_len_at..].try_into().unwrap()) as usize;
        if meta_len > meta_len_at {
            return Err(FileStatsError::Format(format!(
                "{}: column {column_id} metadata overruns its block",
                self.path
            )));
        }
        let meta: StatBlockMeta =
            from_aligned_bytes(&block.slice(meta_len_at - meta_len..meta_len_at)).map_err(|e| {
                FileStatsError::Format(format!("{}: column {column_id} metadata: {e}", self.path))
            })?;

        let data_type = self
            .type_table
            .get(meta.type_id as usize)
            .ok_or_else(|| {
                FileStatsError::Format(format!(
                    "{}: column {column_id} names type {}, outside the segment type table",
                    self.path, meta.type_id
                ))
            })?
            .clone();

        let len = meta.len as usize;
        Ok(Some(ColumnStats {
            file_ids: decode_u64s(&meta.file_ids, &block),
            min: decode_values(&meta.min, len, &data_type, &block)?,
            max: decode_values(&meta.max, len, &data_type, &block)?,
            null_count: decode_counts(&meta.null_count, &meta.null_count_valid, len, &block)?,
            row_count: decode_counts(&meta.row_count, &meta.row_count_valid, len, &block)?,
            data_type,
        }))
    }

    /// Find a column's block through the two-level index.
    async fn locate(&self, column_id: ColumnId) -> Result<Option<(u64, u32)>> {
        if self.footer.index_len == 0 || self.footer.sparse_column_ids.is_empty() {
            return Ok(None);
        }
        // The chunk whose first column id does not exceed the wanted one. When
        // even the first chunk starts above it, the column is not here.
        let chunk = match self
            .footer
            .sparse_column_ids
            .partition_point(|first| *first <= column_id)
        {
            0 => return Ok(None),
            position => position - 1,
        };

        let stride = self.footer.index_stride as usize;
        let start = chunk * stride;
        let end = (start + stride).min(self.footer.index_len as usize);
        let byte_offset = self.footer.index_offset + (start * INDEX_ENTRY_LEN) as u64;
        let byte_len = ((end - start) * INDEX_ENTRY_LEN) as u64;

        let entries = get_range(&self.store, &self.path, byte_offset, byte_len).await?;

        let count = entries.len() / INDEX_ENTRY_LEN;
        let found = binary_search_entries(&entries, count, column_id);
        Ok(found)
    }
}

/// Binary-search a chunk of fixed-width index records.
fn binary_search_entries(entries: &Bytes, count: usize, column_id: ColumnId) -> Option<(u64, u32)> {
    let mut low = 0usize;
    let mut high = count;
    while low < high {
        let mid = (low + high) / 2;
        let record = &entries[mid * INDEX_ENTRY_LEN..(mid + 1) * INDEX_ENTRY_LEN];
        let (id, offset, len) = decode_index_entry(record);
        match id.cmp(&column_id) {
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid,
            std::cmp::Ordering::Equal => return Some((offset, len)),
        }
    }
    None
}

async fn get_range(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    offset: u64,
    len: u64,
) -> Result<Bytes> {
    let options = GetOptions {
        range: Some(GetRange::Bounded(offset..offset + len)),
        ..Default::default()
    };
    Ok(store.get_opts(path, options).await?.bytes().await?)
}

/// Deserialize an archived value from bytes of unknown alignment.
///
/// rkyv's `access` rejects an unaligned buffer outright, and the base address of
/// the `Bytes` an object store returns belongs to the allocator. Footers and
/// block metadata are small, so a copy into an aligned buffer costs nothing and
/// removes the question.
fn from_aligned_bytes<T>(bytes: &[u8]) -> std::result::Result<T, rkyv::rancor::Error>
where
    T: rkyv::Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<
            rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>,
        > + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<T, rkyv::rancor::Error>(&aligned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::format::encode_index_entry;

    #[test]
    fn binary_search_finds_and_misses() {
        let mut raw = Vec::new();
        for id in [2u32, 4, 6, 8] {
            raw.extend_from_slice(&encode_index_entry(id, id as u64 * 100, 16));
        }
        let entries = Bytes::from(raw);

        assert_eq!(binary_search_entries(&entries, 4, 6), Some((600, 16)));
        assert_eq!(binary_search_entries(&entries, 4, 2), Some((200, 16)));
        assert_eq!(binary_search_entries(&entries, 4, 8), Some((800, 16)));
        assert_eq!(binary_search_entries(&entries, 4, 5), None);
        assert_eq!(binary_search_entries(&entries, 4, 9), None);
    }
}
