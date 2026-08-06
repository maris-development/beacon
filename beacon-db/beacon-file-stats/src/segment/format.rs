//! The segment byte layout.
//!
//! ```text
//! [MAGIC 8]
//! [block 0][block 1] ... [block N-1]      each starts 8-aligned
//! [column index]                          N fixed 16-byte records, sorted by column_id
//! [footer: rkyv SegmentFooter]
//! [footer_len: u32 LE][MAGIC 8]
//! ```
//!
//! # Why the buffers sit outside rkyv
//!
//! rkyv aligns an archived region to the alignment of its type, and `Vec<u8>`
//! has alignment 1. So a raw Arrow buffer nested inside an archived struct can
//! land on any offset, and `ScalarBuffer<T>` *asserts* alignment to
//! `align_of::<T>()` (`arrow-buffer/src/buffer/scalar.rs:186`). Nesting the
//! buffers would therefore force a realigning copy on every read.
//!
//! Instead the writer places every buffer itself, 8-byte aligned, and the
//! archived metadata holds only [`BufRef`] offsets into the block. That is the
//! same offset-and-length indirection the binary format uses for its own index,
//! reached here for a different reason.
//!
//! Alignment still cannot be *guaranteed* end to end, because the base address
//! of the `bytes::Bytes` an object store hands back is the allocator's choice.
//! [`crate::segment::values`] therefore checks the pointer and copies only when
//! it must.
//!
//! # Why the column index is not in the footer
//!
//! A segment can hold tens of thousands of columns, so a full index is hundreds
//! of KB. The footer stays small and always-read: it carries a sparse top level
//! with one entry per [`INDEX_STRIDE`] columns. A lookup binary-searches that in
//! memory, then reads one 16 KB index chunk, then the block. Three ranged reads
//! worst case, two when the footer is cached.

/// Segment magic, at both ends of the file.
pub const MAGIC: &[u8; 8] = b"BCNFSTS\x01";

/// The format version the writer emits.
pub const VERSION: u32 = 2;

/// Column index entries per sparse top-level entry.
pub const INDEX_STRIDE: u32 = 1024;

/// Bytes per column index record.
pub const INDEX_ENTRY_LEN: usize = 16;

/// Every block and every buffer starts on this boundary, so a typed Arrow view
/// over the fetched bytes needs no realigning copy.
pub const ALIGN: usize = 8;

/// Trailer: `footer_len: u32 LE` then [`MAGIC`].
pub const TRAILER_LEN: usize = 4 + 8;

/// Round `n` up to the next [`ALIGN`] boundary.
pub const fn align_up(n: usize) -> usize {
    n.next_multiple_of(ALIGN)
}

/// A byte range inside a block, relative to the block start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct BufRef {
    pub offset: u32,
    pub len: u32,
}

impl BufRef {
    pub const EMPTY: Self = Self { offset: 0, len: 0 };

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// How one min or max column is physically stored.
///
/// Four variants cover every logical type this crate keeps statistics for.
/// `Fixed` alone covers every integer width, both floats, `Decimal128`, and
/// every date, time, timestamp, and duration: the logical type comes from the
/// segment type table, so the physical form needs only a width.
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum StatValuesMeta {
    /// No value carries information. Reads back as an all-null array.
    Null,
    Bool {
        values: BufRef,
        valid: BufRef,
    },
    Fixed {
        width: u8,
        values: BufRef,
        valid: BufRef,
    },
    Bytes {
        /// 4 for `Utf8`/`Binary`, 8 for the large variants.
        offset_width: u8,
        offsets: BufRef,
        data: BufRef,
        valid: BufRef,
    },
}

/// One column's statistics inside one segment, sorted ascending by file id.
#[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct StatBlockMeta {
    /// Index into the segment type table.
    pub type_id: u16,
    /// Rows in this block, which is how many files declare the column here.
    pub len: u32,
    /// `u64` little-endian, ascending.
    pub file_ids: BufRef,
    pub min: StatValuesMeta,
    pub max: StatValuesMeta,
    /// `u64` little-endian.
    pub null_count: BufRef,
    /// Validity for [`null_count`](Self::null_count). Empty means every entry is
    /// known.
    ///
    /// A count has to be able to say "unknown", and zero cannot: DataFusion
    /// prunes `IS NOT NULL` on `null_count != row_count`, so a pair of unknowns
    /// recorded as `0, 0` reads as "every value is null" and drops a file that is
    /// full of values. netCDF reports no row count at all, so that would be every
    /// netCDF file in a store.
    pub null_count_valid: BufRef,
    /// `u64` little-endian.
    pub row_count: BufRef,
    /// Validity for [`row_count`](Self::row_count).
    pub row_count_valid: BufRef,
}

/// The always-read tail of a segment.
#[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct SegmentFooter {
    pub version: u32,
    /// `serde_json` of `Vec<DataType>`.
    ///
    /// JSON, not bincode: arrow's `DataType` serde representation is not a
    /// self-describing-format-agnostic shape, and the table holds a few dozen
    /// entries per segment, so its size never matters.
    pub type_table: Vec<u8>,
    /// Where the fixed-width column index starts.
    pub index_offset: u64,
    /// Column index records, one per column in this segment.
    pub index_len: u32,
    /// Index records per [`sparse_column_ids`](Self::sparse_column_ids) entry.
    pub index_stride: u32,
    /// First column id of each index chunk, for the in-memory binary search.
    pub sparse_column_ids: Vec<u32>,
    pub min_file_id: u64,
    pub max_file_id: u64,
    /// Distinct files this segment covers.
    pub num_files: u64,
}

/// Encode one column index record.
pub fn encode_index_entry(column_id: u32, block_offset: u64, block_len: u32) -> [u8; INDEX_ENTRY_LEN] {
    let mut out = [0u8; INDEX_ENTRY_LEN];
    out[0..4].copy_from_slice(&column_id.to_le_bytes());
    out[4..8].copy_from_slice(&block_len.to_le_bytes());
    out[8..16].copy_from_slice(&block_offset.to_le_bytes());
    out
}

/// Decode one column index record. Returns `(column_id, block_offset, block_len)`.
pub fn decode_index_entry(bytes: &[u8]) -> (u32, u64, u32) {
    let column_id = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let block_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let block_offset = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    (column_id, block_offset, block_len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alignment_rounds_up_to_eight() {
        assert_eq!(align_up(0), 0);
        assert_eq!(align_up(1), 8);
        assert_eq!(align_up(8), 8);
        assert_eq!(align_up(9), 16);
    }

    #[test]
    fn index_entries_round_trip() {
        let bytes = encode_index_entry(7, 4096, 512);
        assert_eq!(decode_index_entry(&bytes), (7, 4096, 512));
    }
}
