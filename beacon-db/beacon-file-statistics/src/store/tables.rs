//! The redb tables of the store, and their key layout.
//!
//! Each table maps bytes to bytes, and each name starts with `fstat_`. That is
//! the contract of `RedbStore::database`: a vacuum copies tables whose types it
//! does not know. Integer keys are big-endian, so the byte order is the number order.

use redb::TableDefinition;

use super::error::{Result, StoreError};
use crate::stamp::Stamp;

pub(crate) type ByteTable = TableDefinition<'static, &'static [u8], &'static [u8]>;

/// A dense number for one column name. A stats key holds it in 4 bytes.
pub(crate) type ColumnId = u32;

/// Column name to [`ColumnId`].
pub(crate) const COLUMNS: ByteTable = TableDefinition::new("fstat_columns");
/// Stamp to the column ids of the file. No ids for a failed analysis.
pub(crate) const FILES: ByteTable = TableDefinition::new("fstat_files");
/// `column id ‖ stamp` to `bincode(ColumnStat)`.
pub(crate) const STATS: ByteTable = TableDefinition::new("fstat_stats");

/// The key of one column of one file in [`STATS`].
pub(crate) fn stat_key(column_id: ColumnId, stamp: Stamp) -> [u8; 20] {
    let mut key = [0u8; 20];
    key[..4].copy_from_slice(&column_id.to_be_bytes());
    key[4..].copy_from_slice(&stamp.to_bytes());
    key
}

/// The stamp at the end of a [`FILES`] or [`STATS`] key.
pub(crate) fn stamp_of(key: &[u8]) -> Result<Stamp> {
    let tail = key
        .len()
        .checked_sub(16)
        .map(|start| &key[start..])
        .ok_or_else(|| StoreError::Codec(format!("a key of {} bytes has no stamp", key.len())))?;
    Ok(Stamp::from_bytes(array(tail)?))
}

/// A column id, as stored in [`COLUMNS`].
pub(crate) fn decode_column_id(bytes: &[u8]) -> Result<ColumnId> {
    Ok(ColumnId::from_be_bytes(array(bytes)?))
}

/// The column ids of a [`FILES`] value.
pub(crate) fn decode_column_ids(bytes: &[u8]) -> Result<Vec<ColumnId>> {
    if !bytes.len().is_multiple_of(4) {
        return Err(StoreError::Codec(format!(
            "a column id list of {} bytes",
            bytes.len()
        )));
    }
    bytes.chunks_exact(4).map(decode_column_id).collect()
}

/// `bytes` as a fixed-size array, or an error when the length is wrong.
fn array<const N: usize>(bytes: &[u8]) -> Result<[u8; N]> {
    bytes
        .try_into()
        .map_err(|_| StoreError::Codec(format!("expected {N} bytes, found {}", bytes.len())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stats_key_sorts_by_column_then_stamp() {
        let low = stat_key(1, Stamp::from_parts(u64::MAX, u64::MAX));
        let high = stat_key(2, Stamp::from_parts(0, 0));

        assert!(low < high, "the column id comes first");
        assert_eq!(
            stamp_of(&low).expect("a stamp"),
            Stamp::from_parts(u64::MAX, u64::MAX)
        );
        let file_key = Stamp::from_parts(3, 7).to_bytes();
        assert_eq!(
            stamp_of(&file_key).expect("a stamp"),
            Stamp::from_parts(3, 7)
        );
    }

    #[test]
    fn a_bad_value_is_an_error() {
        assert!(decode_column_ids(&[0, 0, 1]).is_err());
        assert!(stamp_of(&[1, 2]).is_err());
        assert_eq!(decode_column_ids(&[0, 0, 0, 5]).expect("ids"), [5]);
    }
}
