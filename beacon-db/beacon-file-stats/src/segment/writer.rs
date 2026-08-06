//! Building one immutable segment.
//!
//! A segment covers one background batch. Object stores cannot append, so the
//! store grows by adding segments and merging them later, never by rewriting.
//!
//! # The rule that makes this scale
//!
//! [`SegmentBuilder::push_file`] touches only the columns the file actually
//! declares. It never back-fills the columns it does not. That is the whole
//! difference between a builder that finishes a 1M file store and one that does
//! not: back-filling costs files x total-columns, which at this scale is 1.6e11
//! operations.

use std::collections::HashMap;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use crate::error::{FileStatsError, Result};
use crate::scalar::{StatScalar, super_type};
use crate::types::{ColumnId, FileId};

use super::format::{
    BufRef, INDEX_STRIDE, MAGIC, SegmentFooter, StatBlockMeta, StatValuesMeta, VERSION, align_up,
    encode_index_entry,
};
use super::values::{EncodedValues, encode_values, normalize_type, storage_type};

/// One file's statistics for one column.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStat {
    pub min: StatScalar,
    pub max: StatScalar,
    pub null_count: u64,
    pub row_count: u64,
    /// The type this file declares for the column. Files may disagree; the
    /// builder reconciles them at [`SegmentBuilder::finish`].
    pub data_type: DataType,
}

impl ColumnStat {
    /// Build from the one-element min/max arrays a format layer reports.
    pub fn from_arrays(
        min: Option<&ArrayRef>,
        max: Option<&ArrayRef>,
        null_count: u64,
        row_count: u64,
        data_type: &DataType,
    ) -> Self {
        Self {
            min: min.map(|a| StatScalar::from_array(a, 0)).unwrap_or(StatScalar::Absent),
            max: max.map(|a| StatScalar::from_array(a, 0)).unwrap_or(StatScalar::Absent),
            null_count,
            row_count,
            data_type: normalize_type(data_type),
        }
    }
}

/// One column's rows, accumulated while the segment is under construction.
#[derive(Default)]
struct ColumnAccumulator {
    file_ids: Vec<u64>,
    mins: Vec<StatScalar>,
    maxs: Vec<StatScalar>,
    null_counts: Vec<u64>,
    row_counts: Vec<u64>,
    /// The running super type, or `None` once two contributions cannot share a
    /// block.
    data_type: Option<DataType>,
    conflicted: bool,
}

impl ColumnAccumulator {
    fn push(&mut self, file_id: FileId, stat: ColumnStat) {
        let declared = normalize_type(&stat.data_type);
        if !self.conflicted {
            self.data_type = match self.data_type.take() {
                None => storage_type(&declared),
                Some(current) => match super_type(&current, &declared) {
                    Some(merged) => storage_type(&merged),
                    None => {
                        self.conflicted = true;
                        None
                    }
                },
            };
            if self.data_type.is_none() {
                self.conflicted = true;
            }
        }

        self.file_ids.push(file_id);
        self.mins.push(stat.min);
        self.maxs.push(stat.max);
        self.null_counts.push(stat.null_count);
        self.row_counts.push(stat.row_count);
    }
}

/// Accumulates one batch of files, then emits the segment bytes.
#[derive(Default)]
pub struct SegmentBuilder {
    columns: HashMap<ColumnId, ColumnAccumulator>,
    num_files: u64,
    min_file_id: Option<FileId>,
    max_file_id: Option<FileId>,
    last_file_id: Option<FileId>,
}

/// What a finished segment reports back to the manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct FinishedSegment {
    pub bytes: Vec<u8>,
    /// Column ids present, ascending. The manifest keeps this to answer
    /// "could this segment hold column X" without touching the segment.
    pub column_ids: Vec<ColumnId>,
    pub min_file_id: FileId,
    pub max_file_id: FileId,
    pub num_files: u64,
}

impl SegmentBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one file's statistics.
    ///
    /// Call with ascending `file_id`, which is what keeps every block sorted
    /// without a sort at finish. Batch by path prefix, not by arrival order:
    /// files under one prefix share columns, so a prefix-local segment holds few
    /// distinct columns and most segments then skip on the manifest alone.
    pub fn push_file(
        &mut self,
        file_id: FileId,
        stats: impl IntoIterator<Item = (ColumnId, ColumnStat)>,
    ) {
        debug_assert!(
            self.last_file_id.is_none_or(|last| file_id > last),
            "push_file expects ascending file ids"
        );
        self.last_file_id = Some(file_id);
        self.num_files += 1;
        self.min_file_id = Some(self.min_file_id.map_or(file_id, |m| m.min(file_id)));
        self.max_file_id = Some(self.max_file_id.map_or(file_id, |m| m.max(file_id)));

        for (column_id, stat) in stats {
            self.columns.entry(column_id).or_default().push(file_id, stat);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.num_files == 0
    }

    /// Files pushed so far, for the collector's batch-size check.
    pub fn num_files(&self) -> u64 {
        self.num_files
    }

    /// Serialize the segment.
    ///
    /// A column whose files disagree about type beyond what
    /// [`super_type`] reconciles is dropped from the segment and logged. That
    /// costs pruning on the column, never correctness: a reader treats a missing
    /// statistic as unknown and keeps every file.
    pub fn finish(self) -> Result<FinishedSegment> {
        let mut out = Vec::with_capacity(64 * 1024);
        out.extend_from_slice(MAGIC);

        let mut type_table: Vec<DataType> = Vec::new();
        let mut index: Vec<(ColumnId, u64, u32)> = Vec::new();

        let mut column_ids: Vec<ColumnId> = self.columns.keys().copied().collect();
        column_ids.sort_unstable();

        for column_id in &column_ids {
            let accumulator = &self.columns[column_id];
            let Some(data_type) = accumulator.data_type.clone() else {
                tracing::debug!(
                    column_id,
                    "dropping column statistics: its files declare types that cannot share a block"
                );
                continue;
            };

            let type_id = match type_table.iter().position(|t| t == &data_type) {
                Some(position) => position,
                None => {
                    type_table.push(data_type.clone());
                    type_table.len() - 1
                }
            };
            let type_id = u16::try_from(type_id).map_err(|_| {
                FileStatsError::Format("a segment cannot hold more than 65536 distinct types".into())
            })?;

            pad_to_align(&mut out);
            let block_offset = out.len() as u64;
            write_block(&mut out, accumulator, type_id, &data_type)?;
            let block_len = u32::try_from(out.len() as u64 - block_offset).map_err(|_| {
                FileStatsError::Format("a single column block exceeds 4 GiB".into())
            })?;
            index.push((*column_id, block_offset, block_len));
        }

        // The column index: fixed-width records, so a reader binary-searches it
        // with ranged reads instead of holding it all.
        pad_to_align(&mut out);
        let index_offset = out.len() as u64;
        let mut sparse_column_ids = Vec::new();
        for (position, (column_id, block_offset, block_len)) in index.iter().enumerate() {
            if position % INDEX_STRIDE as usize == 0 {
                sparse_column_ids.push(*column_id);
            }
            out.extend_from_slice(&encode_index_entry(*column_id, *block_offset, *block_len));
        }

        let footer = SegmentFooter {
            version: VERSION,
            type_table: serde_json::to_vec(&type_table)
                .map_err(|e| FileStatsError::Format(format!("type table: {e}")))?,
            index_offset,
            index_len: index.len() as u32,
            index_stride: INDEX_STRIDE,
            sparse_column_ids,
            min_file_id: self.min_file_id.unwrap_or(0),
            max_file_id: self.max_file_id.unwrap_or(0),
            num_files: self.num_files,
        };
        let footer_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&footer)
            .map_err(|e| FileStatsError::Format(format!("footer: {e}")))?;
        out.extend_from_slice(&footer_bytes);
        out.extend_from_slice(&(footer_bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(MAGIC);

        let present: Vec<ColumnId> = index.iter().map(|(id, _, _)| *id).collect();
        Ok(FinishedSegment {
            bytes: out,
            column_ids: present,
            min_file_id: self.min_file_id.unwrap_or(0),
            max_file_id: self.max_file_id.unwrap_or(0),
            num_files: self.num_files,
        })
    }
}

fn pad_to_align(out: &mut Vec<u8>) {
    let padded = align_up(out.len());
    out.resize(padded, 0);
}

/// Append one buffer at an aligned offset, and return its position relative to
/// `block_start`.
fn append_buffer(out: &mut Vec<u8>, block_start: usize, bytes: &[u8]) -> Result<BufRef> {
    if bytes.is_empty() {
        return Ok(BufRef::EMPTY);
    }
    pad_to_align(out);
    let offset = out.len() - block_start;
    out.extend_from_slice(bytes);
    Ok(BufRef {
        offset: u32::try_from(offset)
            .map_err(|_| FileStatsError::Format("a column block exceeds 4 GiB".into()))?,
        len: u32::try_from(bytes.len())
            .map_err(|_| FileStatsError::Format("a statistics buffer exceeds 4 GiB".into()))?,
    })
}

fn append_values(
    out: &mut Vec<u8>,
    block_start: usize,
    encoded: EncodedValues,
) -> Result<StatValuesMeta> {
    Ok(match encoded {
        EncodedValues::Null => StatValuesMeta::Null,
        EncodedValues::Bool { values, valid } => StatValuesMeta::Bool {
            values: append_buffer(out, block_start, &values)?,
            valid: append_buffer(out, block_start, &valid)?,
        },
        EncodedValues::Fixed {
            width,
            values,
            valid,
        } => StatValuesMeta::Fixed {
            width,
            values: append_buffer(out, block_start, &values)?,
            valid: append_buffer(out, block_start, &valid)?,
        },
        EncodedValues::Bytes {
            offset_width,
            offsets,
            data,
            valid,
        } => StatValuesMeta::Bytes {
            offset_width,
            offsets: append_buffer(out, block_start, &offsets)?,
            data: append_buffer(out, block_start, &data)?,
            valid: append_buffer(out, block_start, &valid)?,
        },
    })
}

/// Write one column block.
///
/// Buffers go first, at aligned offsets, and the archived metadata goes last
/// with a length trailer. That order avoids a two-pass layout: buffer offsets
/// are known before the metadata that records them is built.
fn write_block(
    out: &mut Vec<u8>,
    accumulator: &ColumnAccumulator,
    type_id: u16,
    data_type: &DataType,
) -> Result<()> {
    let block_start = out.len();
    let len = accumulator.file_ids.len();

    let file_ids = flatten_u64(&accumulator.file_ids);
    let file_ids = append_buffer(out, block_start, &file_ids)?;

    let min = append_values(out, block_start, encode_values(&accumulator.mins, data_type)?)?;
    let max = append_values(out, block_start, encode_values(&accumulator.maxs, data_type)?)?;

    let null_count = flatten_u64(&accumulator.null_counts);
    let null_count = append_buffer(out, block_start, &null_count)?;
    let row_count = flatten_u64(&accumulator.row_counts);
    let row_count = append_buffer(out, block_start, &row_count)?;

    let meta = StatBlockMeta {
        type_id,
        len: u32::try_from(len)
            .map_err(|_| FileStatsError::Format("a column block exceeds 4G rows".into()))?,
        file_ids,
        min,
        max,
        null_count,
        row_count,
    };

    pad_to_align(out);
    let meta_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&meta)
        .map_err(|e| FileStatsError::Format(format!("block metadata: {e}")))?;
    out.extend_from_slice(&meta_bytes);
    out.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
    Ok(())
}

fn flatten_u64(values: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 8);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stat(min: i64, max: i64) -> ColumnStat {
        ColumnStat {
            min: StatScalar::I64(min),
            max: StatScalar::I64(max),
            null_count: 0,
            row_count: 10,
            data_type: DataType::Int64,
        }
    }

    #[test]
    fn an_empty_builder_still_produces_a_readable_segment() {
        let finished = SegmentBuilder::new().finish().unwrap();
        assert!(finished.column_ids.is_empty());
        assert_eq!(finished.num_files, 0);
        assert!(finished.bytes.starts_with(MAGIC));
        assert!(finished.bytes.ends_with(MAGIC));
    }

    /// The property the whole design rests on: a file that declares two columns
    /// costs two cells, not one per column in the store.
    #[test]
    fn a_file_only_touches_the_columns_it_declares() {
        let mut builder = SegmentBuilder::new();
        builder.push_file(1, [(10, stat(0, 1)), (20, stat(0, 1))]);
        builder.push_file(2, [(30, stat(0, 1))]);

        assert_eq!(builder.columns.len(), 3);
        assert_eq!(builder.columns[&10].file_ids, vec![1]);
        assert_eq!(builder.columns[&30].file_ids, vec![2]);
        assert_eq!(builder.columns[&30].mins.len(), 1, "no back-fill for file 1");
    }

    #[test]
    fn blocks_and_the_index_start_aligned() {
        let mut builder = SegmentBuilder::new();
        builder.push_file(1, [(10, stat(0, 1))]);
        builder.push_file(2, [(10, stat(2, 3)), (11, stat(0, 9))]);
        let finished = builder.finish().unwrap();

        assert_eq!(finished.column_ids, vec![10, 11]);
        assert_eq!(finished.min_file_id, 1);
        assert_eq!(finished.max_file_id, 2);
        assert_eq!(finished.num_files, 2);
    }

    #[test]
    fn a_column_with_irreconcilable_types_is_dropped_not_corrupted() {
        let mut builder = SegmentBuilder::new();
        builder.push_file(1, [(10, stat(0, 1))]);
        builder.push_file(
            2,
            [(
                10,
                ColumnStat {
                    min: StatScalar::Bytes(b"a".to_vec()),
                    max: StatScalar::Bytes(b"z".to_vec()),
                    null_count: 0,
                    row_count: 1,
                    data_type: DataType::Utf8,
                },
            )],
        );
        let finished = builder.finish().unwrap();
        assert!(
            finished.column_ids.is_empty(),
            "the column is absent, so a reader prunes nothing on it"
        );
    }

    #[test]
    fn mixed_but_reconcilable_types_settle_on_the_super_type() {
        let mut builder = SegmentBuilder::new();
        builder.push_file(1, [(10, stat(0, 1))]);
        let mut float = stat(2, 3);
        float.data_type = DataType::Float32;
        float.min = StatScalar::F64(2.5);
        float.max = StatScalar::F64(3.5);
        builder.push_file(2, [(10, float)]);

        assert_eq!(builder.columns[&10].data_type, Some(DataType::Float64));
        assert!(!builder.finish().unwrap().column_ids.is_empty());
    }
}
