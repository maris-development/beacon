//! Turning [`StatScalar`]s into Arrow buffers, and back.
//!
//! The write side flattens a column's values into the narrow physical form the
//! block's type calls for. The read side rebuilds an Arrow array over the bytes
//! the object store returned, without a copy whenever the allocator's base
//! address permits it.

use arrow::array::{ArrayData, ArrayRef, make_array, new_null_array};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use bytes::Bytes;

use crate::error::{FileStatsError, Result};
use crate::scalar::StatScalar;

use super::format::{ALIGN, BufRef, StatValuesMeta};

/// Collapse the view and half-width types onto their storage equivalents.
///
/// Keeping `Utf8View` and `Float16` out of the on-disk type set costs nothing
/// (a statistic is one scalar, so the view layout buys no scan speed) and keeps
/// the encoder's match small.
pub fn normalize_type(t: &DataType) -> DataType {
    match t {
        DataType::Float16 => DataType::Float64,
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        other => other.clone(),
    }
}

/// A column's values, encoded but not yet placed in a segment.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EncodedValues {
    Null,
    Bool {
        values: Vec<u8>,
        valid: Vec<u8>,
    },
    Fixed {
        width: u8,
        values: Vec<u8>,
        valid: Vec<u8>,
    },
    Bytes {
        offset_width: u8,
        offsets: Vec<u8>,
        data: Vec<u8>,
        valid: Vec<u8>,
    },
}

/// Packs an Arrow validity bitmap, LSB first.
///
/// Returns an empty vector when every value is valid, which the block meta
/// records as "no null buffer".
pub(crate) struct ValidityBuilder {
    bits: Vec<u8>,
    len: usize,
    any_null: bool,
}

impl ValidityBuilder {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            bits: vec![0u8; capacity.div_ceil(8)],
            len: 0,
            any_null: false,
        }
    }

    pub(crate) fn push(&mut self, valid: bool) {
        if valid {
            self.bits[self.len / 8] |= 1 << (self.len % 8);
        } else {
            self.any_null = true;
        }
        self.len += 1;
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        if self.any_null { self.bits } else { Vec::new() }
    }
}

/// The physical width of a fixed-size statistics type.
fn fixed_width(t: &DataType) -> Option<u8> {
    use DataType::*;
    Some(match t {
        Int8 | UInt8 => 1,
        Int16 | UInt16 => 2,
        Int32 | UInt32 | Float32 | Date32 | Time32(_) => 4,
        Int64 | UInt64 | Float64 | Date64 | Time64(_) | Timestamp(_, _) | Duration(_) => 8,
        Decimal128(_, _) => 16,
        _ => return None,
    })
}

/// Encode one scalar into the target type's little-endian form.
///
/// `None` means the value carries no statistic for this type, and the caller
/// records it as null. Every narrowing here is exact by construction:
/// [`super_type`](crate::scalar::super_type) only picks a narrow type when every
/// contribution already had it.
fn encode_fixed(value: &StatScalar, target: &DataType) -> Option<Vec<u8>> {
    use DataType::*;
    use StatScalar as S;

    Some(match (target, value) {
        (Int8, S::I64(x)) => (*x as i8).to_le_bytes().to_vec(),
        (Int16, S::I64(x)) => (*x as i16).to_le_bytes().to_vec(),
        (Int32 | Date32 | Time32(_), S::I64(x)) => (*x as i32).to_le_bytes().to_vec(),
        (Int64 | Date64 | Time64(_) | Timestamp(_, _) | Duration(_), S::I64(x)) => {
            x.to_le_bytes().to_vec()
        }
        // A `UInt8`/`UInt16`/`UInt32` contribution in a signed block always fits.
        (Int64 | Date64 | Time64(_) | Timestamp(_, _) | Duration(_), S::U64(x)) => {
            (*x as i64).to_le_bytes().to_vec()
        }
        (Int32, S::U64(x)) => (*x as i32).to_le_bytes().to_vec(),

        (UInt8, S::U64(x)) => (*x as u8).to_le_bytes().to_vec(),
        (UInt16, S::U64(x)) => (*x as u16).to_le_bytes().to_vec(),
        (UInt32, S::U64(x)) => (*x as u32).to_le_bytes().to_vec(),
        (UInt64, S::U64(x)) => x.to_le_bytes().to_vec(),

        (Float32, S::F64(x)) => (*x as f32).to_le_bytes().to_vec(),
        (Float64, S::F64(x)) => x.to_le_bytes().to_vec(),
        (Float64, S::I64(x)) => (*x as f64).to_le_bytes().to_vec(),
        (Float64, S::U64(x)) => (*x as f64).to_le_bytes().to_vec(),

        (Decimal128(_, _), S::I128(x)) => x.to_le_bytes().to_vec(),

        _ => return None,
    })
}

/// Flatten a column's values into the block's physical form.
pub(crate) fn encode_values(values: &[StatScalar], target: &DataType) -> Result<EncodedValues> {
    match target {
        DataType::Null => Ok(EncodedValues::Null),

        DataType::Boolean => {
            let mut bits = vec![0u8; values.len().div_ceil(8)];
            let mut valid = ValidityBuilder::new(values.len());
            for (i, value) in values.iter().enumerate() {
                match value {
                    StatScalar::Bool(b) => {
                        if *b {
                            bits[i / 8] |= 1 << (i % 8);
                        }
                        valid.push(true);
                    }
                    _ => valid.push(false),
                }
            }
            Ok(EncodedValues::Bool {
                values: bits,
                valid: valid.finish(),
            })
        }

        DataType::Utf8 | DataType::Binary => encode_bytes(values, 4),
        DataType::LargeUtf8 | DataType::LargeBinary => encode_bytes(values, 8),

        other => {
            let width = fixed_width(other).ok_or_else(|| {
                FileStatsError::Format(format!("no statistics encoding for {other}"))
            })?;
            let mut out = Vec::with_capacity(values.len() * width as usize);
            let mut valid = ValidityBuilder::new(values.len());
            for value in values {
                match encode_fixed(value, other) {
                    Some(bytes) => {
                        out.extend_from_slice(&bytes);
                        valid.push(true);
                    }
                    None => {
                        out.resize(out.len() + width as usize, 0);
                        valid.push(false);
                    }
                }
            }
            Ok(EncodedValues::Fixed {
                width,
                values: out,
                valid: valid.finish(),
            })
        }
    }
}

fn encode_bytes(values: &[StatScalar], offset_width: u8) -> Result<EncodedValues> {
    let mut data: Vec<u8> = Vec::new();
    let mut offsets: Vec<u8> = Vec::with_capacity((values.len() + 1) * offset_width as usize);
    let mut valid = ValidityBuilder::new(values.len());

    let push_offset = |offsets: &mut Vec<u8>, position: usize| -> Result<()> {
        if offset_width == 4 {
            let narrowed: i32 = position.try_into().map_err(|_| {
                FileStatsError::Format("statistics values exceed a 32-bit offset".into())
            })?;
            offsets.extend_from_slice(&narrowed.to_le_bytes());
        } else {
            offsets.extend_from_slice(&(position as i64).to_le_bytes());
        }
        Ok(())
    };

    push_offset(&mut offsets, 0)?;
    for value in values {
        match value {
            StatScalar::Bytes(bytes) => {
                data.extend_from_slice(bytes);
                valid.push(true);
            }
            _ => valid.push(false),
        }
        push_offset(&mut offsets, data.len())?;
    }

    Ok(EncodedValues::Bytes {
        offset_width,
        offsets,
        data,
        valid: valid.finish(),
    })
}

// ── read side ───────────────────────────────────────────────────────────────

/// View a block's byte range as an Arrow buffer.
///
/// Zero-copy when the slice happens to sit on an [`ALIGN`] boundary in memory.
/// The writer aligns every buffer inside the segment, but the base address of
/// the `Bytes` an object store returns belongs to the allocator, so the check
/// has to happen at read time. A copy here is correct, just slower.
fn buffer_from(block: &Bytes, reference: &BufRef) -> Buffer {
    let offset = reference.offset as usize;
    let len = reference.len as usize;
    let slice = &block[offset..offset + len];
    if slice.as_ptr().align_offset(ALIGN) == 0 {
        Buffer::from(block.slice(offset..offset + len))
    } else {
        Buffer::from_slice_ref(slice)
    }
}

/// Rebuild one min or max column as an Arrow array of `data_type`.
pub(crate) fn decode_values(
    meta: &StatValuesMeta,
    len: usize,
    data_type: &DataType,
    block: &Bytes,
) -> Result<ArrayRef> {
    let (buffers, valid) = match meta {
        StatValuesMeta::Null => return Ok(new_null_array(data_type, len)),
        StatValuesMeta::Bool { values, valid } => (vec![buffer_from(block, values)], valid),
        StatValuesMeta::Fixed { values, valid, .. } => (vec![buffer_from(block, values)], valid),
        StatValuesMeta::Bytes {
            offsets,
            data,
            valid,
            ..
        } => (
            vec![buffer_from(block, offsets), buffer_from(block, data)],
            valid,
        ),
    };

    let mut builder = ArrayData::builder(data_type.clone()).len(len);
    for buffer in buffers {
        builder = builder.add_buffer(buffer);
    }
    if !valid.is_empty() {
        builder = builder.null_bit_buffer(Some(buffer_from(block, valid)));
    }
    Ok(make_array(builder.build()?))
}

/// Read a `u64` little-endian buffer out of a block.
pub(crate) fn decode_u64s(reference: &BufRef, block: &Bytes) -> Vec<u64> {
    let offset = reference.offset as usize;
    let len = reference.len as usize;
    block[offset..offset + len]
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

/// Read a count column as a nullable Arrow array.
///
/// An empty `valid` means every entry is known, which is the common case and
/// costs no bytes to say.
pub(crate) fn decode_counts(
    values: &BufRef,
    valid: &BufRef,
    len: usize,
    block: &Bytes,
) -> Result<ArrayRef> {
    let mut builder = ArrayData::builder(DataType::UInt64)
        .len(len)
        .add_buffer(buffer_from(block, values));
    if !valid.is_empty() {
        builder = builder.null_bit_buffer(Some(buffer_from(block, valid)));
    }
    Ok(make_array(builder.build()?))
}

/// Pack `Option<u64>` counts into a value buffer plus a validity bitmap.
pub(crate) fn encode_counts(counts: &[Option<u64>]) -> (Vec<u8>, Vec<u8>) {
    let mut values = Vec::with_capacity(counts.len() * 8);
    let mut valid = ValidityBuilder::new(counts.len());
    for count in counts {
        values.extend_from_slice(&count.unwrap_or(0).to_le_bytes());
        valid.push(count.is_some());
    }
    (values, valid.finish())
}

/// The declared statistics type after normalization, or `None` when this crate
/// keeps no range for it.
pub fn storage_type(t: &DataType) -> Option<DataType> {
    let normalized = normalize_type(t);
    match &normalized {
        DataType::Boolean | DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary => {
            Some(normalized)
        }
        other if fixed_width(other).is_some() => Some(normalized),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validity_is_empty_when_nothing_is_null() {
        let mut builder = ValidityBuilder::new(3);
        builder.push(true);
        builder.push(true);
        builder.push(true);
        assert!(builder.finish().is_empty());
    }

    #[test]
    fn validity_packs_lsb_first() {
        let mut builder = ValidityBuilder::new(3);
        builder.push(true);
        builder.push(false);
        builder.push(true);
        assert_eq!(builder.finish(), vec![0b0000_0101]);
    }

    #[test]
    fn narrowing_to_a_small_int_is_exact() {
        let encoded = encode_fixed(&StatScalar::I64(-7), &DataType::Int16).unwrap();
        assert_eq!(i16::from_le_bytes(encoded.try_into().unwrap()), -7);
    }

    #[test]
    fn an_incompatible_scalar_encodes_as_absent() {
        assert!(encode_fixed(&StatScalar::Bytes(vec![1]), &DataType::Int64).is_none());
        assert!(encode_fixed(&StatScalar::Absent, &DataType::Int64).is_none());
    }

    #[test]
    fn fixed_encoding_pads_absent_slots_and_marks_them_null() {
        let values = vec![StatScalar::I64(1), StatScalar::Absent, StatScalar::I64(3)];
        let encoded = encode_values(&values, &DataType::Int32).unwrap();
        match encoded {
            EncodedValues::Fixed { width, values, valid } => {
                assert_eq!(width, 4);
                assert_eq!(values.len(), 12, "the absent slot still occupies its width");
                assert_eq!(valid, vec![0b0000_0101]);
            }
            other => panic!("expected a fixed encoding, got {other:?}"),
        }
    }

    #[test]
    fn byte_encoding_writes_len_plus_one_offsets() {
        let values = vec![StatScalar::Bytes(b"ab".to_vec()), StatScalar::Absent];
        let encoded = encode_values(&values, &DataType::Utf8).unwrap();
        match encoded {
            EncodedValues::Bytes { offset_width, offsets, data, valid } => {
                assert_eq!(offset_width, 4);
                assert_eq!(offsets.len(), 3 * 4);
                assert_eq!(data, b"ab");
                assert_eq!(valid, vec![0b0000_0001]);
            }
            other => panic!("expected a bytes encoding, got {other:?}"),
        }
    }

    #[test]
    fn view_and_half_types_normalize_to_their_storage_form() {
        assert_eq!(normalize_type(&DataType::Utf8View), DataType::Utf8);
        assert_eq!(normalize_type(&DataType::Float16), DataType::Float64);
        assert_eq!(storage_type(&DataType::Utf8View), Some(DataType::Utf8));
        assert_eq!(storage_type(&DataType::Null), None);
    }
}
