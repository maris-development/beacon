//! XDR rows of a DAP2 Sequence.

use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int32Array, StringArray, UInt32Array,
};
use bytes::{BufMut, BytesMut};

use crate::error::DapError;

/// Marks the start of each Sequence row.
pub const START_OF_INSTANCE: [u8; 4] = [0x5A, 0, 0, 0];
/// Closes the Sequence.
pub const END_OF_SEQUENCE: [u8; 4] = [0xA5, 0, 0, 0];

enum Column<'a> {
    Int32(&'a Int32Array),
    UInt32(&'a UInt32Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    String(&'a StringArray),
}

/// Appends one Sequence row for each row of `columns`.
///
/// The columns come from `types::convert`. A null value becomes the fill value of its type.
/// XDR values are big-endian.
pub fn encode_rows(columns: &[ArrayRef], out: &mut BytesMut) -> Result<(), DapError> {
    let rows = columns.first().map_or(0, |array| array.len());
    let columns = columns.iter().map(column).collect::<Result<Vec<_>, _>>()?;
    for row in 0..rows {
        out.put_slice(&START_OF_INSTANCE);
        for column in &columns {
            match column {
                Column::Int32(a) => out.put_i32(if a.is_null(row) {
                    i32::MAX
                } else {
                    a.value(row)
                }),
                Column::UInt32(a) => out.put_u32(if a.is_null(row) {
                    u32::MAX
                } else {
                    a.value(row)
                }),
                Column::Float32(a) => out.put_f32(if a.is_null(row) {
                    f32::NAN
                } else {
                    a.value(row)
                }),
                Column::Float64(a) => out.put_f64(if a.is_null(row) {
                    f64::NAN
                } else {
                    a.value(row)
                }),
                Column::String(a) => {
                    put_string(out, if a.is_null(row) { "" } else { a.value(row) })
                }
            }
        }
    }
    Ok(())
}

/// An XDR string: a 4-byte byte length, the bytes, then zero bytes to a multiple of 4.
fn put_string(out: &mut BytesMut, text: &str) {
    out.put_u32(text.len() as u32);
    out.put_slice(text.as_bytes());
    out.put_bytes(0, (4 - text.len() % 4) % 4);
}

fn column(array: &ArrayRef) -> Result<Column<'_>, DapError> {
    let any = array.as_any();
    if let Some(a) = any.downcast_ref::<Int32Array>() {
        return Ok(Column::Int32(a));
    }
    if let Some(a) = any.downcast_ref::<UInt32Array>() {
        return Ok(Column::UInt32(a));
    }
    if let Some(a) = any.downcast_ref::<Float32Array>() {
        return Ok(Column::Float32(a));
    }
    if let Some(a) = any.downcast_ref::<Float64Array>() {
        return Ok(Column::Float64(a));
    }
    if let Some(a) = any.downcast_ref::<StringArray>() {
        return Ok(Column::String(a));
    }
    Err(DapError::Internal(format!(
        "the XDR writer cannot write {}",
        array.data_type()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use std::sync::Arc;

    fn encode(columns: Vec<ArrayRef>) -> Vec<u8> {
        let mut out = BytesMut::new();
        encode_rows(&columns, &mut out).unwrap();
        out.to_vec()
    }

    #[test]
    fn each_row_starts_with_the_instance_marker() {
        let bytes = encode(vec![Arc::new(Int32Array::from(vec![7, -1]))]);

        assert_eq!(
            bytes,
            [
                0x5A, 0, 0, 0, 0, 0, 0, 7, //
                0x5A, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF
            ]
        );
    }

    #[test]
    fn nulls_become_fill_values() {
        let bytes = encode(vec![
            Arc::new(Int32Array::from(vec![None])),
            Arc::new(UInt32Array::from(vec![None])),
            Arc::new(Float32Array::from(vec![None])),
            Arc::new(Float64Array::from(vec![None])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ]);

        let mut expected = vec![0x5A, 0, 0, 0];
        expected.extend(i32::MAX.to_be_bytes());
        expected.extend(u32::MAX.to_be_bytes());
        expected.extend(f32::NAN.to_be_bytes());
        expected.extend(f64::NAN.to_be_bytes());
        expected.extend([0, 0, 0, 0]);
        assert_eq!(bytes, expected);
    }

    #[test]
    fn floats_are_big_endian() {
        let bytes = encode(vec![
            Arc::new(Float32Array::from(vec![1.5])),
            Arc::new(Float64Array::from(vec![1.5])),
        ]);

        assert_eq!(
            bytes,
            [0x5A, 0, 0, 0, 0x3F, 0xC0, 0, 0, 0x3F, 0xF8, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn strings_have_a_length_and_pad_to_four_bytes() {
        let bytes = encode(vec![Arc::new(StringArray::from(vec!["abcde", "abcd", ""]))]);

        assert_eq!(
            bytes,
            [
                0x5A, 0, 0, 0, 0, 0, 0, 5, b'a', b'b', b'c', b'd', b'e', 0, 0, 0, //
                0x5A, 0, 0, 0, 0, 0, 0, 4, b'a', b'b', b'c', b'd', //
                0x5A, 0, 0, 0, 0, 0, 0, 0
            ]
        );
    }

    #[test]
    fn non_ascii_text_uses_the_byte_length() {
        let bytes = encode(vec![Arc::new(StringArray::from(vec!["é"]))]);

        assert_eq!(bytes, [0x5A, 0, 0, 0, 0, 0, 0, 2, 0xC3, 0xA9, 0, 0]);
    }

    #[test]
    fn rows_hold_the_columns_in_order() {
        let bytes = encode(vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ]);

        assert_eq!(
            bytes,
            [
                0x5A, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, b'a', 0, 0, 0, //
                0x5A, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, b'b', 0, 0, 0
            ]
        );
    }

    #[test]
    fn no_rows_write_no_bytes() {
        assert!(encode(vec![Arc::new(Int32Array::from(Vec::<i32>::new()))]).is_empty());
    }

    #[test]
    fn an_unconverted_array_is_an_internal_error() {
        let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1]))];

        let result = encode_rows(&columns, &mut BytesMut::new());

        assert!(matches!(result, Err(DapError::Internal(_))));
    }
}
