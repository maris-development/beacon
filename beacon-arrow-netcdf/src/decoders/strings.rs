//! String decoding for NetCDF `String` and fixed-size char arrays.

use std::{
    ffi::{CStr, CString},
    sync::Arc,
};

use arrow::array::Scalar;
use beacon_nd_arrow::array::compat_typings::ArrowTypeConversion;
use ndarray::Axis;
use netcdf::{types::NcVariableType, Extent, NcTypeDescriptor};

use crate::{decoders::VariableDecoder, NcChar, OwnedNcString};

/// Decoder for string-like NetCDF variables.
///
/// When `fixed_sized_string` is `Some(len)`, the decoder expects the source
/// variable to store bytes in a trailing string-length dimension.
#[derive(Debug)]
pub struct StringVariableDecoder {
    /// Arrow field metadata used by downstream array wrappers.
    pub arrow_field: arrow::datatypes::FieldRef,
    /// Optional fill value for missing strings.
    pub fill_value: Option<OwnedNcString>,
    /// Fixed string length for char-array decoding.
    pub fixed_sized_string: Option<usize>,
}

impl StringVariableDecoder {
    /// Construct a string decoder.
    pub fn new(
        arrow_field: arrow::datatypes::FieldRef,
        fill_value: Option<OwnedNcString>,
        fixed_sized_string: Option<usize>,
    ) -> Self {
        Self {
            arrow_field,
            fill_value,
            fixed_sized_string,
        }
    }
}

impl VariableDecoder<OwnedNcString> for StringVariableDecoder {
    fn arrow_field(&self) -> &arrow::datatypes::Field {
        &self.arrow_field
    }

    fn read(
        &self,
        variable: &netcdf::Variable,
        extents: netcdf::Extents,
    ) -> anyhow::Result<ndarray::ArrayD<OwnedNcString>> {
        match self.fixed_sized_string {
            Some(length) => match extents {
                netcdf::Extents::All => {
                    return anyhow::bail!("Fixed-size string decoding requires explicit extents to specify the string length dimension. Got Extents::All.");
                }
                netcdf::Extents::Extent(extents) => {
                    // Add dimension for string length
                    let mut full_extents = extents.clone();
                    full_extents.push(Extent::from(..length));
                    // Read the variable data as a 2D array of shape [num_strings, string_length]
                    let array = variable.get::<NcChar, _>(full_extents)?;

                    let ndim = array.ndim();

                    let string_array = array.map_axis(Axis(ndim - 1), |slice| {
                        let nc_char_bytes: &[NcChar] = slice.as_slice().unwrap_or(&[]);
                        let char_bytes: &[u8] = bytemuck::cast_slice(nc_char_bytes);
                        OwnedNcString(
                            CStr::from_bytes_until_nul(char_bytes)
                                .unwrap()
                                .to_string_lossy()
                                .trim()
                                .to_string(),
                        )
                    });

                    return Ok(string_array);
                }
            },
            None => {
                let dims = variable.dimensions();
                let shape: Vec<usize> = match &extents {
                    netcdf::Extents::All => variable.dimensions().iter().map(|d| d.len()).collect(),
                    netcdf::Extents::Extent(extents) => extents
                        .iter()
                        .map(|e| match e {
                            Extent::Slice { start, stride } => todo!(),
                            Extent::SliceEnd { start, end, stride } => todo!(),
                            Extent::SliceCount {
                                start,
                                count,
                                stride,
                            } => todo!(),
                            Extent::Index(_) => todo!(),
                        })
                        .collect(),
                };
                let array = variable.get_strings(extents)?;
                let owned_array: Vec<OwnedNcString> =
                    array.into_iter().map(OwnedNcString).collect();
                let nd_array = ndarray::Array::from_shape_vec(shape, owned_array)?;

                return Ok(nd_array);
            }
        }
    }

    fn variable_name(&self) -> &str {
        self.arrow_field.name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{decoders::VariableDecoder, OwnedNcString};
    use arrow::datatypes::{DataType, Field};
    use tempfile::Builder;

    use crate::NcChar;

    // ── NcVariableType::Char (fixed-length char arrays) ────────────────────

    /// Create a NetCDF file with a 2-D char variable (obs × string_len).
    /// Each row holds a null-terminated, space-padded sequence of bytes.
    fn write_char_nc(
        var_name: &str,
        strings: &[&str],
        string_len: usize,
    ) -> tempfile::NamedTempFile {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        {
            let mut nc = netcdf::create(tmp.path()).unwrap();
            nc.add_dimension("obs", strings.len()).unwrap();
            let strlen_dim = format!("strlen{}", string_len);
            nc.add_dimension(&strlen_dim, string_len).unwrap();
            let mut var = nc
                .add_variable::<NcChar>(var_name, &["obs", &strlen_dim])
                .unwrap();

            // Build a buffer: each string occupies string_len bytes, null-padded.
            let mut buf: Vec<NcChar> = Vec::with_capacity(strings.len() * string_len);
            for s in strings {
                let bytes = s.as_bytes();
                for i in 0..string_len {
                    buf.push(NcChar(*bytes.get(i).unwrap_or(&b'\0')));
                }
            }
            var.put_values::<NcChar, _>(&buf, netcdf::Extents::All)
                .unwrap();
        }
        tmp
    }

    #[test]
    fn test_string_decoder_char_basic() {
        let var_name = "labels";
        let strings = ["hello", "world", "foo"];
        let string_len = 8;
        let tmp = write_char_nc(var_name, &strings, string_len);

        let file = netcdf::open(tmp.path()).unwrap();
        let variable = file.variable(var_name).unwrap();

        let decoder = StringVariableDecoder {
            arrow_field: Arc::new(Field::new(var_name, DataType::Utf8, true)),
            fill_value: None,
            fixed_sized_string: Some(string_len),
        };

        let array = decoder
            .read(
                &variable,
                netcdf::Extents::Extent(vec![Extent::from(..strings.len())]),
            )
            .expect("StringVariableDecoder::read (char) failed");

        assert_eq!(array.len(), strings.len());
        let values: Vec<OwnedNcString> = array.iter().cloned().collect();
        assert_eq!(values[0].0, "hello");
        assert_eq!(values[1].0, "world");
        assert_eq!(values[2].0, "foo");
    }

    #[test]
    fn test_string_decoder_char_trims_trailing_whitespace() {
        let var_name = "padded";
        // Manually write strings that contain trailing spaces before the null byte
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        let string_len: usize = 8;
        {
            let mut nc = netcdf::create(tmp.path()).unwrap();
            nc.add_dimension("obs", 1).unwrap();
            nc.add_dimension("strlen8", string_len).unwrap();
            let mut var = nc
                .add_variable::<NcChar>(var_name, &["obs", "strlen8"])
                .unwrap();
            // "hi   \0\0\0" — note the spaces before the null terminator
            let buf: Vec<NcChar> = b"hi   \0\0\0".iter().map(|&b| NcChar(b)).collect();
            var.put_values::<NcChar, _>(&buf, netcdf::Extents::All)
                .unwrap();
        }

        let file = netcdf::open(tmp.path()).unwrap();
        let variable = file.variable(var_name).unwrap();

        let decoder = StringVariableDecoder {
            arrow_field: Arc::new(Field::new(var_name, DataType::Utf8, true)),
            fill_value: None,
            fixed_sized_string: Some(string_len),
        };

        let array = decoder
            .read(&variable, netcdf::Extents::Extent(vec![Extent::from(..1)]))
            .expect("StringVariableDecoder::read (char trimming) failed");

        let values: Vec<OwnedNcString> = array.iter().cloned().collect();
        assert_eq!(values[0].0, "hi", "Trailing whitespace should be trimmed");
    }

    #[test]
    fn test_string_decoder_char_empty_string() {
        let var_name = "empties";
        // String that is all null bytes → should decode to empty string
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        let string_len: usize = 4;
        {
            let mut nc = netcdf::create(tmp.path()).unwrap();
            nc.add_dimension("obs", 2).unwrap();
            nc.add_dimension("strlen4", string_len).unwrap();
            let mut var = nc
                .add_variable::<NcChar>(var_name, &["obs", "strlen4"])
                .unwrap();
            // row 0: "\0\0\0\0"  row 1: "ab\0\0"
            let buf: Vec<NcChar> = b"\0\0\0\0ab\0\0".iter().map(|&b| NcChar(b)).collect();
            var.put_values::<NcChar, _>(&buf, netcdf::Extents::All)
                .unwrap();
        }

        let file = netcdf::open(tmp.path()).unwrap();
        let variable = file.variable(var_name).unwrap();

        let decoder = StringVariableDecoder {
            arrow_field: Arc::new(Field::new(var_name, DataType::Utf8, true)),
            fill_value: None,
            fixed_sized_string: Some(string_len),
        };

        let array = decoder
            .read(&variable, netcdf::Extents::Extent(vec![Extent::from(..2)]))
            .expect("StringVariableDecoder::read (empty string) failed");

        let values: Vec<OwnedNcString> = array.iter().cloned().collect();
        assert_eq!(values[0].0, "", "All-null row should produce empty string");
        assert_eq!(values[1].0, "ab");
    }

    #[test]
    fn test_string_decoder_char_missing_string_length_errors() {
        let var_name = "s";
        let tmp = write_char_nc(var_name, &["x"], 4);

        let file = netcdf::open(tmp.path()).unwrap();
        let variable = file.variable(var_name).unwrap();

        let decoder = StringVariableDecoder {
            arrow_field: Arc::new(Field::new(var_name, DataType::Utf8, true)),
            fill_value: None,
            fixed_sized_string: None, // deliberately omitted
        };

        let result = decoder.read(&variable, netcdf::Extents::All);
        assert!(
            result.is_err(),
            "Expected an error when string_length is None"
        );
    }

    // ── NcVariableType::String (native NetCDF strings) ─────────────────────

    #[test]
    fn test_string_decoder_nc_string_type() {
        use crate::NcString;

        let var_name = "names";
        let input = ["alpha", "beta", "gamma"];
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        {
            let mut nc = netcdf::create(tmp.path()).unwrap();
            nc.add_dimension("obs", input.len()).unwrap();
            let mut var = nc.add_variable::<NcString>(var_name, &["obs"]).unwrap();
            let nc_strings: Vec<NcString> = input.iter().map(|s| NcString::new(s)).collect();
            var.put_values::<NcString, _>(&nc_strings, netcdf::Extents::All)
                .unwrap();
        }

        let file = netcdf::open(tmp.path()).unwrap();
        let variable = file.variable(var_name).unwrap();

        let decoder = StringVariableDecoder {
            arrow_field: Arc::new(Field::new(var_name, DataType::Utf8, true)),
            fill_value: None,
            fixed_sized_string: None,
        };

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("StringVariableDecoder::read (NcString) failed");

        assert_eq!(array.len(), input.len());
        let values: Vec<OwnedNcString> = array.iter().cloned().collect();
        assert_eq!(values[0].0, "alpha");
        assert_eq!(values[1].0, "beta");
        assert_eq!(values[2].0, "gamma");
    }

    // ── variable_name ──────────────────────────────────────────────────────

    #[test]
    fn test_string_decoder_variable_name() {
        let decoder = StringVariableDecoder {
            arrow_field: Arc::new(Field::new("station_name", DataType::Utf8, true)),
            fill_value: None,
            fixed_sized_string: Some(10),
        };
        assert_eq!(decoder.variable_name(), "station_name");
    }
}
