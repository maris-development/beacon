use std::{ffi::CStr, sync::Arc};

use arrow::array::Scalar;
use netcdf::types::NcVariableType;

use crate::decoders::VariableDecoder;

#[derive(Debug)]
pub struct StringVariableDecoder {
    pub arrow_field: arrow::datatypes::Field,
    pub nc_type: NcVariableType,
    pub fill_value: Option<arrow::array::ArrayRef>,
    pub string_length: Option<usize>,
}

impl VariableDecoder for StringVariableDecoder {
    fn read(
        &self,
        variable: &netcdf::Variable,
        extents: netcdf::Extents,
    ) -> anyhow::Result<arrow::array::ArrayRef> {
        match self.nc_type {
            NcVariableType::Char => {
                let array = variable.get_raw_values(extents)?;
                // the last dimension of the extents is the string length, so we need to reshape the array to get the strings
                let string_length = self.string_length.ok_or_else(|| {
                    anyhow::anyhow!("String length must be provided for char variables using the string variable decoder")
                })?;

                let num_strings = array.len() / string_length;
                let iter = (0..num_strings).map(|i| {
                    let start = i * string_length;
                    let end = start + string_length;
                    let bytes = &array[start..end];
                    let string = CStr::from_bytes_until_nul(bytes).ok()?.to_str().ok()?;

                    // Trim trailing whitespace
                    let trimmed_string = string.trim().to_string();
                    Some(trimmed_string)
                });
                let arrow_array = arrow::array::StringArray::from_iter(iter);

                if let Some(fill_array) = &self.fill_value {
                    self.mask_fill_values(Arc::new(arrow_array), Scalar::new(fill_array.clone()))
                } else {
                    Ok(Arc::new(arrow_array))
                }
            }
            NcVariableType::String => {
                let string_values = variable.get_strings(extents)?;
                let arrow_array = arrow::array::StringArray::from(string_values);
                if let Some(fill_array) = &self.fill_value {
                    self.mask_fill_values(Arc::new(arrow_array), Scalar::new(fill_array.clone()))
                } else {
                    Ok(Arc::new(arrow_array))
                }
            }
            _ => Err(anyhow::anyhow!(
                "Unsupported variable type for string decoder: {:?} on variable {}",
                self.nc_type,
                self.arrow_field.name()
            )),
        }
    }

    fn variable_name(&self) -> &str {
        self.arrow_field.name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoders::VariableDecoder;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field};
    use netcdf::types::NcVariableType;
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
            arrow_field: Field::new(var_name, DataType::Utf8, true),
            nc_type: NcVariableType::Char,
            fill_value: None,
            string_length: Some(string_len),
        };

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("StringVariableDecoder::read (char) failed");

        assert_eq!(array.len(), strings.len());
        let sa = array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray");
        assert_eq!(sa.value(0), "hello");
        assert_eq!(sa.value(1), "world");
        assert_eq!(sa.value(2), "foo");
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
            arrow_field: Field::new(var_name, DataType::Utf8, true),
            nc_type: NcVariableType::Char,
            fill_value: None,
            string_length: Some(string_len),
        };

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("StringVariableDecoder::read (char trimming) failed");

        let sa = array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray");
        assert_eq!(sa.value(0), "hi", "Trailing whitespace should be trimmed");
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
            arrow_field: Field::new(var_name, DataType::Utf8, true),
            nc_type: NcVariableType::Char,
            fill_value: None,
            string_length: Some(string_len),
        };

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("StringVariableDecoder::read (empty string) failed");

        let sa = array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray");
        assert_eq!(sa.value(0), "", "All-null row should produce empty string");
        assert_eq!(sa.value(1), "ab");
    }

    #[test]
    fn test_string_decoder_char_missing_string_length_errors() {
        let var_name = "s";
        let tmp = write_char_nc(var_name, &["x"], 4);

        let file = netcdf::open(tmp.path()).unwrap();
        let variable = file.variable(var_name).unwrap();

        let decoder = StringVariableDecoder {
            arrow_field: Field::new(var_name, DataType::Utf8, true),
            nc_type: NcVariableType::Char,
            fill_value: None,
            string_length: None, // deliberately omitted
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
            arrow_field: Field::new(var_name, DataType::Utf8, true),
            nc_type: NcVariableType::String,
            fill_value: None,
            string_length: None,
        };

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("StringVariableDecoder::read (NcString) failed");

        assert_eq!(array.len(), input.len());
        let sa = array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray");
        assert_eq!(sa.value(0), "alpha");
        assert_eq!(sa.value(1), "beta");
        assert_eq!(sa.value(2), "gamma");
    }

    // ── variable_name ──────────────────────────────────────────────────────

    #[test]
    fn test_string_decoder_variable_name() {
        let decoder = StringVariableDecoder {
            arrow_field: Field::new("station_name", DataType::Utf8, true),
            nc_type: NcVariableType::Char,
            fill_value: None,
            string_length: Some(10),
        };
        assert_eq!(decoder.variable_name(), "station_name");
    }
}
