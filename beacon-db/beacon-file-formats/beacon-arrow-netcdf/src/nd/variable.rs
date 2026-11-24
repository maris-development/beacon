//! One NetCDF variable on the ND contract.

use std::sync::Arc;

use arrow::array::{
    new_empty_array, new_null_array, ArrayRef, PrimitiveArray, StringArray, UInt64Array,
};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use async_trait::async_trait;
use beacon_nd_dataset::{
    Dimension, Dimensions, Error, NdArrowArray, NdVariable, Region, Result, VariableInfo,
};
use indexmap::IndexMap;
use nd_arrow_array::Attributes;
use oxcdf::{AsyncNetcdfFile, AsyncVariable, DType, Extent, Extents};

use super::attributes::attributes_of;
use super::decode::Decode;
use crate::dimensions::PhonyDimensions;

/// How a string variable stores its text. The rules of `oxcdf_reader/compat.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringForm {
    /// A netCDF `string` or a fixed-size string: one whole string in each element.
    Native,
    /// A `char` variable whose last axis holds the characters of one string.
    FixedChar { width: usize },
    /// A `char` variable with no length axis: one string for each byte.
    Char,
}

/// One root variable of a NetCDF file.
#[derive(Debug)]
pub(crate) struct NetcdfVariable {
    file: Arc<AsyncNetcdfFile>,
    /// The path that binds the variable in `file`.
    path: String,
    info: VariableInfo,
    /// The type of the values before the decode.
    stored: DataType,
    /// `None` for a numeric variable.
    string: Option<StringForm>,
    /// The stored length on each axis. It can be shorter than the dimension.
    length: Vec<usize>,
    decode: Decode,
}

impl NetcdfVariable {
    /// `Err` with a reason when the rules cannot model the variable.
    pub(crate) fn try_new(
        file: Arc<AsyncNetcdfFile>,
        variable: &AsyncVariable<'_>,
        phony: &PhonyDimensions,
        dimension_sizes: &IndexMap<String, usize>,
    ) -> std::result::Result<NetcdfVariable, String> {
        let mut names = phony.apply(&variable.dimensions);
        let mut length = stored_length(variable);
        let (stored, string) = match variable.vartype() {
            DType::Int(1) => (DataType::Int8, None),
            DType::Int(2) => (DataType::Int16, None),
            DType::Int(4) => (DataType::Int32, None),
            DType::Int(8) => (DataType::Int64, None),
            DType::Uint(1) => (DataType::UInt8, None),
            DType::Uint(2) => (DataType::UInt16, None),
            DType::Uint(4) => (DataType::UInt32, None),
            DType::Uint(8) => (DataType::UInt64, None),
            DType::Float(4) => (DataType::Float32, None),
            DType::Float(8) => (DataType::Float64, None),
            DType::String | DType::FixedString(_) => (DataType::Utf8, Some(StringForm::Native)),
            DType::Char if names.last().is_some_and(|name| is_string_length(name)) => {
                // The length axis is the string itself, so it leaves the dimensions.
                names.pop();
                let width = length.pop().unwrap_or(0);
                (DataType::Utf8, Some(StringForm::FixedChar { width }))
            }
            DType::Char => (DataType::Utf8, Some(StringForm::Char)),
            other => return Err(format!("the NetCDF type {other:?} has no Arrow type")),
        };

        let dimensions = names
            .iter()
            .map(|name| {
                dimension_sizes
                    .get(name)
                    .map(|&size| Dimension::new(name.as_str(), size))
                    .ok_or_else(|| format!("the dimension '{name}' has no size"))
            })
            .collect::<std::result::Result<Vec<_>, String>>()?;
        let dimensions = Dimensions::try_new(dimensions).map_err(|error| error.to_string())?;

        // The rule of `backend.rs::file_chunk_shape`.
        let rank = dimensions.rank();
        let chunk_shape = match variable.chunking().ok().flatten() {
            Some(chunks) if chunks.len() >= rank => chunks[..rank].to_vec(),
            _ => dimensions.shape(),
        };

        let attributes = attributes_of(variable.attributes());
        let decode = Decode::choose(&stored, &attributes);
        let data_type = decode.data_type(&stored, &attributes);
        let info = VariableInfo::new(variable.name.as_str(), data_type, dimensions)
            .with_chunk_shape(chunk_shape)
            .with_attributes(attributes);
        Ok(NetcdfVariable {
            file,
            path: variable.path.clone(),
            info,
            stored,
            string,
            length,
            decode,
        })
    }

    /// The stored values of one box inside `length`, in C order.
    async fn read_stored(&self, start: &[usize], count: &[usize]) -> Result<ArrayRef> {
        let variable = self
            .file
            .variable(&self.path)
            .ok_or_else(|| Error::format(format!("the file has no variable '{}'", self.path)))?;
        let mut extents: Vec<Extent> = start
            .iter()
            .zip(count)
            .map(|(&start, &count)| Extent::SliceCount {
                start,
                count,
                stride: 1,
            })
            .collect();
        if let Some(StringForm::FixedChar { width }) = self.string {
            extents.push(Extent::SliceCount {
                start: 0,
                count: width,
                stride: 1,
            });
        }
        let extents = Extents::Extent(extents);
        if let Some(form) = self.string {
            let elements = count.iter().product();
            return self.read_strings(&variable, form, extents, elements).await;
        }
        match self.stored {
            DataType::Int8 => read_numbers::<Int8Type>(&variable, extents).await,
            DataType::Int16 => read_numbers::<Int16Type>(&variable, extents).await,
            DataType::Int32 => read_numbers::<Int32Type>(&variable, extents).await,
            DataType::Int64 => read_numbers::<Int64Type>(&variable, extents).await,
            DataType::UInt8 => read_numbers::<UInt8Type>(&variable, extents).await,
            DataType::UInt16 => read_numbers::<UInt16Type>(&variable, extents).await,
            DataType::UInt32 => read_numbers::<UInt32Type>(&variable, extents).await,
            DataType::UInt64 => read_numbers::<UInt64Type>(&variable, extents).await,
            DataType::Float32 => read_numbers::<Float32Type>(&variable, extents).await,
            DataType::Float64 => read_numbers::<Float64Type>(&variable, extents).await,
            ref other => Err(Error::format(format!(
                "no NetCDF read for the type {other}"
            ))),
        }
    }

    /// The strings of one box, with the string `_FillValue` as null.
    async fn read_strings(
        &self,
        variable: &AsyncVariable<'_>,
        form: StringForm,
        extents: Extents,
        elements: usize,
    ) -> Result<ArrayRef> {
        let values: Vec<String> = match form {
            StringForm::Native => variable.get_strings(extents).await.map_err(Error::format)?,
            // A zero-width length axis holds no characters.
            StringForm::FixedChar { width: 0 } => vec![String::new(); elements],
            StringForm::FixedChar { width } => variable
                .get_raw_values(extents)
                .await
                .map_err(Error::format)?
                .chunks(width)
                .map(|chunk| {
                    String::from_utf8_lossy(chunk)
                        .trim_end_matches(|c: char| c == '\0' || c.is_whitespace())
                        .to_string()
                })
                .collect(),
            StringForm::Char => variable
                .get_raw_values(extents)
                .await
                .map_err(Error::format)?
                .into_iter()
                .map(|byte| (byte as char).to_string())
                .collect(),
        };
        let fill = self.info.attributes.get_str("_FillValue");
        let strings: StringArray = values
            .iter()
            .map(|value| (Some(value.as_str()) != fill).then_some(value.as_str()))
            .collect();
        Ok(Arc::new(strings))
    }
}

/// The current length of each axis that the file stores for `variable`.
///
/// `oxcdf` gives a short variable the length of its unlimited dimension, so
/// the HDF5 dataset gives the stored length.
pub(super) fn stored_length(variable: &AsyncVariable<'_>) -> Vec<usize> {
    let shape = &variable.shape;
    let stored = variable
        .dataset()
        .map(|dataset| &dataset.shape)
        .filter(|stored| stored.len() == shape.len())
        .unwrap_or(shape);
    shape
        .iter()
        .zip(stored)
        .map(|(&length, &stored)| length.min(stored) as usize)
        .collect()
}

/// True for a dimension name that marks the string length axis of a char array.
fn is_string_length(name: &str) -> bool {
    let name = name.to_lowercase();
    name.starts_with("string") || name.starts_with("strlen") || name.starts_with("strnlen")
}

/// Read the numbers of `extents` as an Arrow array of `T`.
async fn read_numbers<T>(variable: &AsyncVariable<'_>, extents: Extents) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: oxcdf::Element,
{
    let values = variable
        .get_values::<T::Native, _>(extents)
        .await
        .map_err(Error::format)?;
    Ok(Arc::new(PrimitiveArray::<T>::from_iter_values(values)))
}

/// Place `values` of the shape `clipped` at the start of `shape`, with nulls elsewhere.
fn pad(values: ArrayRef, clipped: &[usize], shape: &[usize]) -> Result<ArrayRef> {
    let len: usize = shape.iter().product();
    if values.is_empty() {
        return Ok(new_null_array(values.data_type(), len));
    }
    let mut indices = Vec::with_capacity(len);
    let mut index = vec![0usize; shape.len()];
    for _ in 0..len {
        let inside = index.iter().zip(clipped).all(|(&i, &n)| i < n);
        indices.push(inside.then(|| {
            index
                .iter()
                .zip(clipped)
                .fold(0u64, |flat, (&i, &n)| flat * n as u64 + i as u64)
        }));
        // The next index in C order.
        for axis in (0..shape.len()).rev() {
            index[axis] += 1;
            if index[axis] < shape[axis] {
                break;
            }
            index[axis] = 0;
        }
    }
    Ok(arrow::compute::take(
        values.as_ref(),
        &UInt64Array::from(indices),
        None,
    )?)
}

#[async_trait]
impl NdVariable for NetcdfVariable {
    fn name(&self) -> &str {
        &self.info.name
    }

    fn data_type(&self) -> &DataType {
        &self.info.data_type
    }

    fn dimensions(&self) -> &Dimensions {
        &self.info.dimensions
    }

    fn chunk_shape(&self) -> Option<&[usize]> {
        self.info.chunk_shape.as_deref()
    }

    fn attributes(&self) -> &Attributes {
        &self.info.attributes
    }

    async fn read(&self, region: &Region) -> Result<NdArrowArray> {
        region.check(self.name(), self.dimensions())?;
        // A variable shorter than its dimension reads only its stored part.
        let clipped: Vec<usize> = region
            .start()
            .iter()
            .zip(region.shape())
            .zip(&self.length)
            .map(|((&start, &count), &length)| count.min(length.saturating_sub(start)))
            .collect();
        let values = if clipped.iter().product::<usize>() == 0 {
            new_empty_array(&self.stored)
        } else {
            self.read_stored(region.start(), &clipped).await?
        };
        let values = if clipped == region.shape() {
            values
        } else {
            pad(values, &clipped, region.shape())?
        };

        let dimensions = Dimensions::try_new(
            self.dimensions()
                .iter()
                .zip(region.shape())
                .map(|(dimension, &size)| Dimension::new(dimension.name(), size)),
        )?;
        let raw = NdArrowArray::try_new(values, dimensions)?
            .with_attributes(self.info.attributes.clone());
        // The chunk reader names the variable in a read error.
        let decoded = self.decode.apply(raw)?;
        if decoded.data_type() != self.data_type() {
            return Err(Error::Invalid {
                dataset: self.path.clone(),
                reason: format!(
                    "the variable '{}' reads as {}, not as its type {}",
                    self.name(),
                    decoded.data_type(),
                    self.data_type()
                ),
            });
        }
        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, AsArray, BinaryArray, FixedSizeBinaryArray, Float64Array, Int16Array,
    };
    use arrow::datatypes::{DataType, Float64Type};
    use beacon_nd_dataset::{ChunkGrid, ChunkReader, DatasetSchema, Error, NdVariable, Region};
    use indexmap::IndexMap;
    use nd_arrow_array::Attributes;
    use oxcdf::{AsyncNetcdfFile, AttributeValue};

    use super::NetcdfVariable;
    use crate::datafusion::test_util::{
        local_object, test_file, write_cf_netcdf, TestVariable, GRIDDED_FILE,
    };
    use crate::dimensions::PhonyDimensions;
    use crate::oxcdf_reader::open_file;

    async fn open(path: &Path) -> Arc<AsyncNetcdfFile> {
        let (store, object) = local_object(path).await;
        Arc::new(open_file(store, &object).await.unwrap())
    }

    /// The size of each dimension: the largest length of its variables.
    fn dimension_sizes(file: &AsyncNetcdfFile, phony: &PhonyDimensions) -> IndexMap<String, usize> {
        let mut sizes = IndexMap::new();
        for variable in file.variables() {
            let names = phony.apply(&variable.dimensions);
            for (name, &length) in names.into_iter().zip(&variable.shape) {
                let size = sizes.entry(name).or_insert(0);
                *size = (*size).max(length as usize);
            }
        }
        sizes
    }

    fn variable(file: &Arc<AsyncNetcdfFile>, name: &str) -> NetcdfVariable {
        let phony = PhonyDimensions::of_file(file);
        let sizes = dimension_sizes(file, &phony);
        let variable = file.variable(name).unwrap();
        NetcdfVariable::try_new(file.clone(), &variable, &phony, &sizes).unwrap()
    }

    /// Write one file with `variables` and open the variable `name`.
    async fn written(
        dimensions: &[(&str, Option<usize>)],
        variables: Vec<TestVariable>,
        name: &str,
    ) -> NetcdfVariable {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cf.nc");
        write_cf_netcdf(&path, dimensions, variables);
        variable(&open(&path).await, name)
    }

    fn strings(array: &ArrayRef) -> Vec<Option<&str>> {
        array.as_string::<i32>().iter().collect()
    }

    #[tokio::test]
    async fn a_region_reads_the_decoded_values() {
        let temp = written(
            &[("x", Some(4))],
            vec![TestVariable {
                name: "temp",
                dimensions: vec!["x"],
                values: Arc::new(Int16Array::from(vec![10, 20, 30, 40])),
                attributes: vec![("scale_factor", AttributeValue::Double(0.5))],
            }],
            "temp",
        )
        .await;
        assert_eq!(temp.data_type(), &DataType::Float64);

        let read = temp.read(&Region::new(vec![1], vec![2])).await.unwrap();
        assert_eq!(read.dims().shape(), vec![2]);
        let values = read.values().as_primitive::<Float64Type>();
        assert_eq!(values, &Float64Array::from(vec![10.0, 15.0]));
    }

    #[tokio::test]
    async fn a_rank_zero_variable_reads_one_value() {
        let depth = written(
            &[],
            vec![TestVariable {
                name: "depth",
                dimensions: vec![],
                values: Arc::new(Float64Array::from(vec![7.5])),
                attributes: vec![],
            }],
            "depth",
        )
        .await;
        assert_eq!(depth.dimensions().rank(), 0);

        let read = depth.read(&Region::full(depth.dimensions())).await.unwrap();
        assert_eq!(read.dims().rank(), 0);
        let values = read.values().as_primitive::<Float64Type>();
        assert_eq!(values, &Float64Array::from(vec![7.5]));
    }

    #[tokio::test]
    async fn a_fixed_char_array_reads_one_string_per_row() {
        let rows = FixedSizeBinaryArray::try_from_iter([b"ab\0\0", b"cd  "].into_iter()).unwrap();
        let name = written(
            &[("n", Some(2)), ("strlen", Some(4))],
            vec![TestVariable {
                name: "name",
                dimensions: vec!["n", "strlen"],
                values: Arc::new(rows),
                attributes: vec![],
            }],
            "name",
        )
        .await;
        assert_eq!(name.data_type(), &DataType::Utf8);
        assert_eq!(name.dimensions().names(), vec!["n"]);
        assert_eq!(name.chunk_shape().map(<[usize]>::len), Some(1));

        let read = name.read(&Region::full(name.dimensions())).await.unwrap();
        assert_eq!(strings(read.values()), [Some("ab"), Some("cd")]);
    }

    #[tokio::test]
    async fn a_char_array_without_a_length_axis_reads_bytes() {
        let flag = written(
            &[("n", Some(3))],
            vec![TestVariable {
                name: "flag",
                dimensions: vec!["n"],
                values: Arc::new(BinaryArray::from_vec(vec![b"x", b"y", b"z"])),
                attributes: vec![],
            }],
            "flag",
        )
        .await;
        assert_eq!(flag.dimensions().names(), vec!["n"]);

        let read = flag.read(&Region::new(vec![1], vec![2])).await.unwrap();
        assert_eq!(strings(read.values()), [Some("y"), Some("z")]);
    }

    #[tokio::test]
    async fn a_string_fill_becomes_null() {
        // The oxcdf writer refuses a fill on a netCDF string variable, so a char variable carries it.
        let code = written(
            &[("n", Some(3))],
            vec![TestVariable {
                name: "code",
                dimensions: vec!["n"],
                values: Arc::new(BinaryArray::from_vec(vec![b"a", b"-", b"b"])),
                attributes: vec![("_FillValue", AttributeValue::Str("-".to_string()))],
            }],
            "code",
        )
        .await;

        let read = code.read(&Region::full(code.dimensions())).await.unwrap();
        assert_eq!(strings(read.values()), [Some("a"), None, Some("b")]);
    }

    #[tokio::test]
    async fn a_short_variable_pads_with_nulls() {
        let b = written(
            &[("t", None)],
            vec![
                TestVariable {
                    name: "a",
                    dimensions: vec!["t"],
                    values: Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
                    attributes: vec![],
                },
                TestVariable {
                    name: "b",
                    dimensions: vec!["t"],
                    values: Arc::new(Float64Array::from(vec![4.0, 5.0])),
                    attributes: vec![],
                },
            ],
            "b",
        )
        .await;
        assert_eq!(b.dimensions().shape(), vec![3]);

        let read = b.read(&Region::new(vec![0], vec![3])).await.unwrap();
        let values = read.values().as_primitive::<Float64Type>();
        assert_eq!(
            values,
            &Float64Array::from(vec![Some(4.0), Some(5.0), None])
        );

        let past = b.read(&Region::new(vec![2], vec![1])).await.unwrap();
        assert_eq!(past.values().null_count(), 1);
    }

    #[tokio::test]
    async fn a_decode_error_names_the_variable() {
        let mut temp = written(
            &[("x", Some(2))],
            vec![TestVariable {
                name: "temp",
                dimensions: vec!["x"],
                values: Arc::new(Int16Array::from(vec![1, 2])),
                attributes: vec![("scale_factor", AttributeValue::Double(0.5))],
            }],
            "temp",
        )
        .await;
        // The oxcdf writer refuses an empty attribute, and the kernel fails on one.
        temp.info.attributes = Attributes::new().with("scale_factor", Vec::<f32>::new());
        let dimensions = temp.dimensions().clone();
        let dataset = DatasetSchema::try_new(
            "cf.nc",
            dimensions,
            vec![Arc::new(temp) as Arc<dyn NdVariable>],
        )
        .unwrap();
        let grid = ChunkGrid::try_new(&dataset, 8192).unwrap();
        let reader = ChunkReader::try_new(&dataset, grid).unwrap();

        let error = reader.read(&reader.grid().chunk(0)).await.unwrap_err();
        let message = error.to_string();
        let Error::Read { variable, error } = error else {
            panic!("{error:?}");
        };
        assert_eq!(variable, "temp");
        assert!(matches!(*error, Error::Nd(_)), "{error:?}");
        assert_eq!(message.matches("'temp'").count(), 1, "{message}");
    }

    #[tokio::test]
    async fn a_short_two_dimensional_variable_pads_in_c_order() {
        let v = written(
            &[("t", None), ("x", Some(2))],
            vec![
                TestVariable {
                    name: "a",
                    dimensions: vec!["t"],
                    values: Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
                    attributes: vec![],
                },
                TestVariable {
                    name: "v",
                    dimensions: vec!["t", "x"],
                    values: Arc::new(Float64Array::from(vec![10.0, 11.0, 20.0, 21.0])),
                    attributes: vec![],
                },
            ],
            "v",
        )
        .await;
        assert_eq!(v.dimensions().shape(), vec![3, 2]);

        let read = v.read(&Region::full(v.dimensions())).await.unwrap();
        assert_eq!(read.dims().shape(), vec![3, 2]);
        let values = read.values().as_primitive::<Float64Type>();
        let expected = [Some(10.0), Some(11.0), Some(20.0), Some(21.0), None, None];
        assert_eq!(values, &Float64Array::from(expected.to_vec()));

        let corner = v.read(&Region::new(vec![1, 1], vec![2, 1])).await.unwrap();
        let values = corner.values().as_primitive::<Float64Type>();
        assert_eq!(values, &Float64Array::from(vec![Some(21.0), None]));
    }

    #[tokio::test]
    async fn a_region_outside_the_variable_is_invalid() {
        let file = open(&test_file(GRIDDED_FILE)).await;
        let temp = variable(&file, "analysed_sst");
        let mut shape = temp.dimensions().shape();
        shape[0] += 1;
        let error = temp
            .read(&Region::new(vec![0; shape.len()], shape))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::InvalidRegion { .. }), "{error:?}");
    }

    #[tokio::test]
    async fn the_read_type_is_the_declared_type() {
        let file = open(&test_file(GRIDDED_FILE)).await;
        let phony = PhonyDimensions::of_file(&file);
        let sizes = dimension_sizes(&file, &phony);
        let mut count = 0;
        for variable in file.variables() {
            let variable = NetcdfVariable::try_new(file.clone(), &variable, &phony, &sizes)
                .unwrap_or_else(|reason| panic!("{}: {reason}", variable.name));
            let read = variable
                .read(&Region::full(variable.dimensions()))
                .await
                .unwrap();
            assert_eq!(
                read.data_type(),
                variable.data_type(),
                "{}",
                variable.name()
            );
            assert_eq!(read.dims().names(), variable.dimensions().names());
            count += 1;
        }
        assert!(count > 1);
    }
}
