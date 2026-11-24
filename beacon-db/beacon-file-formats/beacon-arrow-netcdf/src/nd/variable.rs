//! One NetCDF variable on the ND contract.

use std::sync::Arc;

use arrow::array::{
    new_empty_array, new_null_array, ArrayRef, Float64Array, PrimitiveArray, StringArray,
    UInt64Array,
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
use nd_arrow_array::compute::cf::SCALE_FACTOR;
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

/// One variable of a NetCDF or HDF5 file.
#[derive(Debug)]
pub struct NetcdfVariable {
    file: Arc<AsyncNetcdfFile>,
    /// The name of the dataset: the file location that the opener knows.
    dataset: Arc<str>,
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
    ///
    /// `name` is the column name. The path of `variable` binds the reads.
    pub fn try_new(
        name: Arc<str>,
        dataset: Arc<str>,
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
        let info = VariableInfo::new(name, data_type, dimensions)
            .with_chunk_shape(chunk_shape)
            .with_attributes(attributes);
        Ok(NetcdfVariable {
            file,
            dataset,
            path: variable.path.clone(),
            info,
            stored,
            string,
            length,
            decode,
        })
    }

    /// The same variable, unpacked with the scale factor `scale`.
    ///
    /// A scale that the file keeps outside the `scale_factor` attribute.
    pub fn with_scale_factor(mut self, scale: f64) -> NetcdfVariable {
        let mut attributes = self.info.attributes.clone();
        if let Err(error) =
            attributes.try_insert_array(SCALE_FACTOR, Arc::new(Float64Array::from(vec![scale])))
        {
            tracing::warn!(variable = %self.info.name, %error, "no scale factor for the variable");
            return self;
        }
        self.decode = Decode::choose(&self.stored, &attributes);
        self.info.data_type = self.decode.data_type(&self.stored, &attributes);
        self.info.attributes = attributes;
        self
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
pub fn stored_length(variable: &AsyncVariable<'_>) -> Vec<usize> {
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
///
/// A variable shorter than its dimension uses it to fill the rest of a region.
pub fn pad(values: ArrayRef, clipped: &[usize], shape: &[usize]) -> Result<ArrayRef> {
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
                dataset: self.dataset.to_string(),
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
    //! The tests that need the private fields of [`NetcdfVariable`]. The others are in `tests/variable.rs`.

    use std::path::Path;
    use std::sync::Arc;

    use beacon_nd_dataset::{ChunkGrid, ChunkReader, DatasetSchema, Error, NdVariable};
    use indexmap::IndexMap;
    use nd_arrow_array::Attributes;
    use object_store::{local::LocalFileSystem, ObjectStore, ObjectStoreExt};
    use oxcdf::{AsyncNetcdfFile, AttributeValue};

    use super::NetcdfVariable;
    use crate::dimensions::PhonyDimensions;
    use crate::oxcdf_reader::open_file;

    async fn open(path: &Path) -> Arc<AsyncNetcdfFile> {
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let location = object_store::path::Path::from_absolute_path(path).unwrap();
        let object = store.head(&location).await.unwrap();
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
        NetcdfVariable::try_new(
            Arc::from(name),
            Arc::from("cf.nc"),
            file.clone(),
            &variable,
            &phony,
            &sizes,
        )
        .unwrap()
    }

    /// Write `temp`, two `i16` values with a `scale_factor`, and open it as a variable.
    async fn scaled_temp() -> NetcdfVariable {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cf.nc");
        let mut file = oxcdf::create(&path).expect("create the file");
        file.add_dimension("x", 2).unwrap();
        let mut temp = file.add_variable::<i16>("temp", &["x"]).unwrap();
        temp.put_attribute("scale_factor", AttributeValue::Double(0.5))
            .unwrap();
        temp.put_values(&[1i16, 2], ..).unwrap();
        file.close().expect("close the file");
        variable(&open(&path).await, "temp")
    }

    #[tokio::test]
    async fn a_decode_error_names_the_variable() {
        let mut temp = scaled_temp().await;
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
}
