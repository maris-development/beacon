//! Lazy array backends that read variable data through [`oxcdf`].
//!
//! The netcdf-c path stacks [`VariableDecoder`](crate::decoders::VariableDecoder)
//! values on top of one typed read. It must: `netcdf::Variable::get` returns
//! only the type the file stores, so a CF transform needs a wrapper for each
//! source type. [`oxcdf`] converts on read, so every transform here is one
//! backend over an `f64` read or a raw byte read. That removes the decoder
//! layer from this path.

use std::sync::Arc;

use beacon_nd_array::{
    array::{backend::ArrayBackend, subset::ArraySubset},
    datatypes::{NdArrayType, TimestampNanosecond},
};
use ndarray::ArrayD;
use oxcdf::{AsyncNetcdfFile, AsyncVariable, Extent, Extents};

/// The chunk shape the file stores for a variable.
///
/// The read costs no bytes. The open holds the layout of every variable.
///
/// The value falls back to `shape`:
///
/// - for a contiguous variable, and for a classic file, which has no chunks;
/// - for a chunk shape of a lower rank than `shape`, which cannot map onto it.
///
/// A fixed-size string variable drops its length axis from `shape`. The chunk
/// shape drops the same axis, so both keep the same rank.
fn file_chunk_shape(file: &AsyncNetcdfFile, name: &str, shape: &[usize]) -> Vec<usize> {
    let chunks = file
        .variable(name)
        .and_then(|variable| variable.chunking().ok().flatten());

    match chunks {
        Some(chunks) if chunks.len() >= shape.len() => chunks[..shape.len()].to_vec(),
        _ => shape.to_vec(),
    }
}

/// One variable, bound to the file that holds it.
///
/// [`AsyncVariable`] borrows the file, so a backend cannot keep one. It keeps
/// the variable name instead and binds on each read. The bind reads no bytes:
/// the open holds every piece of metadata.
#[derive(Debug)]
pub struct VariableRef {
    file: Arc<AsyncNetcdfFile>,
    name: String,
    /// The logical shape. A fixed-size string variable drops its length axis.
    shape: Vec<usize>,
    /// The chunk shape of the file, on the axes of `shape`.
    chunk_shape: Vec<usize>,
    /// The logical dimension names, one for each axis of `shape`.
    dimensions: Vec<String>,
    /// The trailing string-length axis, when the variable has one. It is part
    /// of the read but not of the logical shape.
    string_width: Option<usize>,
}

impl VariableRef {
    /// Bind a variable of the given logical shape.
    ///
    /// The chunk shape comes from the file once, here. A later read of it costs
    /// nothing.
    pub fn new(
        file: Arc<AsyncNetcdfFile>,
        name: String,
        shape: Vec<usize>,
        dimensions: Vec<String>,
    ) -> Self {
        let chunk_shape = file_chunk_shape(&file, &name, &shape);
        Self {
            file,
            name,
            shape,
            chunk_shape,
            dimensions,
            string_width: None,
        }
    }

    /// Add the trailing string-length axis of a fixed-size string variable.
    ///
    /// The axis stays out of `shape`, because it holds the characters of one
    /// string rather than a dimension of the data.
    pub fn with_string_width(mut self, width: usize) -> Self {
        self.string_width = Some(width);
        self
    }

    /// The variable, bound to its file.
    fn bind(&self) -> anyhow::Result<AsyncVariable<'_>> {
        self.file
            .variable(&self.name)
            .ok_or_else(|| anyhow::anyhow!("Variable '{}' not found in NetCDF file", self.name))
    }

    /// Translate a subset into the extents [`oxcdf`] reads.
    ///
    /// A fixed-size string variable gets its length axis appended, in full.
    fn extents(&self, subset: &ArraySubset) -> anyhow::Result<Extents> {
        let mut extents = Vec::with_capacity(self.shape.len() + 1);
        for axis in 0..self.shape.len() {
            let start = subset.start.get(axis).copied().ok_or_else(|| {
                anyhow::anyhow!(
                    "Variable '{}' subset is missing start for axis {}",
                    self.name,
                    axis
                )
            })?;
            let count = subset.shape.get(axis).copied().ok_or_else(|| {
                anyhow::anyhow!(
                    "Variable '{}' subset is missing length for axis {}",
                    self.name,
                    axis
                )
            })?;
            extents.push(Extent::SliceCount {
                start,
                count,
                stride: 1,
            });
        }
        if let Some(width) = self.string_width {
            extents.push(Extent::SliceCount {
                start: 0,
                count: width,
                stride: 1,
            });
        }
        Ok(Extents::Extent(extents))
    }

    /// Number of elements in the logical shape.
    fn element_count(&self) -> usize {
        self.shape.iter().product()
    }

    /// The chunk shape the file stores, on the axes of the logical shape.
    fn chunk_shape(&self) -> Vec<usize> {
        self.chunk_shape.clone()
    }
}

/// Reshape flat, row-major values into the shape the subset selected.
fn shaped<T>(name: &str, subset: &ArraySubset, values: Vec<T>) -> anyhow::Result<ArrayD<T>> {
    ArrayD::from_shape_vec(ndarray::IxDyn(&subset.shape), values).map_err(|e| {
        anyhow::anyhow!(
            "Failed to shape the values of variable '{}' into {:?}: {}",
            name,
            subset.shape,
            e
        )
    })
}

/// Reads a numeric variable as `T`.
///
/// [`oxcdf`] converts from the stored type, so `T` is the logical type this
/// crate wants rather than the one the file holds.
#[derive(Debug)]
pub struct NumericBackend<T> {
    variable: VariableRef,
    fill_value: Option<T>,
}

impl<T> NumericBackend<T> {
    /// Build a numeric backend with an optional `_FillValue`.
    pub fn new(variable: VariableRef, fill_value: Option<T>) -> Self {
        Self {
            variable,
            fill_value,
        }
    }
}

#[async_trait::async_trait]
impl<T> ArrayBackend<T> for NumericBackend<T>
where
    T: NdArrayType + oxcdf::Element,
{
    fn len(&self) -> usize {
        self.variable.element_count()
    }

    fn shape(&self) -> Vec<usize> {
        self.variable.shape.clone()
    }

    fn chunk_shape(&self) -> Vec<usize> {
        self.variable.chunk_shape()
    }

    fn dimensions(&self) -> Vec<String> {
        self.variable.dimensions.clone()
    }

    fn fill_value(&self) -> Option<T> {
        self.fill_value
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ArrayD<T>> {
        let variable = self.variable.bind()?;
        let extents = self.variable.extents(&subset)?;
        Ok(variable.get::<T, _>(extents).await?)
    }
}

/// Reads a packed numeric variable and applies CF `scale_factor` /
/// `add_offset`.
///
/// The values come back as `f64` and decode as `raw * scale + offset`, which
/// matches [`ScaleOffsetVariableDecoder`](crate::decoders::scale_offset::ScaleOffsetVariableDecoder)
/// on the netcdf-c path.
#[derive(Debug)]
pub struct ScaleOffsetBackend {
    variable: VariableRef,
    scale: f64,
    offset: f64,
    fill_value: Option<f64>,
}

impl ScaleOffsetBackend {
    /// Build a scale/offset backend.
    ///
    /// `raw_fill_value` is the fill in packed units. It decodes with the same
    /// arithmetic, so a packed fill cell maps onto the decoded fill and the
    /// engine nulls it after the decode.
    pub fn new(
        variable: VariableRef,
        scale: f64,
        offset: f64,
        raw_fill_value: Option<f64>,
    ) -> Self {
        Self {
            variable,
            scale,
            offset,
            fill_value: raw_fill_value.map(|f| f * scale + offset),
        }
    }
}

#[async_trait::async_trait]
impl ArrayBackend<f64> for ScaleOffsetBackend {
    fn len(&self) -> usize {
        self.variable.element_count()
    }

    fn shape(&self) -> Vec<usize> {
        self.variable.shape.clone()
    }

    fn chunk_shape(&self) -> Vec<usize> {
        self.variable.chunk_shape()
    }

    fn dimensions(&self) -> Vec<String> {
        self.variable.dimensions.clone()
    }

    fn fill_value(&self) -> Option<f64> {
        self.fill_value
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ArrayD<f64>> {
        let variable = self.variable.bind()?;
        let extents = self.variable.extents(&subset)?;
        let array = variable.get::<f64, _>(extents).await?;
        let (scale, offset) = (self.scale, self.offset);
        Ok(array.mapv(|v| v * scale + offset))
    }
}

/// Reads a CF time variable and converts it to nanosecond timestamps.
#[derive(Debug)]
pub struct TimestampBackend {
    variable: VariableRef,
    epoch: hifitime::Epoch,
    unit: hifitime::Unit,
}

impl TimestampBackend {
    /// Build a CF time backend from a reference epoch and a unit.
    pub fn new(variable: VariableRef, epoch: hifitime::Epoch, unit: hifitime::Unit) -> Self {
        Self {
            variable,
            epoch,
            unit,
        }
    }
}

#[async_trait::async_trait]
impl ArrayBackend<TimestampNanosecond> for TimestampBackend {
    fn len(&self) -> usize {
        self.variable.element_count()
    }

    fn shape(&self) -> Vec<usize> {
        self.variable.shape.clone()
    }

    fn chunk_shape(&self) -> Vec<usize> {
        self.variable.chunk_shape()
    }

    fn dimensions(&self) -> Vec<String> {
        self.variable.dimensions.clone()
    }

    async fn read_subset(
        &self,
        subset: ArraySubset,
    ) -> anyhow::Result<ArrayD<TimestampNanosecond>> {
        let variable = self.variable.bind()?;
        let extents = self.variable.extents(&subset)?;
        let array = variable.get::<f64, _>(extents).await?;
        Ok(crate::decoders::cf_time::convert_to_timestamp_nanoseconds(
            array.view(),
            self.epoch,
            self.unit,
        ))
    }
}

/// How a string variable stores its text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringSource {
    /// A netCDF `string` variable. One whole string sits in each element.
    Native,
    /// A `char` variable whose last axis holds the length of one string.
    FixedChar,
    /// A `char` variable with no length axis. Each byte becomes one string.
    Char,
}

/// Reads a string-like variable as UTF-8 strings.
#[derive(Debug)]
pub struct StringBackend {
    variable: VariableRef,
    source: StringSource,
    fill_value: Option<String>,
}

impl StringBackend {
    /// Build a string backend for the given storage form.
    pub fn new(variable: VariableRef, source: StringSource, fill_value: Option<String>) -> Self {
        Self {
            variable,
            source,
            fill_value,
        }
    }
}

#[async_trait::async_trait]
impl ArrayBackend<String> for StringBackend {
    fn len(&self) -> usize {
        self.variable.element_count()
    }

    fn shape(&self) -> Vec<usize> {
        self.variable.shape.clone()
    }

    fn chunk_shape(&self) -> Vec<usize> {
        self.variable.chunk_shape()
    }

    fn dimensions(&self) -> Vec<String> {
        self.variable.dimensions.clone()
    }

    fn fill_value(&self) -> Option<String> {
        self.fill_value.clone()
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ArrayD<String>> {
        let variable = self.variable.bind()?;
        let extents = self.variable.extents(&subset)?;
        let name = &self.variable.name;

        let values: Vec<String> = match self.source {
            StringSource::Native => variable.get_strings(extents).await?,
            StringSource::FixedChar => {
                let width = self.variable.string_width.ok_or_else(|| {
                    anyhow::anyhow!(
                        "Variable '{}' decodes as a fixed-size string but carries no string length",
                        name
                    )
                })?;
                let bytes = variable.get_raw_values(extents).await?;
                if width == 0 {
                    // A zero-width length axis holds no characters, so every
                    // selected element is the empty string.
                    vec![String::new(); subset.shape.iter().product()]
                } else {
                    bytes
                        .chunks(width)
                        .map(|chunk| {
                            String::from_utf8_lossy(chunk)
                                .trim_end_matches(|c: char| c == '\0' || c.is_whitespace())
                                .to_string()
                        })
                        .collect()
                }
            }
            StringSource::Char => variable
                .get_raw_values(extents)
                .await?
                .into_iter()
                .map(|byte| (byte as char).to_string())
                .collect(),
        };

        shaped(name, &subset, values)
    }
}
