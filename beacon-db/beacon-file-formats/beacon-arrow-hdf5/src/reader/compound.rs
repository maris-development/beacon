//! One array per member of a compound dataset.
//!
//! An HDF5 compound dataset holds a record in each element, the way a table row
//! holds columns. The netCDF data model has no such variable, so the netCDF
//! reader skips the dataset and netcdf-c reports nothing at all. This module
//! reads it.
//!
//! Each member becomes its own array, named `dataset/member`. The arrays share
//! the dataset's shape and dimension names, so a query joins them by row the
//! same way it joins two ordinary variables.
//!
//! # How a member is read
//!
//! [`oxcdf`] reads the raw bytes of a selection. One element of the selection is
//! the whole record, so a member is the byte window `offset .. offset + size`
//! inside each record. This module gathers those windows and hands them to
//! [`oxcdf_hdf5::read::RawData`], which owns the decoders.
//!
//! # Limits
//!
//! A member of a fixed-width numeric or string type is read. A member of any
//! other type — a variable-length string, a nested compound, an array — is
//! skipped, because its value is a pointer into a heap rather than the value
//! itself. [`member_arrays`] reports a compound with no readable member by
//! name, which is the "clear error" the caller logs.

use std::sync::Arc;

use beacon_nd_array::{
    array::{backend::ArrayBackend, subset::ArraySubset},
    datatypes::NdArrayType,
    NdArray, NdArrayD,
};
use ndarray::ArrayD;
use oxcdf::{AsyncNetcdfFile, Extent, Extents};
use oxcdf_hdf5::message::datatype::CompoundMember;
use oxcdf_hdf5::message::{ByteOrder, Datatype, DatatypeClass};
use oxcdf_hdf5::read::RawData;
use oxcdf_hdf5::DType;

/// The members of a compound dataset, or `None` when the variable is not one.
pub fn members_of(datatype: &Datatype) -> Option<&[CompoundMember]> {
    match &datatype.class {
        DatatypeClass::Compound { members } => Some(members),
        _ => None,
    }
}

/// Expand a compound variable into one lazy ND array per readable member.
///
/// The names are `dataset/member`, with `dataset` the variable's own array name
/// (its path, without the leading slash).
///
/// # Errors
///
/// Returns an error when the variable is not a compound dataset, or when no
/// member has a type this reader models. The message names the dataset and
/// every member type, so an operator can see what the file holds.
pub fn member_arrays(
    file: Arc<AsyncNetcdfFile>,
    variable: &oxcdf::AsyncVariable<'_>,
    array_name: &str,
) -> anyhow::Result<Vec<(String, Arc<dyn NdArrayD>)>> {
    let datatype = variable.datatype();
    let members = members_of(datatype).ok_or_else(|| {
        anyhow::anyhow!(
            "Dataset '{}' is not a compound dataset, so it has no members to expand",
            array_name
        )
    })?;

    let shape: Vec<usize> = variable.shape.iter().map(|&len| len as usize).collect();
    let dimensions = variable.dimensions.clone();
    let chunk_shape = chunk_shape_of(variable, &shape);
    let record_size = datatype.size as usize;

    let mut arrays: Vec<(String, Arc<dyn NdArrayD>)> = Vec::new();
    for member in members {
        let member_ref = MemberRef {
            file: file.clone(),
            path: variable.path.clone(),
            shape: shape.clone(),
            chunk_shape: chunk_shape.clone(),
            dimensions: dimensions.clone(),
            record_size,
            offset: member.offset as usize,
            datatype: member.datatype.clone(),
        };
        let Some(array) = member_to_nd_array(member_ref) else {
            tracing::debug!(
                dataset = array_name,
                member = member.name,
                "skipping a compound member this reader does not model"
            );
            continue;
        };
        arrays.push((format!("{array_name}/{}", member.name), array?));
    }

    if arrays.is_empty() {
        let types: Vec<String> = members
            .iter()
            .map(|m| format!("{}: {}", m.name, DType::of(&m.datatype).name()))
            .collect();
        return Err(anyhow::anyhow!(
            "Compound dataset '{}' has no member this reader models. Its members are {}. \
             A member of a fixed-width numeric or string type is read; a variable-length, \
             nested or array member is not.",
            array_name,
            types.join(", ")
        ));
    }

    Ok(arrays)
}

/// The chunk shape the file stores, falling back to the full shape.
///
/// A contiguous dataset has no chunk grid, and a chunk shape of a lower rank
/// than the logical shape cannot map onto it.
fn chunk_shape_of(variable: &oxcdf::AsyncVariable<'_>, shape: &[usize]) -> Vec<usize> {
    match variable.chunking().ok().flatten() {
        Some(chunks) if chunks.len() >= shape.len() => chunks[..shape.len()].to_vec(),
        _ => shape.to_vec(),
    }
}

/// Build the ND array of one member, or `None` when its type is not modelled.
fn member_to_nd_array(member: MemberRef) -> Option<anyhow::Result<Arc<dyn NdArrayD>>> {
    /// Wrap a numeric member in the backend that reads it as `T`.
    macro_rules! numeric {
        ($t:ty) => {
            Some(
                NdArray::new_with_backend(NumericMemberBackend::<$t>::new(member))
                    .map(|array| Arc::new(array) as Arc<dyn NdArrayD>)
                    .map_err(anyhow::Error::from),
            )
        };
    }

    match DType::of(&member.datatype) {
        DType::Int(1) => numeric!(i8),
        DType::Int(2) => numeric!(i16),
        DType::Int(4) => numeric!(i32),
        DType::Int(8) => numeric!(i64),
        DType::Uint(1) => numeric!(u8),
        DType::Uint(2) => numeric!(u16),
        DType::Uint(4) => numeric!(u32),
        DType::Uint(8) => numeric!(u64),
        DType::Float(4) => numeric!(f32),
        DType::Float(8) => numeric!(f64),
        // A fixed-width string sits in the record itself. `Char` is the
        // one-byte flavour, which holds a one-character string per element.
        DType::Char | DType::FixedString(_) => Some(
            NdArray::new_with_backend(StringMemberBackend::new(member))
                .map(|array| Arc::new(array) as Arc<dyn NdArrayD>)
                .map_err(anyhow::Error::from),
        ),
        // A variable-length or structured member holds a pointer, not a value.
        // A numeric width HDF5 allows but no Rust type matches lands here too.
        _ => None,
    }
}

/// One member of one compound dataset, bound to the file that holds it.
///
/// [`oxcdf::AsyncVariable`] borrows the file, so a backend cannot keep one. It
/// keeps the variable path instead and binds on each read. The bind reads no
/// bytes: the open holds every piece of metadata.
#[derive(Debug)]
struct MemberRef {
    file: Arc<AsyncNetcdfFile>,
    /// The dataset path, such as `/observations/measurements`.
    path: String,
    shape: Vec<usize>,
    chunk_shape: Vec<usize>,
    dimensions: Vec<String>,
    /// Width of one whole record.
    record_size: usize,
    /// Byte offset of this member inside a record.
    offset: usize,
    /// This member's own type.
    datatype: Datatype,
}

impl MemberRef {
    fn element_count(&self) -> usize {
        self.shape.iter().product()
    }

    /// Width of one member value.
    fn member_size(&self) -> usize {
        self.datatype.size as usize
    }

    /// Translate a subset into the extents [`oxcdf`] reads.
    fn extents(&self, subset: &ArraySubset) -> anyhow::Result<Extents> {
        let mut extents = Vec::with_capacity(self.shape.len());
        for axis in 0..self.shape.len() {
            let start = subset.start.get(axis).copied().ok_or_else(|| {
                anyhow::anyhow!(
                    "Compound dataset '{}' subset is missing start for axis {}",
                    self.path,
                    axis
                )
            })?;
            let count = subset.shape.get(axis).copied().ok_or_else(|| {
                anyhow::anyhow!(
                    "Compound dataset '{}' subset is missing length for axis {}",
                    self.path,
                    axis
                )
            })?;
            extents.push(Extent::SliceCount {
                start,
                count,
                stride: 1,
            });
        }
        Ok(Extents::Extent(extents))
    }

    /// Read the selection and gather this member's bytes out of every record.
    ///
    /// The result is a [`RawData`] over the member alone, which owns the
    /// decoders for every type this module models.
    async fn read_member(&self, subset: &ArraySubset) -> anyhow::Result<RawData> {
        let variable = self
            .file
            .variable(&self.path)
            .ok_or_else(|| anyhow::anyhow!("Dataset '{}' not found in the HDF5 file", self.path))?;
        let records = variable.get_raw_values(self.extents(subset)?).await?;

        let member_size = self.member_size();
        if self.record_size == 0 || member_size == 0 {
            return Err(anyhow::anyhow!(
                "Compound dataset '{}' declares a zero-width record or member",
                self.path
            ));
        }
        let count = records.len() / self.record_size;
        // An out-of-range member offset means the file disagrees with itself.
        if self.offset + member_size > self.record_size {
            return Err(anyhow::anyhow!(
                "Compound dataset '{}' places a member at offset {} of a {}-byte record",
                self.path,
                self.offset,
                self.record_size
            ));
        }

        let mut bytes = Vec::with_capacity(count * member_size);
        for record in 0..count {
            let start = record * self.record_size + self.offset;
            bytes.extend_from_slice(&records[start..start + member_size]);
        }

        // A read of a whole dataset swaps a big-endian value to native order.
        // A compound has no byte order of its own, so that pass leaves the
        // record untouched and each member converts here instead.
        if matches!(self.datatype.byte_order(), Some(ByteOrder::Big)) {
            for value in bytes.chunks_mut(member_size) {
                value.reverse();
            }
        }

        Ok(RawData {
            bytes,
            element_size: member_size,
            shape: subset.shape.iter().map(|&len| len as u64).collect(),
        })
    }
}

/// Reshape flat, row-major values into the shape the subset selected.
fn shaped<T>(path: &str, subset: &ArraySubset, values: Vec<T>) -> anyhow::Result<ArrayD<T>> {
    ArrayD::from_shape_vec(ndarray::IxDyn(&subset.shape), values).map_err(|e| {
        anyhow::anyhow!(
            "Failed to shape the values of compound dataset '{}' into {:?}: {}",
            path,
            subset.shape,
            e
        )
    })
}

/// Reads one numeric member as `T`.
#[derive(Debug)]
struct NumericMemberBackend<T> {
    member: MemberRef,
    _marker: std::marker::PhantomData<T>,
}

impl<T> NumericMemberBackend<T> {
    fn new(member: MemberRef) -> Self {
        Self {
            member,
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<T> ArrayBackend<T> for NumericMemberBackend<T>
where
    T: NdArrayType + oxcdf_hdf5::Element,
{
    fn len(&self) -> usize {
        self.member.element_count()
    }

    fn shape(&self) -> Vec<usize> {
        self.member.shape.clone()
    }

    fn chunk_shape(&self) -> Vec<usize> {
        self.member.chunk_shape.clone()
    }

    fn dimensions(&self) -> Vec<String> {
        self.member.dimensions.clone()
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ArrayD<T>> {
        let raw = self.member.read_member(&subset).await?;
        let values = raw.get_of::<T>(&self.member.datatype, &self.member.path)?;
        shaped(&self.member.path, &subset, values)
    }
}

/// Reads one fixed-width string member as UTF-8 strings.
#[derive(Debug)]
struct StringMemberBackend {
    member: MemberRef,
}

impl StringMemberBackend {
    fn new(member: MemberRef) -> Self {
        Self { member }
    }
}

#[async_trait::async_trait]
impl ArrayBackend<String> for StringMemberBackend {
    fn len(&self) -> usize {
        self.member.element_count()
    }

    fn shape(&self) -> Vec<usize> {
        self.member.shape.clone()
    }

    fn chunk_shape(&self) -> Vec<usize> {
        self.member.chunk_shape.clone()
    }

    fn dimensions(&self) -> Vec<String> {
        self.member.dimensions.clone()
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ArrayD<String>> {
        let raw = self.member.read_member(&subset).await?;
        let values = raw.to_strings_of(&self.member.datatype)?;
        shaped(&self.member.path, &subset, values)
    }
}
