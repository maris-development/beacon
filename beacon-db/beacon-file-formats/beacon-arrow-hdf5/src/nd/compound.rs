//! One ND variable per member of an HDF5 compound dataset.
//!
//! A compound dataset holds one record in each element. Each member with a
//! fixed-width numeric or string type becomes a variable `dataset/member` on
//! the dimensions of the dataset. A variable-length, nested or array member
//! holds a pointer into a heap, so it is left out.

use std::sync::Arc;

use arrow::array::{ArrayRef, PrimitiveArray, StringArray};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use async_trait::async_trait;
use beacon_arrow_netcdf::dimensions::PhonyDimensions;
use beacon_nd_dataset::{
    Attributes, Dimension, Dimensions, Error, NdArrowArray, NdVariable, Region, Result,
    VariableInfo,
};
use indexmap::IndexMap;
use oxcdf::{AsyncNetcdfFile, AsyncVariable, Extent, Extents};
use oxcdf_hdf5::message::{ByteOrder, Datatype, DatatypeClass};
use oxcdf_hdf5::read::RawData;
use oxcdf_hdf5::DType;

/// True when `variable` is an HDF5 compound dataset.
pub(crate) fn is_compound(variable: &AsyncVariable<'_>) -> bool {
    matches!(variable.datatype().class, DatatypeClass::Compound { .. })
}

/// One variable per readable member of the compound `variable`, named `column/member`.
///
/// `Err` names the dataset and every member type when no member is readable.
pub(crate) fn member_variables(
    file: &Arc<AsyncNetcdfFile>,
    variable: &AsyncVariable<'_>,
    column: &str,
    phony: &PhonyDimensions,
    dimension_sizes: &IndexMap<String, usize>,
) -> std::result::Result<Vec<CompoundMember>, String> {
    let datatype = variable.datatype();
    let DatatypeClass::Compound { members } = &datatype.class else {
        return Err(format!("the dataset '{column}' is not a compound dataset"));
    };

    let dimensions = phony
        .apply(&variable.dimensions)
        .iter()
        .map(|name| {
            dimension_sizes
                .get(name)
                .map(|&size| Dimension::new(name.as_str(), size))
                .ok_or_else(|| format!("the dimension '{name}' has no size"))
        })
        .collect::<std::result::Result<Vec<_>, String>>()?;
    let dimensions = Dimensions::try_new(dimensions).map_err(|error| error.to_string())?;
    let rank = dimensions.rank();
    let chunk_shape = match variable.chunking().ok().flatten() {
        Some(chunks) if chunks.len() >= rank => chunks[..rank].to_vec(),
        _ => dimensions.shape(),
    };

    let mut variables = Vec::new();
    for member in members {
        let name = format!("{column}/{}", member.name);
        let Some(data_type) = arrow_type(&member.datatype) else {
            tracing::debug!(variable = %name, "skip a compound member with no fixed-width type");
            continue;
        };
        let info = VariableInfo::new(name.as_str(), data_type, dimensions.clone())
            .with_chunk_shape(chunk_shape.clone());
        variables.push(CompoundMember {
            file: file.clone(),
            path: variable.path.clone(),
            info,
            record_size: datatype.size as usize,
            offset: member.offset as usize,
            datatype: member.datatype.clone(),
        });
    }

    if variables.is_empty() {
        let types: Vec<String> = members
            .iter()
            .map(|m| format!("{}: {}", m.name, DType::of(&m.datatype).name()))
            .collect();
        return Err(format!(
            "the compound dataset '{column}' has no member with a fixed-width numeric or string type; its members are {}",
            types.join(", ")
        ));
    }
    Ok(variables)
}

/// The Arrow type of one member, or `None` when it holds no fixed-width value.
fn arrow_type(datatype: &Datatype) -> Option<DataType> {
    Some(match DType::of(datatype) {
        DType::Int(1) => DataType::Int8,
        DType::Int(2) => DataType::Int16,
        DType::Int(4) => DataType::Int32,
        DType::Int(8) => DataType::Int64,
        DType::Uint(1) => DataType::UInt8,
        DType::Uint(2) => DataType::UInt16,
        DType::Uint(4) => DataType::UInt32,
        DType::Uint(8) => DataType::UInt64,
        DType::Float(4) => DataType::Float32,
        DType::Float(8) => DataType::Float64,
        DType::Char | DType::FixedString(_) => DataType::Utf8,
        _ => return None,
    })
}

/// One member of one compound dataset.
#[derive(Debug)]
pub(crate) struct CompoundMember {
    file: Arc<AsyncNetcdfFile>,
    /// The path that binds the dataset in `file`.
    path: String,
    info: VariableInfo,
    /// The width of one whole record.
    record_size: usize,
    /// The byte offset of the member in a record.
    offset: usize,
    datatype: Datatype,
}

impl CompoundMember {
    /// The member bytes of each record in the box `start`/`count`, in native byte order.
    async fn read_member(&self, start: &[usize], count: &[usize]) -> Result<RawData> {
        let variable = self
            .file
            .variable(&self.path)
            .ok_or_else(|| Error::format(format!("the file has no dataset '{}'", self.path)))?;
        let extents = Extents::Extent(
            start
                .iter()
                .zip(count)
                .map(|(&start, &count)| Extent::SliceCount {
                    start,
                    count,
                    stride: 1,
                })
                .collect(),
        );
        let records = variable
            .get_raw_values(extents)
            .await
            .map_err(Error::format)?;

        let member_size = self.datatype.size as usize;
        if self.record_size == 0 || member_size == 0 || self.offset + member_size > self.record_size
        {
            return Err(Error::format(format!(
                "the compound dataset '{}' has a member at offset {} of {} bytes in a {}-byte record",
                self.path, self.offset, member_size, self.record_size
            )));
        }
        let mut bytes = Vec::with_capacity(records.len() / self.record_size * member_size);
        for record in records.chunks_exact(self.record_size) {
            bytes.extend_from_slice(&record[self.offset..self.offset + member_size]);
        }
        // A compound has no byte order of its own, so each member converts here.
        if matches!(self.datatype.byte_order(), Some(ByteOrder::Big)) {
            for value in bytes.chunks_mut(member_size) {
                value.reverse();
            }
        }
        Ok(RawData {
            bytes,
            element_size: member_size,
            shape: count.iter().map(|&len| len as u64).collect(),
        })
    }

    fn numbers<T>(&self, raw: &RawData) -> Result<ArrayRef>
    where
        T: ArrowPrimitiveType,
        T::Native: oxcdf_hdf5::Element,
    {
        let values = raw
            .get_of::<T::Native>(&self.datatype, &self.path)
            .map_err(Error::format)?;
        Ok(Arc::new(PrimitiveArray::<T>::from_iter_values(values)))
    }
}

#[async_trait]
impl NdVariable for CompoundMember {
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
        let raw = self.read_member(region.start(), region.shape()).await?;
        let values: ArrayRef = match self.info.data_type {
            DataType::Int8 => self.numbers::<Int8Type>(&raw)?,
            DataType::Int16 => self.numbers::<Int16Type>(&raw)?,
            DataType::Int32 => self.numbers::<Int32Type>(&raw)?,
            DataType::Int64 => self.numbers::<Int64Type>(&raw)?,
            DataType::UInt8 => self.numbers::<UInt8Type>(&raw)?,
            DataType::UInt16 => self.numbers::<UInt16Type>(&raw)?,
            DataType::UInt32 => self.numbers::<UInt32Type>(&raw)?,
            DataType::UInt64 => self.numbers::<UInt64Type>(&raw)?,
            DataType::Float32 => self.numbers::<Float32Type>(&raw)?,
            DataType::Float64 => self.numbers::<Float64Type>(&raw)?,
            _ => {
                let strings = raw.to_strings_of(&self.datatype).map_err(Error::format)?;
                Arc::new(StringArray::from(strings))
            }
        };
        let dimensions = Dimensions::try_new(
            self.dimensions()
                .iter()
                .zip(region.shape())
                .map(|(dimension, &size)| Dimension::new(dimension.name(), size)),
        )?;
        Ok(NdArrowArray::try_new(values, dimensions)?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{AsArray, Float32Array, Float64Array, Int32Array};
    use arrow::datatypes::{Float32Type, Float64Type, Int32Type};
    use beacon_arrow_netcdf::dimensions::PhonyDimensions;
    use beacon_nd_dataset::{NdVariable, Region};
    use indexmap::IndexMap;
    use object_store::local::LocalFileSystem;
    use object_store::{path::Path, ObjectStore};
    use oxcdf::AsyncNetcdfFile;

    use super::*;

    async fn compound_file() -> Arc<AsyncNetcdfFile> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_files");
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(root).unwrap());
        Arc::new(
            AsyncNetcdfFile::open_store(store, Path::from("compound.h5"))
                .await
                .unwrap(),
        )
    }

    async fn members() -> (Vec<CompoundMember>, Vec<String>) {
        let file = compound_file().await;
        let phony = PhonyDimensions::of_file(&file);
        let variable = file.variable("measurements").unwrap();
        assert!(is_compound(&variable));
        let index = file.variable("index").unwrap();
        let index_dimensions = phony.apply(&index.dimensions);
        let sizes: IndexMap<String, usize> = index_dimensions
            .iter()
            .map(|name| (name.clone(), 4))
            .collect();
        let members = member_variables(&file, &variable, "measurements", &phony, &sizes).unwrap();
        (members, index_dimensions)
    }

    fn member<'a>(members: &'a [CompoundMember], name: &str) -> &'a CompoundMember {
        members
            .iter()
            .find(|member| member.name() == name)
            .unwrap_or_else(|| panic!("no member '{name}'"))
    }

    async fn read_all(member: &CompoundMember) -> beacon_nd_dataset::NdArrowArray {
        member
            .read(&Region::full(member.dimensions()))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn every_fixed_width_member_is_a_variable() {
        let (members, _) = members().await;

        let station = read_all(member(&members, "measurements/station")).await;
        assert_eq!(
            station.values().as_primitive::<Int32Type>(),
            &Int32Array::from(vec![1, 2, 3, 4])
        );
        let depth = read_all(member(&members, "measurements/depth")).await;
        assert_eq!(
            depth.values().as_primitive::<Float32Type>(),
            &Float32Array::from(vec![0.0, 10.0, 20.0, 30.0])
        );
        let temp = read_all(member(&members, "measurements/temp")).await;
        assert_eq!(
            temp.values().as_primitive::<Float64Type>(),
            &Float64Array::from(vec![12.5, 11.25, 10.0, 9.75])
        );
        let label = read_all(member(&members, "measurements/label")).await;
        let labels: Vec<Option<&str>> = label.values().as_string::<i32>().iter().collect();
        assert_eq!(
            labels,
            [Some("alpha"), Some("beta"), Some("gamma"), Some("delta")]
        );
    }

    #[tokio::test]
    async fn a_variable_length_member_is_left_out() {
        let (members, _) = members().await;
        assert!(members.iter().all(|m| m.name() != "measurements/note"));
    }

    #[tokio::test]
    async fn a_member_keeps_the_dimensions_of_its_dataset() {
        let (members, index_dimensions) = members().await;
        let station = member(&members, "measurements/station");
        assert_eq!(station.dimensions().names(), index_dimensions);
    }

    #[tokio::test]
    async fn a_member_reads_a_sub_region() {
        let (members, _) = members().await;
        let temp = member(&members, "measurements/temp");
        let read = temp.read(&Region::new(vec![1], vec![2])).await.unwrap();
        assert_eq!(
            read.values().as_primitive::<Float64Type>(),
            &Float64Array::from(vec![11.25, 10.0])
        );
    }
}
