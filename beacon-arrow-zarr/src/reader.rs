use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::array::BooleanArray;
use indexmap::IndexMap;
use nd_arrow_array::{
    NdArrowArray,
    dimensions::{Dimension, Dimensions},
};
use zarrs::{array::Array, array_subset::ArraySubset, group::Group};
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::{
    attributes::AttributeValue,
    decoder::{self, Decoder},
    stream::{ArrowZarrStreamComposer, ArrowZarrStreamComposerRef},
};

pub struct AsyncArrowZarrGroupReader {
    group: Group<dyn AsyncReadableListableStorageTraits>,
    decoders: Vec<Arc<dyn Decoder>>,
    schema: arrow::datatypes::SchemaRef,
    arrays: IndexMap<String, Array<dyn AsyncReadableListableStorageTraits>>,
    attributes: IndexMap<String, AttributeValue>,
}

impl AsyncArrowZarrGroupReader {
    pub async fn new(group: Group<dyn AsyncReadableListableStorageTraits>) -> Result<Self, String> {
        let mut fields = Vec::new();

        let mut attributes = IndexMap::new();
        let mut arrays = IndexMap::new();
        for (attr_name, attr_value) in group.attributes() {
            if let Some(attr_value) = AttributeValue::from_json_value(attr_value) {
                let arrow_data_type = attr_value.arrow_data_type();
                fields.push(arrow::datatypes::Field::new(
                    format!(".{}", attr_name),
                    arrow_data_type,
                    true,
                ));
                attributes.insert(format!(".{}", attr_name), attr_value);
            }
        }

        let group_path = group.path();

        for array in group.async_child_arrays().await.unwrap() {
            let array_json_attributes = array.attributes();
            let array_node_path = array.path().clone();
            let array_name = array_node_path
                .as_str()
                .strip_prefix(group_path.as_str())
                .unwrap()
                .to_string();

            // Strip the / from the start of the array name if present
            let array_name = array_name.strip_prefix('/').unwrap_or(&array_name);

            for (attr_name, attr_value) in array_json_attributes {
                if let Some(attr_value) = AttributeValue::from_json_value(attr_value) {
                    fields.push(arrow::datatypes::Field::new(
                        format!("{}.{}", array_name, attr_name),
                        attr_value.arrow_data_type(),
                        true,
                    ));
                    attributes.insert(format!("{}.{}", array_name, attr_name), attr_value);
                }
            }

            let zarr_data_type = array.data_type();
            if let Ok(arrow_data_type) =
                crate::data_types::try_zarrs_dtype_to_arrow(zarr_data_type.clone())
            {
                fields.push(arrow::datatypes::Field::new(
                    array_name,
                    arrow_data_type,
                    true,
                ));
                arrays.insert(array_name.to_string(), array);
            } else {
                // Unsupported data type, skip adding field
                tracing::warn!(
                    "Skipping array {} with unsupported data type {:?}",
                    array_name,
                    zarr_data_type
                );
            }
        }

        let input_schema = Arc::new(arrow::datatypes::Schema::new(fields.clone()));
        let mut self_reader = Self {
            schema: input_schema.clone(),
            group,
            arrays,
            attributes,
            decoders: Vec::new(),
        };

        let mut decoders = Vec::new();
        // Create decoder for fill values
        let fill_value_decoder = decoder::FillValueDecoder::create(&self_reader, input_schema)?;
        self_reader.schema = fill_value_decoder.decoded_schema();
        decoders.push(Arc::new(fill_value_decoder) as Arc<dyn decoder::Decoder>);
        // Create decoder for CF conventions
        let cf_decoder = decoder::CFDecoder::create(&self_reader, self_reader.schema.clone())?;
        self_reader.schema = cf_decoder.decoded_schema();
        decoders.push(Arc::new(cf_decoder) as Arc<dyn decoder::Decoder>);
        self_reader.decoders = decoders;
        Ok(self_reader)
    }

    pub fn arrow_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    pub fn arrays(&self) -> &IndexMap<String, Array<dyn AsyncReadableListableStorageTraits>> {
        &self.arrays
    }

    pub fn attributes(&self) -> &IndexMap<String, AttributeValue> {
        &self.attributes
    }

    pub fn into_parallel_stream_composer<P: AsRef<[usize]>>(
        self,
        projection: Option<P>,
    ) -> Result<ArrowZarrStreamComposerRef, String> {
        let projected_schema = match projection {
            Some(p) => Arc::new(self.schema.project(p.as_ref()).map_err(|e| e.to_string())?),
            None => self.schema.clone(),
        };

        ArrowZarrStreamComposer::new(self, projected_schema)
    }

    pub fn read_attribute(&self, attribute_name: &str) -> Option<NdArrowArray> {
        self.attributes()
            .get(attribute_name)
            .map(|attr| attr.as_nd_arrow_array())
    }

    pub async fn read_array(
        &self,
        array_name: &str,
        subset: &ArraySubset,
    ) -> Result<Option<NdArrowArray>, String> {
        let array_reader = if let Some(array) = self.arrays().get(array_name) {
            array
        } else {
            return Ok(None);
        };

        let dimensions: Vec<nd_arrow_array::dimensions::Dimension> = array_reader
            .dimension_names()
            .as_ref()
            .unwrap()
            .iter()
            .zip(subset.shape().iter())
            .map(|(dim, &len)| Dimension {
                name: dim.clone().unwrap(),
                size: len as usize,
            })
            .collect::<Vec<_>>();
        let dimensions = Dimensions::new(dimensions);

        let mut maybe_nd_array = match array_reader.data_type() {
            zarrs::array::DataType::Bool => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<bool>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_boolean_array = BooleanArray::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_boolean_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::Int8 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<i8>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_int8_array =
                    arrow::array::Int8Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_int8_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::Int16 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<i16>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_int16_array =
                    arrow::array::Int16Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_int16_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::Int32 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<i32>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_int32_array =
                    arrow::array::Int32Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_int32_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::Int64 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<i64>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_int64_array =
                    arrow::array::Int64Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_int64_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::UInt8 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<u8>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_uint8_array =
                    arrow::array::UInt8Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_uint8_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::UInt16 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<u16>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_uint16_array =
                    arrow::array::UInt16Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_uint16_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::UInt32 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<u32>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_uint32_array =
                    arrow::array::UInt32Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_uint32_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::UInt64 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<u64>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_uint64_array =
                    arrow::array::UInt64Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_uint64_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::Float32 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<f32>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_float32_array =
                    arrow::array::Float32Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_float32_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::Float64 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<f64>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_float64_array =
                    arrow::array::Float64Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_float64_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::String => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<String>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_string_array =
                    arrow::array::StringArray::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_string_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::Bytes => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<Vec<u8>>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_binary_array =
                    arrow::array::BinaryArray::from_iter_values(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_binary_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarr_data_type => Err(format!("Unsupported Zarrs data type: {:?}", zarr_data_type)),
        };

        maybe_nd_array = if let Ok(Some(mut nd_array)) = maybe_nd_array {
            // Apply decoders
            for decoder in &self.decoders {
                nd_array = decoder.decode_array(array_name, nd_array)?;
            }

            Ok(Some(nd_array))
        } else {
            maybe_nd_array
        };

        maybe_nd_array
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use zarrs::{array::Array, array_subset::ArraySubset, group::Group};
    use zarrs_object_store::AsyncObjectStore;
    use zarrs_storage::AsyncReadableListableStorage;

    #[tokio::test]
    async fn test_name() {
        let local_fs = LocalFileSystem::new_with_prefix("./test_files").unwrap();

        let object_store = AsyncObjectStore::new(local_fs);

        let zarr_store: AsyncReadableListableStorage = Arc::new(object_store);

        let group = Group::async_open(zarr_store.clone(), "/gridded-example.zarr")
            .await
            .unwrap();

        println!("{:?}", group.attributes());

        println!("{:?}", group.async_child_array_paths().await.unwrap());
        println!("{:?}", group.async_child_group_paths().await.unwrap());

        let sst_arr = Array::async_open(zarr_store.clone(), "/gridded-example.zarr/analysed_sst")
            .await
            .unwrap();
        println!(
            "SST: {:?}",
            sst_arr
                .chunk_grid()
                .iter_chunk_indices()
                .collect::<Vec<_>>()
        );

        println!("SST attrs: {:?}", sst_arr.attributes());

        let y = sst_arr
            .chunk_grid()
            .iter_chunk_indices()
            .collect::<Vec<_>>();
        let chunk_grid = sst_arr.chunk_grid().clone();

        println!("SST chunk subset: {:?}", sst_arr.chunk_subset(&[0, 3, 1]));
        println!("SST chunk subset: {:?}", chunk_grid.subset(&[0, 3, 1]));

        let lat_arr = Array::async_open(zarr_store.clone(), "/gridded-example.zarr/lat")
            .await
            .unwrap();
        println!("Lat: {:?}", lat_arr.chunk_grid());
        println!("Lat data type: {:?}", lat_arr.data_type());

        let arr = lat_arr
            .async_retrieve_array_subset_ndarray::<f32>(&ArraySubset::new_with_ranges(&[(0..10)]))
            .await
            .unwrap();

        println!("Lat arr: {:?}", arr);

        let time_arr = Array::async_open(zarr_store.clone(), "/gridded-example.zarr/time")
            .await
            .unwrap();

        println!("Time: {:?}", time_arr.chunk_grid());
        println!("Time shape: {:?}", time_arr.chunk_grid_shape());
        println!("Time attrs: {:?}", time_arr.attributes());
        println!("Time data type: {:?}", time_arr.data_type());
    }

    #[tokio::test]
    async fn test_arrow_schema() {
        let local_fs = LocalFileSystem::new_with_prefix("./test_files").unwrap();

        let object_store = AsyncObjectStore::new(local_fs);

        let zarr_store: AsyncReadableListableStorage = Arc::new(object_store);

        let group = Group::async_open(zarr_store.clone(), "/gridded-example.zarr")
            .await
            .unwrap();

        let reader = super::AsyncArrowZarrGroupReader::new(group).await.unwrap();

        let schema = reader.arrow_schema();
        for field in schema.fields() {
            println!("Field: {:?} : {:?}", field.name(), field.data_type());
        }
    }

    #[tokio::test]
    async fn test_read_stream() {
        let local_fs = LocalFileSystem::new_with_prefix("./test_files").unwrap();

        let object_store = AsyncObjectStore::new(local_fs);

        let zarr_store: AsyncReadableListableStorage = Arc::new(object_store);

        let group = Group::async_open(zarr_store.clone(), "/gridded-example.zarr")
            .await
            .unwrap();

        let reader = super::AsyncArrowZarrGroupReader::new(group).await.unwrap();

        let longitude_idx = reader
            .schema
            .index_of("lon")
            .expect("longitude field not found in schema");
        let latitude_idx = reader
            .schema
            .index_of("lat")
            .expect("latitude field not found in schema");

        let analysed_sst_idx = reader
            .schema
            .index_of("analysed_sst")
            .expect("analysed_sst field not found in schema");

        let mut stream = reader
            .into_parallel_stream_composer(Some(&[longitude_idx, latitude_idx, analysed_sst_idx]))
            .unwrap()
            .pollable_shared_stream();

        let next_batch = stream.next().await;

        arrow::util::pretty::print_batches(&[next_batch
            .unwrap()
            .unwrap()
            .to_arrow_record_batch()
            .unwrap()])
        .unwrap();
    }
}
