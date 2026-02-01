use arrow::{
    array::{Array, ArrayRef, FixedSizeListArray, ListArray, PrimitiveArray, UInt32Array},
    datatypes::UInt32Type,
};
use object_store::ObjectStore;
use std::sync::Arc;

use anyhow::Context;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};

use crate::arrow_object_store::ArrowObjectStoreReader;

pub struct DatasetArrayLayout {
    pub dataset_index: u32,
    pub array_indexes: Vec<[u32; 2]>,
    pub chunk_indexes: Vec<Vec<u32>>,
    pub chunk_shape: Vec<u32>,
}

/// In-memory representation of dataset layout metadata stored as Arrow IPC.
pub struct ArrayLayouts {
    dataset_index: PrimitiveArray<UInt32Type>, // u32
    array_index: ListArray,                    // List<FixedSized<u32;2>>
    chunk_index: ListArray,                    // List<List<u32>>
    chunk_shape: ListArray,                    // List<u32>
}

impl ArrayLayouts {
    /// Creates a new `ArrayLayouts` from the given dataset layouts.
    pub fn new(layouts: Vec<DatasetArrayLayout>) -> Self {
        let dataset_index =
            UInt32Array::from(layouts.iter().map(|l| l.dataset_index).collect::<Vec<_>>());
        let array_index =
            Self::vec_fixed_to_array(layouts.iter().map(|l| l.array_indexes.clone()).collect());
        let chunk_index = Self::vecvecvec_to_list_array(
            layouts.iter().map(|l| l.chunk_indexes.clone()).collect(),
        );
        let chunk_shape =
            Self::vecvec_to_list_array(layouts.iter().map(|l| l.chunk_shape.clone()).collect());

        Self {
            dataset_index,
            array_index,
            chunk_index,
            chunk_shape,
        }
    }

    /// Persists the layout as an Arrow IPC file in the given object store.
    ///
    /// # Errors
    /// Returns an error if the IPC writer fails or if the object store write fails.
    pub async fn save<S: ObjectStore>(
        &self,
        store: S,
        path: object_store::path::Path,
    ) -> anyhow::Result<()> {
        // Create RecordBatch
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("dataset_index", DataType::UInt32, false),
            Field::new(
                "array_index",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("value", DataType::UInt32, false)),
                        2,
                    ),
                    false,
                ))),
                false,
            ),
            Field::new(
                "chunk_index",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
                    false,
                ))),
                false,
            ),
            Field::new(
                "chunk_shape",
                DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
                false,
            ),
        ]));

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.dataset_index.clone()),
                Arc::new(self.array_index.clone()),
                Arc::new(self.chunk_index.clone()),
                Arc::new(self.chunk_shape.clone()),
            ],
        )
        .context("failed to build layout record batch")?;

        // Create cursor to write to
        let mut cursor = std::io::Cursor::new(Vec::new());
        let mut writer = arrow::ipc::writer::FileWriter::try_new_with_options(
            &mut cursor,
            &batch.schema(),
            crate::IPC_WRITE_OPTS.clone(),
        )
        .context("failed to create IPC file writer")?;

        writer
            .write(&batch)
            .context("failed to write layout record batch")?;
        writer.finish().context("failed to finish IPC write")?;

        store
            .put(&path, bytes::Bytes::from(cursor.into_inner()).into())
            .await
            .context("failed to store layout IPC object")?;

        Ok(())
    }

    /// Converts a vector of fixed-size pairs into a `ListArray` of `FixedSizeList` values.
    fn vec_fixed_to_array(data: Vec<Vec<[u32; 2]>>) -> ListArray {
        // Flatten values
        let mut flat_values = Vec::new();
        let mut offsets = Vec::with_capacity(data.len() + 1);
        offsets.push(0i32);

        for inner in &data {
            for item in inner {
                flat_values.extend_from_slice(item);
            }
            offsets.push(flat_values.len() as i32 / 2); // each item has 2 u32s
        }

        let values_array = UInt32Array::from(flat_values);
        let value_field = Arc::new(Field::new("value", DataType::UInt32, false));
        let fixed_values = FixedSizeListArray::try_new(
            value_field.clone(),
            2,
            Arc::new(values_array) as ArrayRef,
            None,
        )
        .expect("failed to build FixedSizeListArray for array_index");

        let offsets_buffer = OffsetBuffer::new(offsets.into());

        let field = Field::new("item", DataType::FixedSizeList(value_field, 2), false);

        ListArray::new(
            Arc::new(field),
            offsets_buffer,
            Arc::new(fixed_values),
            None, // null bitmap (None = no null lists)
        )
    }

    /// Converts a `Vec<Vec<u32>>` into a `ListArray` of `UInt32` values.
    fn vecvec_to_list_array(data: Vec<Vec<u32>>) -> ListArray {
        // Flatten values
        let mut flat_values = Vec::new();
        let mut offsets = Vec::with_capacity(data.len() + 1);
        offsets.push(0i32);

        for inner in &data {
            flat_values.extend_from_slice(inner);
            offsets.push(flat_values.len() as i32);
        }

        let values_array = UInt32Array::from(flat_values);

        let offsets_buffer = OffsetBuffer::new(offsets.into());

        let field = Field::new("item", DataType::UInt32, false);

        ListArray::new(
            Arc::new(field),
            offsets_buffer,
            Arc::new(values_array),
            None, // null bitmap (None = no null lists)
        )
    }

    /// Converts a `Vec<Vec<Vec<u32>>>` into a `ListArray` of `List<UInt32>` values.
    fn vecvecvec_to_list_array(data: Vec<Vec<Vec<u32>>>) -> ListArray {
        let mut values = Vec::new();
        let mut inner_offsets = Vec::new();
        inner_offsets.push(0i32);

        for inner_list in data.iter().flatten() {
            values.extend_from_slice(inner_list);
            inner_offsets.push(values.len() as i32);
        }

        let values_array = UInt32Array::from(values);
        let inner_offsets_buffer = OffsetBuffer::new(inner_offsets.into());
        let inner_field = Arc::new(Field::new("item", DataType::UInt32, false));
        let inner_list_array = ListArray::new(
            inner_field,
            inner_offsets_buffer,
            Arc::new(values_array),
            None,
        );

        let mut outer_offsets = Vec::with_capacity(data.len() + 1);
        outer_offsets.push(0i32);
        let mut total_lists = 0i32;
        for lists in &data {
            total_lists += lists.len() as i32;
            outer_offsets.push(total_lists);
        }

        let outer_offsets_buffer = OffsetBuffer::new(outer_offsets.into());
        let outer_field = Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
            false,
        );

        ListArray::new(
            Arc::new(outer_field),
            outer_offsets_buffer,
            Arc::new(inner_list_array),
            None,
        )
    }

    /// Returns the array indexes for the dataset at `index`.
    ///
    /// Returns `None` when the index is out of range or the data is malformed.
    pub fn get_array_indexes(&self, index: usize) -> Option<Vec<[u32; 2]>> {
        if index >= self.array_index.len() {
            return None;
        }
        let list_value = self.array_index.value(index);
        let inner_list = list_value.as_any().downcast_ref::<FixedSizeListArray>()?;

        let mut result = Vec::with_capacity(inner_list.len());
        for i in 0..inner_list.len() {
            let inner_value = inner_list.value(i);
            let values = inner_value.as_any().downcast_ref::<UInt32Array>()?;
            if values.len() < 2 {
                return None;
            }
            result.push([values.value(0), values.value(1)]);
        }
        Some(result)
    }

    /// Returns the chunk indexes for the dataset at `index`.
    ///
    /// Returns `None` when the index is out of range or the data is malformed.
    pub fn get_chunk_indexes(&self, index: usize) -> Option<Vec<Vec<u32>>> {
        if index >= self.chunk_index.len() {
            return None;
        }
        let list_value = self.chunk_index.value(index);
        let inner_list = list_value.as_any().downcast_ref::<ListArray>()?;
        let mut result = Vec::with_capacity(inner_list.len());
        for i in 0..inner_list.len() {
            let inner_value = inner_list.value(i);
            let values = inner_value.as_any().downcast_ref::<UInt32Array>()?;
            result.push((0..values.len()).map(|j| values.value(j)).collect());
        }
        Some(result)
    }

    /// Returns the chunk shape for the dataset at `index`.
    ///
    /// Returns `None` when the index is out of range or the data is malformed.
    pub fn get_chunk_shape(&self, index: usize) -> Option<Vec<u32>> {
        if index >= self.chunk_shape.len() {
            return None;
        }
        let list_value = self.chunk_shape.value(index);
        let values = list_value.as_any().downcast_ref::<UInt32Array>()?;
        Some((0..values.len()).map(|j| values.value(j)).collect())
    }

    /// Finds the dataset index using binary search on sorted `dataset_index` values.
    pub fn find_dataset_array_layout(&self, dataset_index: u32) -> Option<DatasetArrayLayout> {
        let index = self
            .dataset_index
            .values()
            .binary_search(&dataset_index)
            .ok()?;
        Some(DatasetArrayLayout {
            dataset_index,
            array_indexes: self.get_array_indexes(index)?,
            chunk_indexes: self.get_chunk_indexes(index)?,
            chunk_shape: self.get_chunk_shape(index)?,
        })
    }

    /// Loads a `Layout` from an Arrow IPC object.
    ///
    /// # Errors
    /// Returns an error if the object cannot be read, the schema is invalid,
    /// or the IPC content cannot be decoded.
    pub async fn from_object<S: ObjectStore>(
        store: S,
        path: object_store::path::Path,
    ) -> anyhow::Result<Self> {
        let reader = ArrowObjectStoreReader::new(store, path).await?;
        // Read all batches and concatenate them
        let mut batches = Vec::new();
        for i in 0..reader.num_batches() {
            match reader.read_batch(i).await? {
                Some(batch) => batches.push(batch),
                None => break,
            }
        }

        let batch = arrow::compute::concat_batches(&reader.schema(), &batches)
            .context("failed to concatenate layout record batches")?;

        // Check if the schema is as expected
        let expected_fields = vec!["dataset_index", "array_index", "chunk_index", "chunk_shape"];
        for field in expected_fields {
            if batch.schema().field_with_name(field).is_err() {
                return Err(anyhow::anyhow!(
                    "Missing expected field '{}' in layout IPC schema",
                    field
                ));
            }
        }
        let dataset_index = batch
            .column(batch.schema().index_of("dataset_index")?)
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt32Type>>()
            .ok_or_else(|| anyhow::anyhow!("Invalid type for dataset_index"))?
            .clone();
        let array_index = batch
            .column(batch.schema().index_of("array_index")?)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| anyhow::anyhow!("Invalid type for array_index"))?
            .clone();

        let chunk_index = batch
            .column(batch.schema().index_of("chunk_index")?)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| anyhow::anyhow!("Invalid type for chunk_index"))?
            .clone();

        let chunk_shape = batch
            .column(batch.schema().index_of("chunk_shape")?)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| anyhow::anyhow!("Invalid type for chunk_shape"))?
            .clone();

        Ok(Self {
            dataset_index,
            array_index,
            chunk_index,
            chunk_shape,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{ArrayLayouts, DatasetArrayLayout};
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use std::sync::Arc;

    fn sample_layout() -> ArrayLayouts {
        ArrayLayouts::new(vec![
            DatasetArrayLayout {
                dataset_index: 1,
                array_indexes: vec![[0, 1], [2, 3]],
                chunk_indexes: vec![vec![10, 11], vec![12, 13]],
                chunk_shape: vec![2, 2],
            },
            DatasetArrayLayout {
                dataset_index: 2,
                array_indexes: vec![[4, 5]],
                chunk_indexes: vec![vec![20, 21, 22]],
                chunk_shape: vec![3, 3, 3],
            },
        ])
    }

    #[tokio::test]
    async fn save_and_load_roundtrip() -> anyhow::Result<()> {
        let layout = sample_layout();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let path = Path::from("layout.arrow");

        layout.save(store.clone(), path.clone()).await?;
        let loaded = ArrayLayouts::from_object(store, path).await?;

        assert_eq!(
            layout
                .dataset_index
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            loaded
                .dataset_index
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        );
        assert_eq!(layout.get_array_indexes(0), loaded.get_array_indexes(0));
        assert_eq!(layout.get_array_indexes(1), loaded.get_array_indexes(1));
        assert_eq!(layout.get_chunk_indexes(0), loaded.get_chunk_indexes(0));
        assert_eq!(layout.get_chunk_indexes(1), loaded.get_chunk_indexes(1));
        assert_eq!(layout.get_chunk_shape(0), loaded.get_chunk_shape(0));
        assert_eq!(layout.get_chunk_shape(1), loaded.get_chunk_shape(1));
        Ok(())
    }

    #[tokio::test]
    async fn getters_return_none_out_of_range() -> anyhow::Result<()> {
        let layout = sample_layout();
        assert!(layout.get_array_indexes(2).is_none());
        assert!(layout.get_chunk_indexes(2).is_none());
        assert!(layout.get_chunk_shape(2).is_none());
        Ok(())
    }

    #[tokio::test]
    async fn find_layout_by_dataset_index() -> anyhow::Result<()> {
        let layout = sample_layout();
        let found = layout.find_dataset_array_layout(2).expect("dataset exists");
        assert_eq!(found.dataset_index, 2);
        assert_eq!(found.array_indexes, vec![[4, 5]]);
        assert_eq!(found.chunk_indexes, vec![vec![20, 21, 22]]);
        assert_eq!(found.chunk_shape, vec![3, 3, 3]);
        Ok(())
    }
}
