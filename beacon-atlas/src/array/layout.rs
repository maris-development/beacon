use arrow::{
    array::{Array, DictionaryArray, ListArray, PrimitiveArray, StringArray, UInt32Array},
    datatypes::{UInt32Type, UInt64Type},
};
use object_store::ObjectStore;
use std::sync::Arc;

use anyhow::Context;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use std::collections::HashMap;

use crate::arrow_object_store::ArrowObjectStoreReader;

#[derive(Debug, Clone)]
pub struct ArrayLayout {
    pub dataset_index: u32,
    pub array_start: u64,
    pub array_len: u64,
    pub array_shape: Vec<u32>, // Shape of the full array (e.g. [100, 100])
    pub dimensions: Vec<String>, // List of dimension names
}

/// In-memory representation of dataset layout metadata stored as Arrow IPC.
pub struct ArrayLayouts {
    dataset_index: PrimitiveArray<UInt32Type>, // u32
    array_start: PrimitiveArray<UInt64Type>,   // u64
    array_len: PrimitiveArray<UInt64Type>,     // u64>
    array_shape: ListArray,                    // List<u32>
    dimensions: ListArray,                     // List<Dictionary<UInt32, Utf8>>
}

impl ArrayLayouts {
    /// Creates a new `ArrayLayouts` from the given dataset layouts.
    pub fn new(layouts: Vec<ArrayLayout>) -> Self {
        let dataset_index =
            UInt32Array::from(layouts.iter().map(|l| l.dataset_index).collect::<Vec<_>>());
        let array_start = PrimitiveArray::<UInt64Type>::from(
            layouts.iter().map(|l| l.array_start).collect::<Vec<_>>(),
        );
        let array_len = PrimitiveArray::<UInt64Type>::from(
            layouts.iter().map(|l| l.array_len).collect::<Vec<_>>(),
        );
        let array_shape =
            Self::vecvec_to_list_array(layouts.iter().map(|l| l.array_shape.clone()).collect());
        let dimensions =
            Self::vecvecstring_to_dict_list(layouts.iter().map(|l| l.dimensions.clone()).collect());

        Self {
            dataset_index,
            array_start,
            array_len,
            array_shape,
            dimensions,
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
            Field::new("array_start", DataType::UInt64, false),
            Field::new("array_len", DataType::UInt64, false),
            Field::new(
                "array_shape",
                DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
                false,
            ),
            Field::new(
                "dimensions",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                    false,
                ))),
                false,
            ),
        ]));

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.dataset_index.clone()),
                Arc::new(self.array_start.clone()),
                Arc::new(self.array_len.clone()),
                Arc::new(self.array_shape.clone()),
                Arc::new(self.dimensions.clone()),
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

    /// Converts a `Vec<Vec<String>>` into a `ListArray` of dictionary-encoded strings.
    fn vecvecstring_to_dict_list(data: Vec<Vec<String>>) -> ListArray {
        let mut offsets = Vec::with_capacity(data.len() + 1);
        offsets.push(0i32);

        let mut dict_values: Vec<String> = Vec::new();
        let mut keys: Vec<u32> = Vec::new();
        let mut dict_index = HashMap::<String, u32>::new();

        for inner in &data {
            for value in inner {
                let key = match dict_index.get(value) {
                    Some(key) => *key,
                    None => {
                        let key = dict_values.len() as u32;
                        dict_values.push(value.clone());
                        dict_index.insert(value.clone(), key);
                        key
                    }
                };
                keys.push(key);
            }
            offsets.push(keys.len() as i32);
        }

        let keys_array = UInt32Array::from(keys);
        let values_array = StringArray::from(dict_values);
        let dict_array = DictionaryArray::<UInt32Type>::try_new(keys_array, Arc::new(values_array))
            .expect("failed to build dictionary array for dimensions");

        let offsets_buffer = OffsetBuffer::new(offsets.into());
        let field = Field::new(
            "item",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            false,
        );

        ListArray::new(Arc::new(field), offsets_buffer, Arc::new(dict_array), None)
    }

    /// Returns the array shape for the dataset at `index`.
    ///
    /// Returns `None` when the index is out of range or the data is malformed.
    pub fn get_array_shape(&self, index: usize) -> Option<Vec<u32>> {
        if index >= self.array_shape.len() {
            return None;
        }
        let list_value = self.array_shape.value(index);
        let values = list_value.as_any().downcast_ref::<UInt32Array>()?;
        Some((0..values.len()).map(|j| values.value(j)).collect())
    }

    /// Returns the dimensions for the dataset at `index`.
    ///
    /// Returns `None` when the index is out of range or the data is malformed.
    pub fn get_dimensions(&self, index: usize) -> Option<Vec<String>> {
        if index >= self.dimensions.len() {
            return None;
        }
        let list_value = self.dimensions.value(index);
        let dict_array = list_value
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()?;
        if dict_array.null_count() > 0 {
            return None;
        }
        let values = dict_array.values();
        let values = values.as_any().downcast_ref::<StringArray>()?;
        let mut result = Vec::with_capacity(dict_array.len());
        let keys = dict_array.keys();
        for i in 0..dict_array.len() {
            let key = keys.value(i) as usize;
            result.push(values.value(key).to_string());
        }
        Some(result)
    }

    /// Finds the dataset index using binary search on sorted `dataset_index` values.
    pub fn find_dataset_array_layout(&self, dataset_index: u32) -> Option<ArrayLayout> {
        let index = self
            .dataset_index
            .values()
            .binary_search(&dataset_index)
            .ok()?;
        Some(ArrayLayout {
            dataset_index,
            array_start: self.array_start.value(index),
            array_len: self.array_len.value(index),
            array_shape: self.get_array_shape(index)?,
            dimensions: self.get_dimensions(index)?,
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
        let expected_fields = vec![
            "dataset_index",
            "array_start",
            "array_len",
            "array_shape",
            "dimensions",
        ];
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
        let array_start = batch
            .column(batch.schema().index_of("array_start")?)
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt64Type>>()
            .ok_or_else(|| anyhow::anyhow!("Invalid type for array_start"))?
            .clone();

        let array_len = batch
            .column(batch.schema().index_of("array_len")?)
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt64Type>>()
            .ok_or_else(|| anyhow::anyhow!("Invalid type for array_len"))?
            .clone();

        let array_shape = batch
            .column(batch.schema().index_of("array_shape")?)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| anyhow::anyhow!("Invalid type for array_shape"))?
            .clone();

        let dimensions = batch
            .column(batch.schema().index_of("dimensions")?)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| anyhow::anyhow!("Invalid type for dimensions"))?
            .clone();

        Ok(Self {
            dataset_index,
            array_start,
            array_len,
            array_shape,
            dimensions,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{ArrayLayout, ArrayLayouts};
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use std::sync::Arc;

    fn sample_layout() -> ArrayLayouts {
        ArrayLayouts::new(vec![
            ArrayLayout {
                dataset_index: 1,
                array_start: 0,
                array_len: 8,
                array_shape: vec![4, 4],
                dimensions: vec!["x".to_string(), "y".to_string()],
            },
            ArrayLayout {
                dataset_index: 2,
                array_start: 8,
                array_len: 27,
                array_shape: vec![9, 9, 9],
                dimensions: vec!["x".to_string(), "y".to_string(), "z".to_string()],
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
        assert_eq!(
            layout
                .array_start
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            loaded
                .array_start
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        );
        assert_eq!(
            layout
                .array_len
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            loaded
                .array_len
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        );
        assert_eq!(layout.get_array_shape(0), loaded.get_array_shape(0));
        assert_eq!(layout.get_array_shape(1), loaded.get_array_shape(1));
        assert_eq!(layout.get_dimensions(0), loaded.get_dimensions(0));
        assert_eq!(layout.get_dimensions(1), loaded.get_dimensions(1));
        Ok(())
    }

    #[tokio::test]
    async fn getters_return_none_out_of_range() -> anyhow::Result<()> {
        let layout = sample_layout();
        assert!(layout.get_array_shape(2).is_none());
        assert!(layout.get_dimensions(2).is_none());
        Ok(())
    }

    #[tokio::test]
    async fn find_layout_by_dataset_index() -> anyhow::Result<()> {
        let layout = sample_layout();
        let found = layout.find_dataset_array_layout(2).expect("dataset exists");
        assert_eq!(found.dataset_index, 2);
        assert_eq!(found.array_start, 8);
        assert_eq!(found.array_len, 27);
        assert_eq!(found.array_shape, vec![9, 9, 9]);
        assert_eq!(
            found.dimensions,
            vec!["x".to_string(), "y".to_string(), "z".to_string()]
        );
        Ok(())
    }
}
