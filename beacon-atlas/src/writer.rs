use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use anyhow::anyhow;
use array_format::{
    ArrayData, BinaryArray, CompressionCodec, DType, PrimitiveArray, Storage, StringArray, Writer,
    WriterConfig,
};
use beacon_nd_array::array::subset::ArraySubset;
use beacon_nd_array::dataset::Dataset;
use beacon_nd_array::datatypes::{NdArrayDataType, TimestampNanosecond};
use beacon_nd_array::{NdArray, NdArrayD};
use bytes::Bytes;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use object_store::path::Path;

use crate::schema::{ColumnInput, DatasetEntry, SchemaStore};

// ─── Storage Adapter ──────────────────────────────────────────────────────────

/// Adapter implementing `array_format::Storage` over the workspace `object_store` crate.
#[derive(Clone)]
pub(crate) struct ObjStoreStorage {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) path: Path,
}

impl Storage for ObjStoreStorage {
    fn read_range(&self, range: Range<u64>) -> BoxFuture<'_, array_format::Result<Bytes>> {
        Box::pin(async move {
            let bytes = self
                .store
                .get_range(&self.path, range)
                .await
                .map_err(|e| array_format::Error::Storage(e.to_string()))?;
            Ok(bytes)
        })
    }

    fn write(&self, data: Bytes) -> BoxFuture<'_, array_format::Result<()>> {
        Box::pin(async move {
            self.store
                .put(&self.path, data.into())
                .await
                .map_err(|e| array_format::Error::Storage(e.to_string()))?;
            Ok(())
        })
    }

    fn size(&self) -> BoxFuture<'_, array_format::Result<u64>> {
        Box::pin(async move {
            let meta = self
                .store
                .head(&self.path)
                .await
                .map_err(|e| array_format::Error::Storage(e.to_string()))?;
            Ok(meta.size as u64)
        })
    }
}

/// An atlas writer that stores datasets as per-array array-format files
/// with an interned schema store for column definitions.
///
/// Each array across all datasets lives in a single `.arrf` file at
/// `{base_path}/columns/{array_name}.arrf`. All arrays (including those with
/// dotted names like `"temperature.units"` or `".convention"`) are stored
/// as data arrays — there is no special attribute extraction.
pub struct AtlasWriter<C: CompressionCodec> {
    schema_store: SchemaStore,
    column_writers: HashMap<String, Writer<C>>,
    storage: Arc<dyn ObjectStore>,
    base_path: Path,
    block_target_size: usize,
    codec: C,
}

impl<C: CompressionCodec + Clone> AtlasWriter<C> {
    /// Create a new writer with an empty schema store.
    pub fn new(
        storage: Arc<dyn ObjectStore>,
        base_path: Path,
        codec: C,
        block_target_size: usize,
    ) -> Self {
        Self {
            schema_store: SchemaStore::new(),
            column_writers: HashMap::new(),
            storage,
            base_path,
            block_target_size,
            codec,
        }
    }

    /// Open an existing atlas collection, loading the schema store from
    /// `{base_path}/schema.bin`. Column writers are opened lazily on first write.
    pub async fn open(
        storage: Arc<dyn ObjectStore>,
        base_path: Path,
        codec: C,
        block_target_size: usize,
    ) -> anyhow::Result<Self> {
        let schema_path = Path::from(format!("{}/schema.bin", base_path));
        let schema_store =
            SchemaStore::load_from_object_store(storage.as_ref(), &schema_path).await?;

        Ok(Self {
            schema_store,
            column_writers: HashMap::new(),
            storage,
            base_path,
            block_target_size,
            codec,
        })
    }

    /// Write a dataset to the atlas.
    ///
    /// All arrays in the dataset are written to per-array `.arrf` files and
    /// registered in the schema store with their names and data types.
    ///
    /// Returns the [`DatasetEntry`] with the assigned dataset id and schema id.
    pub async fn write_dataset(&mut self, dataset: &Dataset) -> anyhow::Result<DatasetEntry> {
        // Build ColumnInput list for schema registration — every array is a "column"
        let column_inputs: Vec<ColumnInput> = dataset
            .arrays
            .iter()
            .map(|(name, array)| ColumnInput {
                name: name.clone(),
                data_type: array.datatype(),
            })
            .collect();

        // Register schema
        let entry = self
            .schema_store
            .register_schema(&dataset.name, column_inputs);

        // Write all arrays to per-array .arrf files
        for (array_name, array) in &dataset.arrays {
            let dimensions = array.dimensions();
            let shape = array.shape();
            let chunk_shape = array.chunk_shape();

            let writer = self.get_or_create_column_writer(array_name).await?;

            let shape_u32: Vec<u32> = shape.iter().map(|&s| s as u32).collect();

            if chunk_shape == shape {
                // Flat (unchunked) array — write in one shot
                let array_data = convert_ndarray_to_array_data(array.as_ref()).await?;
                writer.write_array(
                    &dataset.name,
                    dimensions,
                    shape_u32,
                    None,
                    array_data.as_ref(),
                )?;
            } else {
                // Chunked array — write each chunk separately
                let dtype = ndarray_dtype_to_arrf_dtype(array.datatype());
                let chunk_shape_u32: Vec<u32> = chunk_shape.iter().map(|&s| s as u32).collect();
                let mut chunked_writer = writer.begin_chunked_array(
                    &dataset.name,
                    dtype,
                    dimensions,
                    shape_u32,
                    chunk_shape_u32,
                    None,
                )?;

                let subsets = generate_chunk_subsets(&shape, &chunk_shape);
                for subset in subsets {
                    let coord: Vec<u32> = subset
                        .start
                        .iter()
                        .zip(chunk_shape.iter())
                        .map(|(&start, &cs)| (start / cs) as u32)
                        .collect();

                    let chunk_array = array.subset(subset).await?;
                    let chunk_data = convert_ndarray_to_array_data(chunk_array.as_ref()).await?;
                    chunked_writer.write_array(coord, chunk_data.as_ref())?;
                }
            }
        }

        Ok(entry)
    }

    /// Remove a dataset from the schema store and delete its arrays from column files.
    pub fn remove_dataset(&mut self, dataset_name: &str) -> anyhow::Result<Option<DatasetEntry>> {
        let entry = self.schema_store.remove_dataset(dataset_name);

        // Mark arrays as deleted in column writers that are currently open
        for writer in self.column_writers.values_mut() {
            let _ = writer.delete(dataset_name);
        }

        Ok(entry)
    }

    /// Flush all open column writers and persist the schema store.
    pub async fn flush(&mut self) -> anyhow::Result<()> {
        for writer in self.column_writers.values_mut() {
            writer.flush().await?;
        }

        let schema_path = Path::from(format!("{}/schema.bin", self.base_path));
        self.schema_store
            .save_to_object_store(self.storage.as_ref(), &schema_path)
            .await?;

        Ok(())
    }

    /// Get a reference to the schema store.
    pub fn schema_store(&self) -> &SchemaStore {
        &self.schema_store
    }

    /// Get a mutable reference to the schema store.
    pub fn schema_store_mut(&mut self) -> &mut SchemaStore {
        &mut self.schema_store
    }

    async fn get_or_create_column_writer(
        &mut self,
        column_name: &str,
    ) -> anyhow::Result<&mut Writer<C>> {
        if !self.column_writers.contains_key(column_name) {
            let col_path = array_name_to_path(&self.base_path, column_name);
            let backend = ObjStoreStorage {
                store: self.storage.clone(),
                path: col_path.clone(),
            };

            let config = WriterConfig {
                block_target_size: self.block_target_size,
                codec: self.codec.clone(),
            };

            // Try to open existing file, fall back to creating new
            let writer = match Writer::open(backend.clone(), config).await {
                Ok(w) => w,
                Err(_) => {
                    let config = WriterConfig {
                        block_target_size: self.block_target_size,
                        codec: self.codec.clone(),
                    };
                    Writer::new(backend, config)
                }
            };

            self.column_writers.insert(column_name.to_string(), writer);
        }

        Ok(self.column_writers.get_mut(column_name).unwrap())
    }
}

// ─── Path Helpers ─────────────────────────────────────────────────────────────

/// Convert an array name to its storage path.
///
/// Dots in array names become directory separators so that related arrays
/// are stored hierarchically, avoiding huge fan-out in a single directory:
///
/// - `"temperature"` → `{base}/columns/temperature.arrf`
/// - `"temperature.units"` → `{base}/columns/temperature/units.arrf`
/// - `".convention"` → `{base}/columns/.convention.arrf`
pub(crate) fn array_name_to_path(base_path: &Path, array_name: &str) -> Path {
    // Global attributes start with '.' — don't split the leading dot
    let relative = if let Some(rest) = array_name.strip_prefix('.') {
        // Global attribute: ".convention" → ".convention.arrf"
        format!("columns/.{}.arrf", rest)
    } else if array_name.contains('.') {
        // Nested: "temperature.units" → "columns/temperature/units.arrf"
        let parts: Vec<&str> = array_name.splitn(2, '.').collect();
        format!("columns/{}/{}.arrf", parts[0], parts[1].replace('.', "/"))
    } else {
        // Plain column: "temperature" → "columns/temperature.arrf"
        format!("columns/{}.arrf", array_name)
    };
    Path::from(format!("{}/{}", base_path, relative))
}

// ─── Conversion Helpers ───────────────────────────────────────────────────────

/// Convert an `NdArrayD` to an `array_format::ArrayData` boxed trait object.
pub(crate) async fn convert_ndarray_to_array_data(
    array: &dyn NdArrayD,
) -> anyhow::Result<Box<dyn ArrayData>> {
    macro_rules! convert_primitive {
        ($array:expr, $rust_ty:ty, $label:expr) => {{
            let nd = $array
                .as_any()
                .downcast_ref::<NdArray<$rust_ty>>()
                .ok_or_else(|| anyhow!("Failed to downcast NdArray to NdArray<{}>", $label))?;
            let values = nd.clone_into_raw_vec().await;
            Ok(Box::new(PrimitiveArray::from_slice(&values)) as Box<dyn ArrayData>)
        }};
    }

    match array.datatype() {
        NdArrayDataType::Bool => {
            let nd = array
                .as_any()
                .downcast_ref::<NdArray<bool>>()
                .ok_or_else(|| anyhow!("Failed to downcast NdArray to NdArray<bool>"))?;
            let values: Vec<u8> = nd
                .clone_into_raw_vec()
                .await
                .into_iter()
                .map(|b| b as u8)
                .collect();
            Ok(Box::new(PrimitiveArray::from_slice(&values)) as Box<dyn ArrayData>)
        }
        NdArrayDataType::I8 => convert_primitive!(array, i8, "i8"),
        NdArrayDataType::I16 => convert_primitive!(array, i16, "i16"),
        NdArrayDataType::I32 => convert_primitive!(array, i32, "i32"),
        NdArrayDataType::I64 => convert_primitive!(array, i64, "i64"),
        NdArrayDataType::U8 => convert_primitive!(array, u8, "u8"),
        NdArrayDataType::U16 => convert_primitive!(array, u16, "u16"),
        NdArrayDataType::U32 => convert_primitive!(array, u32, "u32"),
        NdArrayDataType::U64 => convert_primitive!(array, u64, "u64"),
        NdArrayDataType::F32 => convert_primitive!(array, f32, "f32"),
        NdArrayDataType::F64 => convert_primitive!(array, f64, "f64"),
        NdArrayDataType::Timestamp => {
            let nd = array
                .as_any()
                .downcast_ref::<NdArray<TimestampNanosecond>>()
                .ok_or_else(|| {
                    anyhow!("Failed to downcast NdArray to NdArray<TimestampNanosecond>")
                })?;
            let values: Vec<i64> = nd
                .clone_into_raw_vec()
                .await
                .into_iter()
                .map(|t| t.0)
                .collect();
            Ok(Box::new(PrimitiveArray::from_slice(&values)) as Box<dyn ArrayData>)
        }
        NdArrayDataType::String => {
            let nd = array
                .as_any()
                .downcast_ref::<NdArray<String>>()
                .ok_or_else(|| anyhow!("Failed to downcast NdArray to NdArray<String>"))?;
            let values = nd.clone_into_raw_vec().await;
            let str_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
            Ok(Box::new(StringArray::from_slices(&str_refs)) as Box<dyn ArrayData>)
        }
        NdArrayDataType::Binary => {
            let nd = array
                .as_any()
                .downcast_ref::<NdArray<Vec<u8>>>()
                .ok_or_else(|| anyhow!("Failed to downcast NdArray to NdArray<Vec<u8>>"))?;
            let values = nd.clone_into_raw_vec().await;
            let byte_refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
            Ok(Box::new(BinaryArray::from_slices(&byte_refs)) as Box<dyn ArrayData>)
        }
    }
}

// ─── Chunking Helpers ─────────────────────────────────────────────────────────

/// Map `NdArrayDataType` to `array_format::DType`.
pub(crate) fn ndarray_dtype_to_arrf_dtype(dt: NdArrayDataType) -> DType {
    match dt {
        NdArrayDataType::Bool => DType::UInt8,
        NdArrayDataType::I8 => DType::Int8,
        NdArrayDataType::I16 => DType::Int16,
        NdArrayDataType::I32 => DType::Int32,
        NdArrayDataType::I64 => DType::Int64,
        NdArrayDataType::U8 => DType::UInt8,
        NdArrayDataType::U16 => DType::UInt16,
        NdArrayDataType::U32 => DType::UInt32,
        NdArrayDataType::U64 => DType::UInt64,
        NdArrayDataType::F32 => DType::Float32,
        NdArrayDataType::F64 => DType::Float64,
        NdArrayDataType::Timestamp => DType::Int64,
        NdArrayDataType::String => DType::String,
        NdArrayDataType::Binary => DType::Binary,
    }
}

/// Generate all chunk subsets that tile a given shape with the given chunk shape.
fn generate_chunk_subsets(shape: &[usize], chunk_shape: &[usize]) -> Vec<ArraySubset> {
    if shape.is_empty() {
        return vec![ArraySubset::new(vec![], vec![])];
    }

    let chunk_counts: Vec<usize> = shape
        .iter()
        .zip(chunk_shape.iter())
        .map(|(&axis_len, &axis_chunk)| {
            if axis_len == 0 {
                0
            } else {
                axis_len.div_ceil(axis_chunk.max(1))
            }
        })
        .collect();

    if chunk_counts.contains(&0) {
        return vec![];
    }

    let total_chunks: usize = chunk_counts.iter().product();
    let mut subsets = Vec::with_capacity(total_chunks);

    for linear_idx in 0..total_chunks {
        let mut rem = linear_idx;
        let mut chunk_index = vec![0usize; shape.len()];
        for axis in (0..shape.len()).rev() {
            chunk_index[axis] = rem % chunk_counts[axis];
            rem /= chunk_counts[axis];
        }

        let start: Vec<usize> = chunk_index
            .iter()
            .zip(chunk_shape.iter())
            .map(|(&ci, &cs)| ci * cs.max(1))
            .collect();

        let sub_shape: Vec<usize> = shape
            .iter()
            .zip(start.iter())
            .zip(chunk_shape.iter())
            .map(|((&axis_len, &axis_start), &axis_chunk)| {
                axis_chunk.max(1).min(axis_len - axis_start)
            })
            .collect();

        subsets.push(ArraySubset::new(start, sub_shape));
    }

    subsets
}

#[cfg(test)]
mod tests {
    use super::*;
    use array_format::NoCompression;
    use beacon_nd_array::NdArray;
    use indexmap::IndexMap;
    use object_store::memory::InMemory;

    /// Helper: build a Dataset from named arrays.
    async fn make_dataset(name: &str, arrays: Vec<(&str, Arc<dyn NdArrayD>)>) -> Dataset {
        let map: IndexMap<String, Arc<dyn NdArrayD>> = arrays
            .into_iter()
            .map(|(n, a)| (n.to_string(), a))
            .collect();
        Dataset::new(name.to_string(), map).await
    }

    fn new_writer(store: Arc<InMemory>) -> AtlasWriter<NoCompression> {
        AtlasWriter::new(store, Path::from("test-atlas"), NoCompression, 64 * 1024)
    }

    #[tokio::test]
    async fn test_write_simple_dataset() {
        let store = Arc::new(InMemory::new());
        let mut writer = new_writer(store);

        let temp = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0, 3.0],
            vec![3],
            vec!["obs".to_string()],
            None,
        )
        .unwrap();

        let ds = make_dataset("ds-1", vec![("temperature", Arc::new(temp))]).await;
        let entry = writer.write_dataset(&ds).await.unwrap();

        assert_eq!(entry.dataset_id.as_u32(), 0);
        let resolved = writer.schema_store().get_schema("ds-1").unwrap();
        assert_eq!(resolved.columns.len(), 1);
        assert_eq!(resolved.columns[0].name, "temperature");
    }

    #[tokio::test]
    async fn test_write_dataset_with_attribute_arrays() {
        let store = Arc::new(InMemory::new());
        let mut writer = new_writer(store);

        let temp = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![15.0, 16.0],
            vec![2],
            vec!["obs".to_string()],
            None,
        )
        .unwrap();

        let units = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["celsius".to_string()],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();

        let ds = make_dataset(
            "ds-attrs",
            vec![
                ("temperature", Arc::new(temp)),
                ("temperature.units", Arc::new(units)),
            ],
        )
        .await;

        let entry = writer.write_dataset(&ds).await.unwrap();
        writer.flush().await.unwrap();
        let resolved = writer.schema_store().get_schema("ds-attrs").unwrap();

        // Both arrays are stored as columns in the schema
        assert_eq!(resolved.columns.len(), 2);
        let names: Vec<&str> = resolved.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"temperature"));
        assert!(names.contains(&"temperature.units"));
        assert_eq!(entry.dataset_id.as_u32(), 0);

        // Verify hierarchical path: "temperature.units" stored under temperature/ dir
        let base = Path::from("test-atlas");
        let temp_path = array_name_to_path(&base, "temperature");
        let units_path = array_name_to_path(&base, "temperature.units");
        assert_eq!(temp_path.to_string(), "test-atlas/columns/temperature.arrf");
        assert_eq!(
            units_path.to_string(),
            "test-atlas/columns/temperature/units.arrf"
        );
    }

    #[tokio::test]
    async fn test_write_dataset_with_global_attribute_arrays() {
        let store = Arc::new(InMemory::new());
        let mut writer = new_writer(store);

        let temp = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![10.0],
            vec![1],
            vec!["obs".to_string()],
            None,
        )
        .unwrap();

        let convention = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["CF-1.6".to_string()],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();

        let ds = make_dataset(
            "ds-global",
            vec![
                ("temperature", Arc::new(temp)),
                (".convention", Arc::new(convention)),
            ],
        )
        .await;

        let entry = writer.write_dataset(&ds).await.unwrap();
        let resolved = writer.schema_store().get_schema("ds-global").unwrap();

        // Both arrays are columns — no special treatment for globals
        assert_eq!(resolved.columns.len(), 2);
        let names: Vec<&str> = resolved.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"temperature"));
        assert!(names.contains(&".convention"));
        assert_eq!(entry.dataset_id.as_u32(), 0);
    }

    #[tokio::test]
    async fn test_write_multiple_datasets() {
        let store = Arc::new(InMemory::new());
        let mut writer = new_writer(store);

        let temp1 = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0],
            vec![2],
            vec!["obs".to_string()],
            None,
        )
        .unwrap();

        let temp2 = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![3.0, 4.0, 5.0],
            vec![3],
            vec!["obs".to_string()],
            None,
        )
        .unwrap();

        let ds1 = make_dataset("ds-1", vec![("temperature", Arc::new(temp1))]).await;
        let ds2 = make_dataset("ds-2", vec![("temperature", Arc::new(temp2))]).await;

        let entry1 = writer.write_dataset(&ds1).await.unwrap();
        let entry2 = writer.write_dataset(&ds2).await.unwrap();

        assert_ne!(entry1.dataset_id, entry2.dataset_id);
        assert_eq!(entry1.schema_id, entry2.schema_id);
        assert_eq!(writer.schema_store().dataset_count(), 2);
    }

    #[tokio::test]
    async fn test_remove_dataset() {
        let store = Arc::new(InMemory::new());
        let mut writer = new_writer(store);

        let arr = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0],
            vec![1],
            vec!["x".to_string()],
            None,
        )
        .unwrap();

        let ds = make_dataset("to-remove", vec![("value", Arc::new(arr))]).await;
        writer.write_dataset(&ds).await.unwrap();
        assert_eq!(writer.schema_store().dataset_count(), 1);

        let removed = writer.remove_dataset("to-remove").unwrap();
        assert!(removed.is_some());
        assert_eq!(writer.schema_store().dataset_count(), 0);

        let removed2 = writer.remove_dataset("to-remove").unwrap();
        assert!(removed2.is_none());
    }

    #[tokio::test]
    async fn test_flush_and_reopen() {
        let store = Arc::new(InMemory::new());

        {
            let mut writer = AtlasWriter::new(
                store.clone(),
                Path::from("test-atlas"),
                NoCompression,
                64 * 1024,
            );

            let arr = NdArray::<f64>::try_new_from_vec_in_mem(
                vec![1.0, 2.0],
                vec![2],
                vec!["obs".to_string()],
                None,
            )
            .unwrap();

            let ds = make_dataset("persisted", vec![("value", Arc::new(arr))]).await;
            writer.write_dataset(&ds).await.unwrap();
            writer.flush().await.unwrap();
        }

        let reopened = AtlasWriter::open(
            store.clone(),
            Path::from("test-atlas"),
            NoCompression,
            64 * 1024,
        )
        .await
        .unwrap();

        assert_eq!(reopened.schema_store().dataset_count(), 1);
        let resolved = reopened.schema_store().get_schema("persisted").unwrap();
        assert_eq!(resolved.columns.len(), 1);
        assert_eq!(resolved.columns[0].name, "value");
    }

    #[tokio::test]
    async fn test_all_arrays_written_to_files() {
        let store = Arc::new(InMemory::new());
        let mut writer = new_writer(store);

        let temp = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![5.0],
            vec![1],
            vec!["obs".to_string()],
            None,
        )
        .unwrap();

        let global =
            NdArray::<i32>::try_new_from_vec_in_mem(vec![42], vec![1], vec![] as Vec<String>, None)
                .unwrap();

        let ds = make_dataset(
            "ds-all-written",
            vec![("data", Arc::new(temp)), (".version", Arc::new(global))],
        )
        .await;

        writer.write_dataset(&ds).await.unwrap();

        // Both arrays should have column writers (both written to .arrf files)
        assert!(writer.column_writers.contains_key("data"));
        assert!(writer.column_writers.contains_key(".version"));
    }

    #[tokio::test]
    async fn test_schema_dedup() {
        let store = Arc::new(InMemory::new());
        let mut writer = new_writer(store);

        // Two datasets with same array names and types → same schema
        for name in ["ds-a", "ds-b"] {
            let arr = NdArray::<f64>::try_new_from_vec_in_mem(
                vec![1.0],
                vec![1],
                vec!["x".to_string()],
                None,
            )
            .unwrap();

            let ga = NdArray::<String>::try_new_from_vec_in_mem(
                vec!["CF-1.6".to_string()],
                vec![1],
                vec![] as Vec<String>,
                None,
            )
            .unwrap();

            let ds = make_dataset(
                name,
                vec![("value", Arc::new(arr)), (".convention", Arc::new(ga))],
            )
            .await;
            writer.write_dataset(&ds).await.unwrap();
        }

        let entry_a = writer.schema_store().get_dataset_entry("ds-a").unwrap();
        let entry_b = writer.schema_store().get_dataset_entry("ds-b").unwrap();
        assert_eq!(entry_a.schema_id, entry_b.schema_id);

        // Different columns → different schema
        let arr = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0],
            vec![1],
            vec!["x".to_string()],
            None,
        )
        .unwrap();

        let ds = make_dataset("ds-c", vec![("other_col", Arc::new(arr))]).await;
        writer.write_dataset(&ds).await.unwrap();

        let entry_c = writer.schema_store().get_dataset_entry("ds-c").unwrap();
        assert_ne!(entry_a.schema_id, entry_c.schema_id);
    }
}
