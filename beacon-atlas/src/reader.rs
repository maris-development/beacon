use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use array_format::Reader;
use beacon_nd_array::NdArrayD;
use beacon_nd_array::dataset::Dataset;
use beacon_nd_array::datatypes::NdArrayDataType;
use indexmap::IndexMap;
use object_store::ObjectStore;
use object_store::path::Path;
use parking_lot::RwLock;
use tokio::sync::OnceCell;

use crate::array::lazy_ndarray_dyn;
use crate::schema::{ResolvedSchema, SchemaStore};
use crate::writer::{ObjStoreStorage, array_name_to_path};

// ─── AtlasReader ──────────────────────────────────────────────────────────────

/// A concurrent reader for atlas collections.
///
/// Opens per-array `.arrf` files lazily on first access and caches them for
/// reuse across subsequent reads. Thread-safe: multiple tasks can read
/// different (or the same) datasets concurrently.
///
/// All arrays (including those with dotted names like `"temperature.units"` or
/// `".convention"`) are read directly from their `.arrf` files.
pub struct AtlasReader {
    schema_store: SchemaStore,
    /// Lazy cache of opened `array_format::Reader` instances, keyed by array name.
    column_readers: RwLock<HashMap<String, Arc<OnceCell<Arc<Reader>>>>>,
    storage: Arc<dyn ObjectStore>,
    base_path: Path,
    cache_capacity_bytes: u64,
}

impl AtlasReader {
    /// Open an existing atlas collection for reading.
    ///
    /// Loads the schema store from `{base_path}/schema.bin`. Column `.arrf`
    /// files are opened lazily on first read.
    pub async fn open(
        storage: Arc<dyn ObjectStore>,
        base_path: Path,
        cache_capacity_bytes: u64,
    ) -> anyhow::Result<Self> {
        let schema_path = Path::from(format!("{}/schema.bin", base_path));
        let schema_store =
            SchemaStore::load_from_object_store(storage.as_ref(), &schema_path).await?;

        Ok(Self {
            schema_store,
            column_readers: RwLock::new(HashMap::new()),
            storage,
            base_path,
            cache_capacity_bytes,
        })
    }

    /// Read a full dataset by name, returning all arrays.
    pub async fn read_dataset(&self, dataset_name: &str) -> anyhow::Result<Dataset> {
        let schema = self
            .schema_store
            .get_schema(dataset_name)
            .ok_or_else(|| anyhow!("Dataset not found: {}", dataset_name))?;

        let array_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        self.read_dataset_impl(dataset_name, &schema, &array_names)
            .await
    }

    /// Read a dataset with only the specified arrays.
    ///
    /// Only the `.arrf` files for the requested arrays are opened.
    pub async fn read_dataset_projected(
        &self,
        dataset_name: &str,
        columns: &[&str],
    ) -> anyhow::Result<Dataset> {
        let schema = self
            .schema_store
            .get_schema(dataset_name)
            .ok_or_else(|| anyhow!("Dataset not found: {}", dataset_name))?;

        // Validate requested columns exist in schema
        for &col in columns {
            if !schema.columns.iter().any(|c| c.name == col) {
                anyhow::bail!("Column '{}' not found in dataset '{}'", col, dataset_name);
            }
        }

        self.read_dataset_impl(dataset_name, &schema, columns).await
    }

    /// Read specific arrays from a dataset by name, returning them as a map.
    ///
    /// Only the `.arrf` files for the requested arrays are opened.
    pub async fn read_dataset_arrays(
        &self,
        dataset_name: &str,
        array_names: &[&str],
    ) -> anyhow::Result<IndexMap<String, Arc<dyn NdArrayD>>> {
        let schema = self
            .schema_store
            .get_schema(dataset_name)
            .ok_or_else(|| anyhow!("Dataset not found: {}", dataset_name))?;

        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();

        for &name in array_names {
            let col = schema
                .columns
                .iter()
                .find(|c| c.name == name)
                .ok_or_else(|| {
                    anyhow!("Array '{}' not found in dataset '{}'", name, dataset_name)
                })?;

            let reader = self.get_or_open_column_reader(name).await?;
            let nd = lazy_ndarray_dyn(reader, dataset_name, col.data_type.clone())?;
            arrays.insert(name.to_string(), nd);
        }

        Ok(arrays)
    }

    /// Get a reference to the schema store.
    pub fn schema_store(&self) -> &SchemaStore {
        &self.schema_store
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    async fn read_dataset_impl(
        &self,
        dataset_name: &str,
        schema: &ResolvedSchema,
        columns: &[&str],
    ) -> anyhow::Result<Dataset> {
        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();

        for &col_name in columns {
            let col = schema
                .columns
                .iter()
                .find(|c| c.name == col_name)
                .ok_or_else(|| anyhow!("Column '{}' not in schema", col_name))?;

            let reader = self.get_or_open_column_reader(col_name).await?;
            let nd = lazy_ndarray_dyn(reader, dataset_name, col.data_type.clone())?;
            arrays.insert(col_name.to_string(), nd);
        }

        Ok(Dataset::new(dataset_name.to_string(), arrays).await)
    }

    /// Lazily open an `array_format::Reader` for the given array name.
    ///
    /// If multiple tasks call this for the same array concurrently, only one
    /// will actually open the file — the others wait on the `OnceCell`.
    async fn get_or_open_column_reader(&self, column_name: &str) -> anyhow::Result<Arc<Reader>> {
        let cell = {
            let read_guard = self.column_readers.read();
            if let Some(cell) = read_guard.get(column_name) {
                Arc::clone(cell)
            } else {
                drop(read_guard);
                let mut write_guard = self.column_readers.write();
                write_guard
                    .entry(column_name.to_string())
                    .or_insert_with(|| Arc::new(OnceCell::new()))
                    .clone()
            }
        };

        let storage = self.storage.clone();
        let base_path = self.base_path.clone();
        let col_name = column_name.to_string();
        let cache_cap = self.cache_capacity_bytes;

        let reader = cell
            .get_or_try_init(|| async {
                let col_path = array_name_to_path(&base_path, &col_name);
                let backend = ObjStoreStorage {
                    store: storage,
                    path: col_path,
                };
                let r = Reader::open(backend, cache_cap)
                    .await
                    .map_err(|e| anyhow!("Failed to open column '{}': {}", col_name, e))?;
                Ok::<_, anyhow::Error>(Arc::new(r))
            })
            .await?;

        Ok(Arc::clone(reader))
    }

    /// Stream all datasets with a column projection.
    ///
    /// Returns a `Send` receiver stream of [`ProjectedDataset`] items that can
    /// be polled from multiple threads. Internally spawns `concurrency` tasks
    /// that read datasets in parallel and push results into a bounded channel.
    ///
    /// For each dataset in the store, the requested `projection` columns are
    /// read. If a column does not exist for a given dataset, that slot is
    /// `None`. If **all** projected columns are `None`, the dataset is skipped
    /// entirely (it has no relevant data).
    ///
    /// # Arguments
    ///
    /// * `projection` — column names to read from each dataset.
    /// * `concurrency` — how many datasets to read in parallel.
    pub fn stream_projected(
        self: &Arc<Self>,
        projection: &[&str],
        concurrency: usize,
    ) -> flume::r#async::RecvStream<'static, anyhow::Result<ProjectedDataset>> {
        let projection: Arc<Vec<String>> =
            Arc::new(projection.iter().map(|&s| s.to_string()).collect());
        let dataset_names: Vec<String> = self
            .schema_store
            .dataset_names()
            .map(|s| s.to_owned())
            .collect();
        let reader = Arc::clone(self);

        let (tx, rx) = flume::bounded(concurrency);

        tokio::spawn(async move {
            let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
            let mut handles = Vec::with_capacity(dataset_names.len());

            for dataset_name in dataset_names {
                let reader = Arc::clone(&reader);
                let projection = Arc::clone(&projection);
                let tx = tx.clone();
                let permit = semaphore.clone().acquire_owned().await;

                let handle = tokio::spawn(async move {
                    let _permit = permit;
                    let result = reader
                        .read_projected_dataset(&dataset_name, &projection)
                        .await;
                    match result {
                        Ok(Some(ds)) => {
                            let _ = tx.send_async(Ok(ds)).await;
                        }
                        Ok(None) => {} // skip — all columns missing
                        Err(e) => {
                            let _ = tx.send_async(Err(e)).await;
                        }
                    }
                });
                handles.push(handle);
            }

            // Wait for all tasks to complete (channel drops when tx + all clones drop)
            for handle in handles {
                let _ = handle.await;
            }
        });

        rx.into_stream()
    }

    /// Read a single dataset with projected columns, returning `None` values
    /// for missing columns. Returns `Ok(None)` if all columns are missing.
    async fn read_projected_dataset(
        &self,
        dataset_name: &str,
        projection: &[String],
    ) -> anyhow::Result<Option<ProjectedDataset>> {
        let schema = match self.schema_store.get_schema(dataset_name) {
            Some(s) => s,
            None => return Ok(None),
        };

        let mut arrays: IndexMap<String, Option<Arc<dyn NdArrayD>>> =
            IndexMap::with_capacity(projection.len());
        let mut any_present = false;

        for col_name in projection {
            let col = schema.columns.iter().find(|c| c.name == col_name.as_str());

            let value = match col {
                Some(col_def) => {
                    // Column exists in schema — try to read from .arrf file
                    match self
                        .try_read_array(dataset_name, col_name, col_def.data_type.clone())
                        .await
                    {
                        Ok(Some(arr)) => {
                            any_present = true;
                            Some(arr)
                        }
                        Ok(None) => None,
                        Err(e) => return Err(e),
                    }
                }
                None => None, // column not in schema for this dataset
            };

            arrays.insert(col_name.clone(), value);
        }

        if !any_present {
            return Ok(None);
        }

        Ok(Some(ProjectedDataset {
            name: dataset_name.to_string(),
            arrays,
        }))
    }

    /// Try to read a single array. Returns `Ok(None)` if the array doesn't
    /// exist in the .arrf file (dataset was never written to this column file).
    async fn try_read_array(
        &self,
        dataset_name: &str,
        column_name: &str,
        data_type: NdArrayDataType,
    ) -> anyhow::Result<Option<Arc<dyn NdArrayD>>> {
        let reader = match self.get_or_open_column_reader(column_name).await {
            Ok(r) => r,
            Err(_) => return Ok(None), // .arrf file doesn't exist
        };

        match reader.get_array(dataset_name) {
            Ok(_) => {
                let nd = lazy_ndarray_dyn(reader, dataset_name, data_type)?;
                Ok(Some(nd))
            }
            Err(array_format::Error::ArrayNotFound { .. }) => Ok(None),
            Err(e) => Err(anyhow!(
                "Error reading '{}' for '{}': {}",
                column_name,
                dataset_name,
                e
            )),
        }
    }
}

// ─── Projected Dataset ────────────────────────────────────────────────────────

/// A dataset read with a column projection.
///
/// Each entry in `arrays` corresponds to a projected column name. The value
/// is `Some(array)` if the column existed for this dataset, or `None` if it
/// was missing. Datasets where all columns are `None` are never emitted by
/// the streaming reader.
#[derive(Debug)]
pub struct ProjectedDataset {
    pub name: String,
    pub arrays: IndexMap<String, Option<Arc<dyn NdArrayD>>>,
}

impl ProjectedDataset {
    /// Returns only the columns that are present (non-None).
    pub fn present_arrays(&self) -> impl Iterator<Item = (&str, &Arc<dyn NdArrayD>)> {
        self.arrays
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|a| (k.as_str(), a)))
    }

    /// Convert to a full `Dataset`, keeping only the present arrays.
    pub async fn into_dataset(self) -> Dataset {
        let arrays: IndexMap<String, Arc<dyn NdArrayD>> = self
            .arrays
            .into_iter()
            .filter_map(|(k, v)| v.map(|a| (k, a)))
            .collect();
        Dataset::new(self.name, arrays).await
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::AtlasWriter;
    use array_format::NoCompression;
    use beacon_nd_array::NdArray;
    use beacon_nd_array::datatypes::TimestampNanosecond;
    use futures::StreamExt;
    use object_store::memory::InMemory;

    async fn make_dataset(name: &str, arrays: Vec<(&str, Arc<dyn NdArrayD>)>) -> Dataset {
        let map: IndexMap<String, Arc<dyn NdArrayD>> = arrays
            .into_iter()
            .map(|(n, a)| (n.to_string(), a))
            .collect();
        Dataset::new(name.to_string(), map).await
    }

    async fn write_and_flush(store: Arc<InMemory>, datasets: Vec<Dataset>) {
        let mut writer =
            AtlasWriter::new(store.clone(), Path::from("atlas"), NoCompression, 64 * 1024);
        for ds in &datasets {
            writer.write_dataset(ds).await.unwrap();
        }
        writer.flush().await.unwrap();
    }

    async fn open_reader(store: Arc<InMemory>) -> AtlasReader {
        AtlasReader::open(store, Path::from("atlas"), 64 * 1024 * 1024)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_roundtrip_simple() {
        let store = Arc::new(InMemory::new());

        let temp = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0, 3.0],
            vec![3],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let ds = make_dataset("ds-1", vec![("temperature", Arc::new(temp))]).await;
        write_and_flush(store.clone(), vec![ds]).await;

        let reader = open_reader(store).await;
        let result = reader.read_dataset("ds-1").await.unwrap();

        assert_eq!(result.name, "ds-1");
        let arr = result.arrays.get("temperature").unwrap();
        assert_eq!(arr.datatype(), NdArrayDataType::F64);
        assert_eq!(arr.shape(), vec![3]);

        let typed = arr.as_any().downcast_ref::<NdArray<f64>>().unwrap();
        let values = typed.clone_into_raw_vec().await;
        assert_eq!(values, vec![1.0, 2.0, 3.0]);
    }

    #[tokio::test]
    async fn test_roundtrip_with_attribute_arrays() {
        let store = Arc::new(InMemory::new());

        let temp = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![15.0, 16.0],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let units = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["celsius".into()],
            vec![1],
            vec![],
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
        write_and_flush(store.clone(), vec![ds]).await;

        let reader = open_reader(store.clone()).await;
        let result = reader.read_dataset("ds-attrs").await.unwrap();

        // Both arrays are read from .arrf files stored hierarchically
        assert!(result.arrays.contains_key("temperature"));
        assert!(result.arrays.contains_key("temperature.units"));

        let arr = result.arrays.get("temperature").unwrap();
        let typed = arr.as_any().downcast_ref::<NdArray<f64>>().unwrap();
        assert_eq!(typed.clone_into_raw_vec().await, vec![15.0, 16.0]);

        let attr_arr = result.arrays.get("temperature.units").unwrap();
        assert_eq!(attr_arr.datatype(), NdArrayDataType::String);
        let typed_attr = attr_arr.as_any().downcast_ref::<NdArray<String>>().unwrap();
        assert_eq!(
            typed_attr.clone_into_raw_vec().await,
            vec!["celsius".to_string()]
        );

        // Verify the underlying storage uses hierarchical paths
        use object_store::ObjectStore;
        let base = Path::from("atlas/columns/temperature");
        let list: Vec<_> = store
            .list_with_delimiter(Some(&base))
            .await
            .unwrap()
            .objects;
        // "temperature/units.arrf" should exist under the temperature directory
        assert!(
            list.iter()
                .any(|o| o.location.to_string().contains("temperature/units.arrf")),
            "Expected temperature/units.arrf in store, found: {:?}",
            list.iter()
                .map(|o| o.location.to_string())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn test_roundtrip_with_global_attribute_arrays() {
        let store = Arc::new(InMemory::new());

        let temp =
            NdArray::<f64>::try_new_from_vec_in_mem(vec![10.0], vec![1], vec!["obs".into()], None)
                .unwrap();

        let convention = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["CF-1.6".into()],
            vec![1],
            vec![],
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
        write_and_flush(store.clone(), vec![ds]).await;

        let reader = open_reader(store).await;
        let result = reader.read_dataset("ds-global").await.unwrap();

        assert!(result.arrays.contains_key("temperature"));
        assert!(result.arrays.contains_key(".convention"));

        let ga = result.arrays.get(".convention").unwrap();
        assert_eq!(ga.datatype(), NdArrayDataType::String);
        let typed = ga.as_any().downcast_ref::<NdArray<String>>().unwrap();
        assert_eq!(typed.clone_into_raw_vec().await, vec!["CF-1.6".to_string()]);
    }

    #[tokio::test]
    async fn test_projected_read() {
        let store = Arc::new(InMemory::new());

        let temp = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let depth = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![10.0, 20.0],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let salinity = NdArray::<f32>::try_new_from_vec_in_mem(
            vec![35.0, 36.0],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let ds = make_dataset(
            "ds-proj",
            vec![
                ("temperature", Arc::new(temp)),
                ("depth", Arc::new(depth)),
                ("salinity", Arc::new(salinity)),
            ],
        )
        .await;
        write_and_flush(store.clone(), vec![ds]).await;

        let reader = open_reader(store).await;
        let result = reader
            .read_dataset_projected("ds-proj", &["temperature"])
            .await
            .unwrap();

        assert!(result.arrays.contains_key("temperature"));
        assert!(!result.arrays.contains_key("depth"));
        assert!(!result.arrays.contains_key("salinity"));
    }

    #[tokio::test]
    async fn test_projected_read_invalid_column() {
        let store = Arc::new(InMemory::new());

        let temp =
            NdArray::<f64>::try_new_from_vec_in_mem(vec![1.0], vec![1], vec!["obs".into()], None)
                .unwrap();

        let ds = make_dataset("ds-err", vec![("temperature", Arc::new(temp))]).await;
        write_and_flush(store.clone(), vec![ds]).await;

        let reader = open_reader(store).await;
        let err = reader
            .read_dataset_projected("ds-err", &["nonexistent"])
            .await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_read_dataset_arrays_selective() {
        let store = Arc::new(InMemory::new());

        let temp = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let units = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["celsius".into()],
            vec![1],
            vec![],
            None,
        )
        .unwrap();

        let convention = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["CF-1.6".into()],
            vec![1],
            vec![],
            None,
        )
        .unwrap();

        let ds = make_dataset(
            "ds-sel",
            vec![
                ("temperature", Arc::new(temp)),
                ("temperature.units", Arc::new(units)),
                (".convention", Arc::new(convention)),
            ],
        )
        .await;
        write_and_flush(store.clone(), vec![ds]).await;

        let reader = open_reader(store).await;

        // Read only specific arrays
        let arrays = reader
            .read_dataset_arrays("ds-sel", &["temperature.units", ".convention"])
            .await
            .unwrap();

        assert_eq!(arrays.len(), 2);
        assert!(arrays.contains_key("temperature.units"));
        assert!(arrays.contains_key(".convention"));
    }

    #[tokio::test]
    async fn test_read_nonexistent_dataset() {
        let store = Arc::new(InMemory::new());

        let temp =
            NdArray::<f64>::try_new_from_vec_in_mem(vec![1.0], vec![1], vec!["obs".into()], None)
                .unwrap();

        let ds = make_dataset("exists", vec![("val", Arc::new(temp))]).await;
        write_and_flush(store.clone(), vec![ds]).await;

        let reader = open_reader(store).await;
        assert!(reader.read_dataset("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn test_concurrent_reads() {
        let store = Arc::new(InMemory::new());

        let mut datasets = Vec::new();
        for i in 0..10 {
            let arr = NdArray::<f64>::try_new_from_vec_in_mem(
                vec![i as f64; 100],
                vec![100],
                vec!["obs".into()],
                None,
            )
            .unwrap();
            datasets
                .push(make_dataset(&format!("ds-{}", i), vec![("values", Arc::new(arr))]).await);
        }
        write_and_flush(store.clone(), datasets).await;

        let reader = Arc::new(open_reader(store).await);

        let mut handles = Vec::new();
        for i in 0..10 {
            let r = Arc::clone(&reader);
            handles.push(tokio::spawn(async move {
                let ds = r.read_dataset(&format!("ds-{}", i)).await.unwrap();
                let arr = ds.arrays.get("values").unwrap();
                let typed = arr.as_any().downcast_ref::<NdArray<f64>>().unwrap();
                let vals = typed.clone_into_raw_vec().await;
                assert_eq!(vals.len(), 100);
                assert!(vals.iter().all(|&v| v == i as f64));
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_roundtrip_multiple_types() {
        let store = Arc::new(InMemory::new());

        let f64_arr = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let i32_arr = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![10, 20],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let str_arr = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["hello".into(), "world".into()],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let ds = make_dataset(
            "ds-types",
            vec![
                ("float_col", Arc::new(f64_arr)),
                ("int_col", Arc::new(i32_arr)),
                ("str_col", Arc::new(str_arr)),
            ],
        )
        .await;
        write_and_flush(store.clone(), vec![ds]).await;

        let reader = open_reader(store).await;
        let result = reader.read_dataset("ds-types").await.unwrap();

        let f = result.arrays.get("float_col").unwrap();
        assert_eq!(f.datatype(), NdArrayDataType::F64);
        let typed = f.as_any().downcast_ref::<NdArray<f64>>().unwrap();
        assert_eq!(typed.clone_into_raw_vec().await, vec![1.0, 2.0]);

        let i = result.arrays.get("int_col").unwrap();
        assert_eq!(i.datatype(), NdArrayDataType::I32);
        let typed = i.as_any().downcast_ref::<NdArray<i32>>().unwrap();
        assert_eq!(typed.clone_into_raw_vec().await, vec![10, 20]);

        let s = result.arrays.get("str_col").unwrap();
        assert_eq!(s.datatype(), NdArrayDataType::String);
        let typed = s.as_any().downcast_ref::<NdArray<String>>().unwrap();
        assert_eq!(
            typed.clone_into_raw_vec().await,
            vec!["hello".to_string(), "world".to_string()]
        );
    }

    // ─── Streaming tests ──────────────────────────────────────────────────

    #[tokio::test]
    async fn test_stream_projected_basic() {
        let store = Arc::new(InMemory::new());

        // Write 3 datasets, all with "temperature" column
        let mut datasets = Vec::new();
        for i in 0..3 {
            let arr = NdArray::<f64>::try_new_from_vec_in_mem(
                vec![i as f64 * 10.0],
                vec![1],
                vec!["obs".into()],
                None,
            )
            .unwrap();
            datasets.push(
                make_dataset(&format!("ds-{}", i), vec![("temperature", Arc::new(arr))]).await,
            );
        }
        write_and_flush(store.clone(), datasets).await;

        let reader = Arc::new(open_reader(store).await);
        let mut stream = reader.stream_projected(&["temperature"], 4);

        let mut results = Vec::new();
        while let Some(item) = stream.next().await {
            results.push(item.unwrap());
        }

        assert_eq!(results.len(), 3);
        for r in &results {
            assert!(r.arrays.get("temperature").unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn test_stream_projected_missing_columns() {
        let store = Arc::new(InMemory::new());

        // ds-a has temperature + depth
        let temp_a =
            NdArray::<f64>::try_new_from_vec_in_mem(vec![1.0], vec![1], vec!["obs".into()], None)
                .unwrap();
        let depth_a =
            NdArray::<f64>::try_new_from_vec_in_mem(vec![10.0], vec![1], vec!["obs".into()], None)
                .unwrap();
        let ds_a = make_dataset(
            "ds-a",
            vec![
                ("temperature", Arc::new(temp_a)),
                ("depth", Arc::new(depth_a)),
            ],
        )
        .await;

        // ds-b has only temperature (no depth)
        let temp_b =
            NdArray::<f64>::try_new_from_vec_in_mem(vec![2.0], vec![1], vec!["obs".into()], None)
                .unwrap();
        let ds_b = make_dataset("ds-b", vec![("temperature", Arc::new(temp_b))]).await;

        write_and_flush(store.clone(), vec![ds_a, ds_b]).await;

        let reader = Arc::new(open_reader(store).await);
        let mut stream = reader.stream_projected(&["temperature", "depth"], 4);

        let mut results = Vec::new();
        while let Some(item) = stream.next().await {
            results.push(item.unwrap());
        }

        // Both datasets should be emitted (both have at least "temperature")
        assert_eq!(results.len(), 2);

        let a = results.iter().find(|r| r.name == "ds-a").unwrap();
        assert!(a.arrays.get("temperature").unwrap().is_some());
        assert!(a.arrays.get("depth").unwrap().is_some());

        let b = results.iter().find(|r| r.name == "ds-b").unwrap();
        assert!(b.arrays.get("temperature").unwrap().is_some());
        assert!(b.arrays.get("depth").unwrap().is_none()); // missing column
    }

    #[tokio::test]
    async fn test_stream_projected_skips_empty_datasets() {
        let store = Arc::new(InMemory::new());

        // ds-has has "temperature"
        let temp =
            NdArray::<f64>::try_new_from_vec_in_mem(vec![5.0], vec![1], vec!["obs".into()], None)
                .unwrap();
        let ds_has = make_dataset("ds-has", vec![("temperature", Arc::new(temp))]).await;

        // ds-missing has only "salinity" (not in the projection)
        let sal =
            NdArray::<f32>::try_new_from_vec_in_mem(vec![35.0], vec![1], vec!["obs".into()], None)
                .unwrap();
        let ds_missing = make_dataset("ds-missing", vec![("salinity", Arc::new(sal))]).await;

        write_and_flush(store.clone(), vec![ds_has, ds_missing]).await;

        let reader = Arc::new(open_reader(store).await);
        let mut stream = reader.stream_projected(&["temperature", "depth"], 4);

        let mut results = Vec::new();
        while let Some(item) = stream.next().await {
            results.push(item.unwrap());
        }

        // Only ds-has should appear — ds-missing has no projected columns
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "ds-has");
        assert!(results[0].arrays.get("temperature").unwrap().is_some());
        assert!(results[0].arrays.get("depth").unwrap().is_none());
    }

    #[tokio::test]
    async fn test_stream_projected_concurrent() {
        let store = Arc::new(InMemory::new());

        // Write 50 datasets
        let mut datasets = Vec::new();
        for i in 0..50 {
            let arr = NdArray::<f64>::try_new_from_vec_in_mem(
                vec![i as f64; 10],
                vec![10],
                vec!["obs".into()],
                None,
            )
            .unwrap();
            datasets
                .push(make_dataset(&format!("ds-{}", i), vec![("values", Arc::new(arr))]).await);
        }
        write_and_flush(store.clone(), datasets).await;

        let reader = Arc::new(open_reader(store).await);
        // Use high concurrency
        let mut stream = reader.stream_projected(&["values"], 16);

        let mut count = 0;
        while let Some(item) = stream.next().await {
            let pd = item.unwrap();
            assert!(pd.arrays.get("values").unwrap().is_some());
            count += 1;
        }
        assert_eq!(count, 50);
    }

    #[tokio::test]
    async fn test_projected_dataset_into_dataset() {
        let store = Arc::new(InMemory::new());

        let temp = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();
        let ds = make_dataset("ds-conv", vec![("temperature", Arc::new(temp))]).await;
        write_and_flush(store.clone(), vec![ds]).await;

        let reader = Arc::new(open_reader(store).await);
        let mut stream = reader.stream_projected(&["temperature", "nonexistent"], 4);

        let projected = stream.next().await.unwrap().unwrap();
        assert!(projected.arrays.get("temperature").unwrap().is_some());
        assert!(projected.arrays.get("nonexistent").unwrap().is_none());

        // Convert to Dataset — only present arrays included
        let dataset = projected.into_dataset().await;
        assert!(dataset.arrays.contains_key("temperature"));
        assert!(!dataset.arrays.contains_key("nonexistent"));
    }
}
