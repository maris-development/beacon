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
use crate::format::footer::{CollectionFooter, DeletionMask};
use crate::format::schema::{ResolvedSchema, SchemaStore};
use crate::format::statistics::{ColumnStatistics, StatisticsValue};
use crate::storage::{AtlasStorage, array_name_to_path, stats_path};

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
    deletion_mask: DeletionMask,
    /// Lazy cache of opened `array_format::Reader` instances, keyed by array name.
    column_readers: RwLock<HashMap<String, Arc<OnceCell<Arc<Reader>>>>>,
    /// Cache for loaded column statistics.
    column_stats_cache: RwLock<HashMap<String, Arc<ColumnStatistics>>>,
    storage: Arc<dyn ObjectStore>,
    base_path: Path,
    cache_capacity_bytes: u64,
}

impl AtlasReader {
    /// Open an existing atlas collection for reading.
    ///
    /// Loads the footer from `{base_path}/collection.atlas` and the deletion
    /// mask from `{base_path}/deleted.atlas`. Column `.arrf` files are opened
    /// lazily on first read.
    pub async fn open(
        storage: Arc<dyn ObjectStore>,
        base_path: Path,
        cache_capacity_bytes: u64,
    ) -> anyhow::Result<Self> {
        let footer_path = Path::from(format!("{}/collection.atlas", base_path));
        let footer =
            CollectionFooter::load_from_object_store(storage.as_ref(), &footer_path).await?;

        let mask_path = Path::from(format!("{}/deleted.atlas", base_path));
        let deletion_mask =
            DeletionMask::load_from_object_store(storage.as_ref(), &mask_path).await?;

        Ok(Self {
            schema_store: footer.schema_store,
            deletion_mask,
            column_readers: RwLock::new(HashMap::new()),
            column_stats_cache: RwLock::new(HashMap::new()),
            storage,
            base_path,
            cache_capacity_bytes,
        })
    }

    pub async fn read_dataset_schema(&self, dataset_name: &str) -> Option<ResolvedSchema> {
        self.schema_store.get_schema(dataset_name)
    }

    /// Read a dataset by name, returning all or projected arrays.
    ///
    /// If `projection` is provided, only arrays with names in the projection are returned (if they exist in the schema). Otherwise all arrays in the schema are returned.
    /// The schema is used to determine which arrays exist for the dataset and their types, but all arrays are read directly from their `.arrf` files on demand.
    /// Returns an error if any requested column is not in the schema, or if the dataset is deleted.
    pub async fn read_dataset<S: AsRef<str>, P: AsRef<[S]>>(
        &self,
        dataset_name: &str,
        projection: Option<P>,
    ) -> anyhow::Result<Dataset> {
        self.check_not_deleted(dataset_name)?;

        let schema = self
            .schema_store
            .get_schema(dataset_name)
            .ok_or_else(|| anyhow!("Dataset '{}' not found in schema store", dataset_name))?;

        let requested_columns: Vec<String> = if let Some(cols) = projection {
            cols.as_ref()
                .iter()
                .map(|s| s.as_ref().to_string())
                .collect()
        } else {
            schema.columns.iter().map(|c| c.name.clone()).collect()
        };

        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();

        for col_name in &requested_columns {
            let col = match schema.columns.iter().find(|c| &c.name == col_name) {
                Some(col) => col,
                None => {
                    anyhow::bail!("Column '{}' not in schema", col_name);
                }
            };

            let reader = self.get_or_open_column_reader(col_name).await?;
            let nd = lazy_ndarray_dyn(reader, dataset_name, col.data_type.clone())?;
            arrays.insert(col_name.to_string(), nd);
        }

        Ok(Dataset::new(dataset_name.to_string(), arrays).await)
    }

    /// Get a reference to the schema store.
    pub fn schema_store(&self) -> &SchemaStore {
        &self.schema_store
    }

    /// Get the global schema (merged union of all non-deleted dataset schemas).
    pub fn global_schema(&self) -> &ResolvedSchema {
        self.schema_store.global_schema()
    }

    /// Get a reference to the deletion mask.
    pub fn deletion_mask(&self) -> &DeletionMask {
        &self.deletion_mask
    }

    /// Returns dataset names that are not deleted.
    pub fn dataset_names(&self) -> impl Iterator<Item = &str> {
        self.schema_store.datasets().filter_map(|(name, entry)| {
            if self.deletion_mask.is_deleted(entry.dataset_id) {
                None
            } else {
                Some(name)
            }
        })
    }

    /// Load column statistics for a given column on demand.
    ///
    /// Returns cached statistics if already loaded, otherwise reads from
    /// `{base_path}/columns/{name}.stats.atlas`.
    pub async fn load_column_statistics(
        &self,
        column_name: &str,
    ) -> anyhow::Result<Arc<ColumnStatistics>> {
        // Check cache first
        {
            let cache = self.column_stats_cache.read();
            if let Some(stats) = cache.get(column_name) {
                return Ok(Arc::clone(stats));
            }
        }

        // Load from object store
        let path = stats_path(&self.base_path, column_name);
        let stats = ColumnStatistics::load_from_object_store(self.storage.as_ref(), &path).await?;
        let stats = Arc::new(stats);

        // Cache it
        {
            let mut cache = self.column_stats_cache.write();
            cache.insert(column_name.to_string(), Arc::clone(&stats));
        }

        Ok(stats)
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    /// Returns an error if the dataset is deleted.
    fn check_not_deleted(&self, dataset_name: &str) -> anyhow::Result<()> {
        if let Some(entry) = self.schema_store.get_dataset_entry(dataset_name)
            && self.deletion_mask.is_deleted(entry.dataset_id)
        {
            anyhow::bail!("Dataset '{}' has been deleted", dataset_name);
        }
        Ok(())
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
                let backend = AtlasStorage {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::AtlasWriter;
    use array_format::NoCompression;
    use beacon_nd_array::NdArray;
    use beacon_nd_array::datatypes::TimestampNanosecond;
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
        let schema = reader.read_dataset_schema("ds-1").await.unwrap();
        assert!(!schema.columns.is_empty());
        let result = reader.read_dataset("ds-1", None::<&[&str]>).await.unwrap();

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
        let schema = reader.read_dataset_schema("ds-attrs").await.unwrap();
        assert_eq!(schema.columns.len(), 2);
        let result = reader
            .read_dataset("ds-attrs", None::<&[&str]>)
            .await
            .unwrap();

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
        let schema = reader.read_dataset_schema("ds-global").await.unwrap();
        assert_eq!(schema.columns.len(), 2);
        let result = reader
            .read_dataset("ds-global", None::<&[&str]>)
            .await
            .unwrap();

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
        let schema = reader.read_dataset_schema("ds-proj").await.unwrap();
        assert!(schema.columns.iter().any(|c| c.name == "temperature"));
        let result = reader
            .read_dataset("ds-proj", Some(&["temperature"]))
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
        let schema = reader.read_dataset_schema("ds-err").await.unwrap();
        assert!(!schema.columns.iter().any(|c| c.name == "nonexistent"));
        let err = reader.read_dataset("ds-err", Some(&["nonexistent"])).await;
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
        let schema = reader.read_dataset_schema("ds-sel").await.unwrap();
        assert_eq!(schema.columns.len(), 3);

        // Read only specific arrays
        let result = reader
            .read_dataset("ds-sel", Some(&["temperature.units", ".convention"]))
            .await
            .unwrap();
        let arrays = &result.arrays;

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
        assert!(reader.read_dataset_schema("nonexistent").await.is_none());
        assert!(
            reader
                .read_dataset("nonexistent", None::<&[&str]>)
                .await
                .is_err()
        );
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
                let name = format!("ds-{}", i);
                let schema = r.read_dataset_schema(&name).await.unwrap();
                assert!(!schema.columns.is_empty());
                let ds = r.read_dataset(&name, None::<&[&str]>).await.unwrap();
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
        let schema = reader.read_dataset_schema("ds-types").await.unwrap();
        assert_eq!(schema.columns.len(), 3);
        let result = reader
            .read_dataset("ds-types", None::<&[&str]>)
            .await
            .unwrap();

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

    // ─── Schema-driven projected read tests ─────────────────────────────

    #[tokio::test]
    async fn test_read_dataset_schema_then_projected_read() {
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

        let reader = open_reader(store).await;

        for i in 0..3 {
            let name = format!("ds-{}", i);
            let schema = reader.read_dataset_schema(&name).await.unwrap();
            let cols: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
            assert!(cols.contains(&"temperature"));

            let result = reader
                .read_dataset(&name, Some(cols.as_slice()))
                .await
                .unwrap();
            assert!(result.arrays.contains_key("temperature"));
        }
    }

    #[tokio::test]
    async fn test_read_dataset_schema_filters_missing_columns() {
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

        let reader = open_reader(store).await;
        let desired = &["temperature", "depth"];

        // ds-a has both columns — schema confirms both exist
        let schema_a = reader.read_dataset_schema("ds-a").await.unwrap();
        let cols_a: Vec<&str> = desired
            .iter()
            .filter(|c| schema_a.columns.iter().any(|sc| sc.name == **c))
            .copied()
            .collect();
        assert_eq!(cols_a, vec!["temperature", "depth"]);
        let a = reader
            .read_dataset("ds-a", Some(cols_a.as_slice()))
            .await
            .unwrap();
        assert!(a.arrays.contains_key("temperature"));
        assert!(a.arrays.contains_key("depth"));

        // ds-b is missing "depth" — schema-based filter removes it
        let schema_b = reader.read_dataset_schema("ds-b").await.unwrap();
        let cols_b: Vec<&str> = desired
            .iter()
            .filter(|c| schema_b.columns.iter().any(|sc| sc.name == **c))
            .copied()
            .collect();
        assert_eq!(cols_b, vec!["temperature"]);
        let b = reader
            .read_dataset("ds-b", Some(cols_b.as_slice()))
            .await
            .unwrap();
        assert!(b.arrays.contains_key("temperature"));
        assert!(!b.arrays.contains_key("depth"));
    }

    #[tokio::test]
    async fn test_read_dataset_schema_no_matching_columns() {
        let store = Arc::new(InMemory::new());

        // ds-missing has only "salinity" (not in the desired projection)
        let sal =
            NdArray::<f32>::try_new_from_vec_in_mem(vec![35.0], vec![1], vec!["obs".into()], None)
                .unwrap();
        let ds_missing = make_dataset("ds-missing", vec![("salinity", Arc::new(sal))]).await;

        write_and_flush(store.clone(), vec![ds_missing]).await;

        let reader = open_reader(store).await;
        let desired = &["temperature", "depth"];

        // Schema shows no overlap with desired columns
        let schema = reader.read_dataset_schema("ds-missing").await.unwrap();
        let cols: Vec<&str> = desired
            .iter()
            .filter(|c| schema.columns.iter().any(|sc| sc.name == **c))
            .copied()
            .collect();
        assert!(cols.is_empty());

        // Reading with empty projection gives empty dataset
        let result = reader
            .read_dataset("ds-missing", Some(cols.as_slice()))
            .await
            .unwrap();
        assert!(result.arrays.is_empty());
    }

    #[tokio::test]
    async fn test_read_dataset_schema_many_datasets() {
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

        let reader = open_reader(store).await;

        for i in 0..50 {
            let name = format!("ds-{}", i);
            let schema = reader.read_dataset_schema(&name).await.unwrap();
            let cols: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
            let result = reader
                .read_dataset(&name, Some(cols.as_slice()))
                .await
                .unwrap();
            assert!(result.arrays.contains_key("values"));
        }
    }

    #[tokio::test]
    async fn test_read_dataset_schema_only_present_columns_included() {
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

        let reader = open_reader(store).await;
        let desired = &["temperature", "nonexistent"];

        // Use schema to filter — only "temperature" is in the schema
        let schema = reader.read_dataset_schema("ds-conv").await.unwrap();
        let cols: Vec<&str> = desired
            .iter()
            .filter(|c| schema.columns.iter().any(|sc| sc.name == **c))
            .copied()
            .collect();

        let result = reader
            .read_dataset("ds-conv", Some(cols.as_slice()))
            .await
            .unwrap();

        // Only present columns are included
        assert!(result.arrays.contains_key("temperature"));
        assert!(!result.arrays.contains_key("nonexistent"));
    }

    // ─── Lazy AtlasArray integration tests ────────────────────────────────

    #[tokio::test]
    async fn test_lazy_read_subset_through_reader() {
        let store = Arc::new(InMemory::new());

        // Write a 2D array: 3×4 matrix
        let values: Vec<f64> = (0..12).map(|i| i as f64).collect();
        let arr = NdArray::<f64>::try_new_from_vec_in_mem(
            values,
            vec![3, 4],
            vec!["row".into(), "col".into()],
            None,
        )
        .unwrap();

        let ds = make_dataset("ds-2d", vec![("matrix", Arc::new(arr))]).await;
        write_and_flush(store.clone(), vec![ds]).await;

        let reader = open_reader(store).await;
        let schema = reader.read_dataset_schema("ds-2d").await.unwrap();
        assert_eq!(schema.columns.len(), 1);
        let result = reader.read_dataset("ds-2d", None::<&[&str]>).await.unwrap();

        let matrix = result.arrays.get("matrix").unwrap();
        assert_eq!(matrix.shape(), vec![3, 4]);
        assert_eq!(matrix.dimensions(), vec!["row", "col"]);

        // Read a subset via the NdArrayD trait — exercises AtlasArray::read_subset
        use beacon_nd_array::array::subset::ArraySubset;
        let subset = matrix
            .subset(ArraySubset {
                start: vec![1, 1],
                shape: vec![2, 2],
            })
            .await
            .unwrap();
        assert_eq!(subset.shape(), vec![2, 2]);

        let typed = subset.as_any().downcast_ref::<NdArray<f64>>().unwrap();
        assert_eq!(typed.clone_into_raw_vec().await, vec![5.0, 6.0, 9.0, 10.0]);
    }

    #[tokio::test]
    async fn test_lazy_read_multiple_datasets_same_column() {
        let store = Arc::new(InMemory::new());

        // Write two datasets sharing the same column name but different data
        let arr1 = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0, 3.0],
            vec![3],
            vec!["obs".into()],
            None,
        )
        .unwrap();
        let arr2 = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![10.0, 20.0],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let ds1 = make_dataset("ds-a", vec![("temperature", Arc::new(arr1))]).await;
        let ds2 = make_dataset("ds-b", vec![("temperature", Arc::new(arr2))]).await;
        write_and_flush(store.clone(), vec![ds1, ds2]).await;

        let reader = open_reader(store).await;

        // Both datasets share the same .arrf file but read different arrays
        let schema_a = reader.read_dataset_schema("ds-a").await.unwrap();
        assert_eq!(schema_a.columns.len(), 1);
        let r1 = reader.read_dataset("ds-a", None::<&[&str]>).await.unwrap();
        let schema_b = reader.read_dataset_schema("ds-b").await.unwrap();
        assert_eq!(schema_b.columns.len(), 1);
        let r2 = reader.read_dataset("ds-b", None::<&[&str]>).await.unwrap();

        let t1 = r1.arrays.get("temperature").unwrap();
        let t2 = r2.arrays.get("temperature").unwrap();

        assert_eq!(t1.shape(), vec![3]);
        assert_eq!(t2.shape(), vec![2]);

        let v1 = t1.as_any().downcast_ref::<NdArray<f64>>().unwrap();
        let v2 = t2.as_any().downcast_ref::<NdArray<f64>>().unwrap();
        assert_eq!(v1.clone_into_raw_vec().await, vec![1.0, 2.0, 3.0]);
        assert_eq!(v2.clone_into_raw_vec().await, vec![10.0, 20.0]);
    }

    #[tokio::test]
    async fn test_lazy_read_all_types_roundtrip() {
        let store = Arc::new(InMemory::new());

        let bool_arr = NdArray::<bool>::try_new_from_vec_in_mem(
            vec![true, false],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let u8_arr =
            NdArray::<u8>::try_new_from_vec_in_mem(vec![255, 0], vec![2], vec!["obs".into()], None)
                .unwrap();

        let u64_arr = NdArray::<u64>::try_new_from_vec_in_mem(
            vec![u64::MAX, 0],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let ts_arr = NdArray::<TimestampNanosecond>::try_new_from_vec_in_mem(
            vec![
                TimestampNanosecond(1_000_000_000),
                TimestampNanosecond(2_000_000_000),
            ],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let bin_arr = NdArray::<Vec<u8>>::try_new_from_vec_in_mem(
            vec![vec![0xDE, 0xAD], vec![0xBE, 0xEF]],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let ds = make_dataset(
            "ds-alltypes",
            vec![
                ("bools", Arc::new(bool_arr)),
                ("bytes", Arc::new(u8_arr)),
                ("big_ints", Arc::new(u64_arr)),
                ("timestamps", Arc::new(ts_arr)),
                ("binary", Arc::new(bin_arr)),
            ],
        )
        .await;
        write_and_flush(store.clone(), vec![ds]).await;

        let reader = open_reader(store).await;
        let schema = reader.read_dataset_schema("ds-alltypes").await.unwrap();
        assert_eq!(schema.columns.len(), 5);
        let result = reader
            .read_dataset("ds-alltypes", None::<&[&str]>)
            .await
            .unwrap();

        // Verify bool
        let b = result.arrays.get("bools").unwrap();
        let typed = b.as_any().downcast_ref::<NdArray<bool>>().unwrap();
        assert_eq!(typed.clone_into_raw_vec().await, vec![true, false]);

        // Verify u8
        let u = result.arrays.get("bytes").unwrap();
        let typed = u.as_any().downcast_ref::<NdArray<u8>>().unwrap();
        assert_eq!(typed.clone_into_raw_vec().await, vec![255, 0]);

        // Verify u64
        let big = result.arrays.get("big_ints").unwrap();
        let typed = big.as_any().downcast_ref::<NdArray<u64>>().unwrap();
        assert_eq!(typed.clone_into_raw_vec().await, vec![u64::MAX, 0]);

        // Verify timestamps
        let ts = result.arrays.get("timestamps").unwrap();
        assert_eq!(ts.datatype(), NdArrayDataType::Timestamp);
        let typed = ts
            .as_any()
            .downcast_ref::<NdArray<TimestampNanosecond>>()
            .unwrap();
        assert_eq!(
            typed.clone_into_raw_vec().await,
            vec![
                TimestampNanosecond(1_000_000_000),
                TimestampNanosecond(2_000_000_000)
            ]
        );

        // Verify binary
        let bin = result.arrays.get("binary").unwrap();
        assert_eq!(bin.datatype(), NdArrayDataType::Binary);
        let typed = bin.as_any().downcast_ref::<NdArray<Vec<u8>>>().unwrap();
        assert_eq!(
            typed.clone_into_raw_vec().await,
            vec![vec![0xDE, 0xAD], vec![0xBE, 0xEF]]
        );
    }

    #[tokio::test]
    async fn test_lazy_read_with_subset_reads() {
        let store = Arc::new(InMemory::new());

        // Write datasets with 1D arrays of increasing length
        let mut datasets = Vec::new();
        for i in 1..=5 {
            let values: Vec<f64> = (0..i * 10).map(|v| v as f64).collect();
            let arr = NdArray::<f64>::try_new_from_vec_in_mem(
                values,
                vec![i * 10],
                vec!["obs".into()],
                None,
            )
            .unwrap();
            datasets
                .push(make_dataset(&format!("ds-{}", i), vec![("values", Arc::new(arr))]).await);
        }
        write_and_flush(store.clone(), datasets).await;

        let reader = open_reader(store).await;

        use beacon_nd_array::array::subset::ArraySubset;
        for i in 1..=5 {
            let name = format!("ds-{}", i);
            let schema = reader.read_dataset_schema(&name).await.unwrap();
            assert!(schema.columns.iter().any(|c| c.name == "values"));
            let result = reader.read_dataset(&name, Some(&["values"])).await.unwrap();
            let arr = result.arrays.get("values").unwrap();
            let len = arr.shape()[0];

            // Read a subset: first 5 elements (or all if fewer)
            let subset_len = len.min(5);
            let sub = arr
                .subset(ArraySubset {
                    start: vec![0],
                    shape: vec![subset_len],
                })
                .await
                .unwrap();
            assert_eq!(sub.shape(), vec![subset_len]);

            // Verify values are 0..subset_len
            let typed = sub.as_any().downcast_ref::<NdArray<f64>>().unwrap();
            let vals = typed.clone_into_raw_vec().await;
            let expected: Vec<f64> = (0..subset_len).map(|v| v as f64).collect();
            assert_eq!(vals, expected);
        }
    }

    #[tokio::test]
    async fn test_deleted_dataset_invisible_to_reader() {
        let store = Arc::new(InMemory::new());

        // Write two datasets
        let arr1 = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();
        let arr2 = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![3.0, 4.0],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let ds1 = make_dataset("keep-me", vec![("val", Arc::new(arr1))]).await;
        let ds2 = make_dataset("delete-me", vec![("val", Arc::new(arr2))]).await;

        let mut writer =
            AtlasWriter::new(store.clone(), Path::from("atlas"), NoCompression, 64 * 1024);
        writer.write_dataset(&ds1).await.unwrap();
        writer.write_dataset(&ds2).await.unwrap();

        // Delete one dataset
        writer.remove_dataset("delete-me").unwrap();
        writer.flush().await.unwrap();

        // Open reader — deleted dataset should be invisible
        let reader = open_reader(store).await;
        let names: Vec<&str> = reader.dataset_names().collect();
        assert_eq!(names, vec!["keep-me"]);

        // Reading deleted dataset — schema is not available, read errors
        assert!(reader.read_dataset_schema("delete-me").await.is_none());
        let err = reader.read_dataset("delete-me", None::<&[&str]>).await;
        assert!(err.is_err());

        // Reading the kept dataset should work fine
        let schema = reader.read_dataset_schema("keep-me").await.unwrap();
        assert_eq!(schema.columns.len(), 1);
        let ds = reader
            .read_dataset("keep-me", None::<&[&str]>)
            .await
            .unwrap();
        assert_eq!(ds.name, "keep-me");
    }

    #[tokio::test]
    async fn test_load_column_statistics() {
        let store = Arc::new(InMemory::new());

        // Write datasets with different ranges
        let arr1 = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0, 3.0],
            vec![3],
            vec!["obs".into()],
            None,
        )
        .unwrap();
        let arr2 = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![10.0, 20.0, 30.0],
            vec![3],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let ds1 = make_dataset("low", vec![("temp", Arc::new(arr1))]).await;
        let ds2 = make_dataset("high", vec![("temp", Arc::new(arr2))]).await;

        let mut writer =
            AtlasWriter::new(store.clone(), Path::from("atlas"), NoCompression, 64 * 1024);
        writer.write_dataset(&ds1).await.unwrap();
        writer.write_dataset(&ds2).await.unwrap();
        writer.flush().await.unwrap();

        let reader = open_reader(store).await;
        let stats = reader.load_column_statistics("temp").await.unwrap();

        let s0 = stats.get(crate::schema::DatasetId::from_raw(0)).unwrap();
        assert_eq!(s0.row_count, 3);
        assert_eq!(
            s0.min_value,
            Some(crate::statistics::StatisticsValue::F64(1.0))
        );
        assert_eq!(
            s0.max_value,
            Some(crate::statistics::StatisticsValue::F64(3.0))
        );

        let s1 = stats.get(crate::schema::DatasetId::from_raw(1)).unwrap();
        assert_eq!(s1.row_count, 3);
        assert_eq!(
            s1.min_value,
            Some(crate::statistics::StatisticsValue::F64(10.0))
        );
        assert_eq!(
            s1.max_value,
            Some(crate::statistics::StatisticsValue::F64(30.0))
        );
    }
}
