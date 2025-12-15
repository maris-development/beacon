#![allow(unsafe_op_in_unsafe_fn)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use beacon_binary_format::collection::CollectionWriter;
use beacon_binary_format::collection_partition::{CollectionPartitionWriter, WriterOptions};
use futures::stream;
use object_store::ObjectStore;
use object_store::path::Path;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::types::PyDict;

use crate::numpy_arrays::build_nd_array;
use crate::utils::{StorageOptions, init_store, prepare_store_inputs, to_py_err};

struct CollectionInner {
    runtime: Arc<tokio::runtime::Runtime>,
    store: Arc<dyn ObjectStore>,
    collection_path: Path,
    writer: Arc<Mutex<CollectionWriter>>,
    partition_counter: AtomicUsize,
}

impl CollectionInner {
    fn new(base_dir: String, collection_path: String, storage: StorageOptions) -> PyResult<Self> {
        let runtime =
            Arc::new(tokio::runtime::Runtime::new().map_err(|err| {
                PyRuntimeError::new_err(format!("failed to start runtime: {err}"))
            })?);
        let store_handle = init_store(base_dir, storage)?;
        let path = store_handle.resolve_collection_path(&collection_path)?;
        let store = store_handle.store.clone();
        let writer = runtime
            .block_on(CollectionWriter::new(store.clone(), path.clone()))
            .map_err(to_py_err)?;
        Ok(Self {
            runtime,
            store,
            collection_path: path,
            writer: Arc::new(Mutex::new(writer)),
            partition_counter: AtomicUsize::new(0),
        })
    }

    fn next_partition_name(&self, provided: Option<String>) -> String {
        match provided {
            Some(name) => {
                self.partition_counter.fetch_add(1, Ordering::SeqCst);
                name
            }
            None => {
                let id = self.partition_counter.fetch_add(1, Ordering::SeqCst);
                format!("partition-{id}")
            }
        }
    }
}

/// Python-facing façade for the Beacon Binary Format collection writer.
///
/// A [`Collection`] can host any number of logical partitions that are flushed
/// individually, which mirrors the ergonomics of the Python bindings. Each
/// partition exposes the same `write_entry` interface, while the collection
/// ensures metadata is persisted exactly once per partition.
#[pyclass]
#[derive(Clone)]
pub struct Collection {
    inner: Arc<CollectionInner>,
}

#[pymethods]
impl Collection {
    #[new]
    #[pyo3(signature = (base_dir, collection_path, storage_options=None, filesystem=None))]
    pub fn new(
        base_dir: String,
        collection_path: String,
        storage_options: Option<Bound<'_, PyDict>>,
        filesystem: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let (normalized_base, storage) =
            prepare_store_inputs(base_dir, storage_options, filesystem)?;
        Self::with_storage(normalized_base, collection_path, storage)
    }

    /// Create a new logical partition writer.
    ///
    /// `partition_name` defaults to `partition-{n}` if omitted, while
    /// `max_group_size` controls the Arrow IPC chunking threshold inside the
    /// partition.
    #[pyo3(signature = (partition_name=None, max_group_size=None))]
    pub fn create_partition(
        &self,
        partition_name: Option<String>,
        max_group_size: Option<usize>,
    ) -> PyResult<PartitionBuilder> {
        let name = self.inner.next_partition_name(partition_name);
        let writer_options = WriterOptions {
            max_group_size: max_group_size.unwrap_or(8 * 1024 * 1024),
        };
        let partition_writer = CollectionPartitionWriter::new(
            self.inner.collection_path.clone(),
            self.inner.store.clone(),
            name,
            writer_options,
        );
        Ok(PartitionBuilder {
            collection: self.clone(),
            partition_writer: Some(partition_writer),
        })
    }

    /// Return the Beacon Binary Format crate version embedded in the writer metadata.
    pub fn library_version(&self) -> PyResult<String> {
        let writer = lock_writer(&self.inner.writer)?;
        Ok(writer.metadata().library_version.clone())
    }
}

impl Collection {
    fn with_storage(
        base_dir: String,
        collection_path: String,
        storage: StorageOptions,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(CollectionInner::new(base_dir, collection_path, storage)?),
        })
    }

    fn runtime(&self) -> &Arc<tokio::runtime::Runtime> {
        &self.inner.runtime
    }

    fn append_partition(
        &self,
        metadata: beacon_binary_format::collection_partition::CollectionPartitionMetadata,
    ) -> PyResult<()> {
        let mut writer = lock_writer(&self.inner.writer)?;
        writer.append_partition(metadata).map_err(to_py_err)?;
        self.runtime().block_on(writer.persist()).map_err(to_py_err)
    }
}

/// Builder for a single logical partition belonging to a [`Collection`].
#[pyclass]
pub struct PartitionBuilder {
    collection: Collection,
    partition_writer: Option<CollectionPartitionWriter>,
}

#[pymethods]
impl PartitionBuilder {
    /// Append a logical entry to the current partition.
    ///
    /// `arrays` accepts the dict described in the Python README/stubs. Each
    /// value can be a bare NumPy array, a `(array, [dim, ...])` tuple, or a
    /// `{"data": array, "dims": [...]}` mapping. Named dimensions and
    /// NumPy masked arrays are both honored before the Arrow payload is
    /// materialized.
    /// Mirror of [`PartitionBuilder::write_entry`] for scripts that rely on the
    /// legacy single-partition builder.
    pub fn write_entry(
        &mut self,
        py: Python<'_>,
        entry_key: &str,
        arrays: Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let writer = self
            .partition_writer
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("partition already finalized"))?;

        let mut entries = Vec::with_capacity(arrays.len());
        for (key, value) in arrays.iter() {
            let name: String = key.extract()?;
            let (field, nd_array) = build_nd_array(py, &name, value.unbind())?;
            entries.push((field, nd_array));
        }

        let stream = stream::iter(entries);
        self.collection
            .runtime()
            .block_on(writer.write_entry(entry_key, stream))
            .map_err(to_py_err)
    }

    /// Finish the partition, append it to the collection, and persist metadata.
    ///
    /// Once a partition is finished the same builder cannot be reused, which
    /// mirrors the behavior of the lower-level `CollectionPartitionWriter`.
    pub fn finish(&mut self) -> PyResult<()> {
        let writer = self
            .partition_writer
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("partition already finalized"))?;
        let metadata = self
            .collection
            .runtime()
            .block_on(writer.finish())
            .map_err(to_py_err)?;
        self.collection.append_partition(metadata)
    }
}

/// Backwards-compatible façade that creates a single partition on initialization.
///
/// `CollectionBuilder` still exists so that legacy scripts which only ever
/// target a single partition do not need to be rewritten immediately. All new
/// code should favor [`Collection`] + [`PartitionBuilder`].
#[pyclass]
pub struct CollectionBuilder {
    collection: Collection,
    partition: Option<PartitionBuilder>,
}

#[pymethods]
impl CollectionBuilder {
    #[new]
    #[pyo3(signature = (base_dir, collection_path, partition_name=None, max_group_size=None, storage_options=None, filesystem=None))]
    pub fn new(
        base_dir: String,
        collection_path: String,
        partition_name: Option<String>,
        max_group_size: Option<usize>,
        storage_options: Option<Bound<'_, PyDict>>,
        filesystem: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let (normalized_base, storage) =
            prepare_store_inputs(base_dir, storage_options, filesystem)?;
        let collection = Collection::with_storage(normalized_base, collection_path, storage)?;
        let partition = collection.create_partition(partition_name, max_group_size)?;
        Ok(Self {
            collection,
            partition: Some(partition),
        })
    }

    pub fn write_entry(
        &mut self,
        py: Python<'_>,
        entry_key: &str,
        arrays: Bound<'_, PyDict>,
    ) -> PyResult<()> {
        self.partition
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("builder already finalized"))?
            .write_entry(py, entry_key, arrays)
    }

    /// Finalize the underlying partition and release the builder handle.
    pub fn finish(&mut self) -> PyResult<()> {
        let result = self
            .partition
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("builder already finalized"))?
            .finish();
        self.partition = None;
        result
    }

    /// Convenience proxy for [`Collection::library_version`].
    pub fn library_version(&self) -> PyResult<String> {
        self.collection.library_version()
    }
}

fn lock_writer<'a>(
    writer: &'a Arc<Mutex<CollectionWriter>>,
) -> PyResult<MutexGuard<'a, CollectionWriter>> {
    writer
        .lock()
        .map_err(|_| PyRuntimeError::new_err("collection writer poisoned"))
}
