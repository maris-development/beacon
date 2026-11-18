//! High-level [`oxcdf`] reader that produces [`AnyDataset`] values.
//!
//! The entry point is [`open_dataset`]. It opens an object through
//! [`object_store`], turns every variable and attribute into a lazy
//! [`NdArrayD`] wrapper, and returns the result as an [`AnyDataset`].
//!
//! # CF conventions
//!
//! - A variable whose last dimension name starts with `string`, `strlen` or
//!   `strnlen` decodes as UTF-8 strings instead of raw characters.
//! - A variable with a `units` attribute in the CF time form (for example
//!   `"days since 1950-01-01"`) decodes as nanosecond timestamps.
//! - `scale_factor` and `add_offset` decode a packed variable to `f64`.
//!
//! # Naming conventions
//!
//! - Each variable becomes a named array in the dataset.
//! - A variable attribute becomes an array named `"variable_name.attribute_name"`.
//! - A global attribute becomes an array named `".attribute_name"`.
//!
//! # Groups
//!
//! Only the root group is read, which is what netcdf-c's `File::variables`
//! returns. Both readers therefore report the same variables.
//!
//! # Dimensions
//!
//! A file that names no dimension, which is every plain HDF5 file, gets the
//! dimensions netCDF invents. [`crate::dimensions`] gives every invented axis of
//! one length one name, so two groups of one file broadcast against each other.
//! A NetCDF-4 file names every dimension and is untouched.
//!
//! # Example
//!
//! ```no_run
//! # use std::sync::Arc;
//! # async fn example(store: Arc<dyn object_store::ObjectStore>) -> anyhow::Result<()> {
//! use object_store::path::Path;
//!
//! let any =
//!     beacon_arrow_netcdf::oxcdf_reader::open_dataset(store, Path::from("data.nc")).await?;
//! for name in any.dataset().arrays.keys() {
//!     println!("{name}");
//! }
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use beacon_nd_array::{
    dataset::{AnyDataset, Dataset},
    NdArrayD,
};
use indexmap::IndexMap;
use object_store::{path::Path, ObjectMeta, ObjectStore};
use oxcdf::{async_source::AsyncObjectStoreSource, AsyncNetcdfFile, OpenOptions};

use crate::dimensions::PhonyDimensions;
use crate::oxcdf_reader::compat;

/// Open a NetCDF object from `store` and return its contents as an
/// [`AnyDataset`].
///
/// The reader fetches byte ranges. It never copies the whole object, so an
/// object in S3, GCS or Azure needs no local file.
///
/// Variable data is **not** read eagerly. The backends fetch data on demand
/// when individual arrays are accessed.
///
/// # Errors
///
/// Returns an error when the object cannot be opened, or when a variable uses a
/// type this reader does not support.
pub async fn open_dataset(store: Arc<dyn ObjectStore>, path: Path) -> anyhow::Result<AnyDataset> {
    let name = path.to_string();
    let file = Arc::new(AsyncNetcdfFile::open_store(store, path).await?);
    dataset_from_file(name, file).await
}

/// Open the NetCDF object that `object` describes and return its contents as an
/// [`AnyDataset`].
///
/// This is [`open_dataset`] for an object that a listing already found. The
/// size comes from `object`, so the open sends no `head` request.
///
/// # Errors
///
/// Returns an error when the object cannot be opened, or when a variable uses a
/// type this reader does not support.
pub async fn open_object(
    store: Arc<dyn ObjectStore>,
    object: &ObjectMeta,
) -> anyhow::Result<AnyDataset> {
    let name = object.location.to_string();
    let file = Arc::new(open_file(store, object).await?);
    dataset_from_file(name, file).await
}

/// Open the file that `object` describes, with the size from `object`.
///
/// # Errors
///
/// Returns an error when the file metadata cannot be read.
pub async fn open_file(
    store: Arc<dyn ObjectStore>,
    object: &ObjectMeta,
) -> oxcdf::Result<AsyncNetcdfFile> {
    let source = AsyncObjectStoreSource::with_size(store, object.location.clone(), object.size);
    AsyncNetcdfFile::open_with(Arc::new(source), OpenOptions::remote()).await
}

async fn dataset_from_file(name: String, file: Arc<AsyncNetcdfFile>) -> anyhow::Result<AnyDataset> {
    let phony = PhonyDimensions::of_file(&file);
    let arrays = read_arrays(&file)?;
    // A file that names no dimension is an instrument file, and `SELECT *`
    // picks its grid by volume rather than by variable count.
    let dataset = Dataset::new(name, arrays)
        .await
        .with_invented_dimensions(phony.invented_names().iter().cloned());
    AnyDataset::try_from_dataset(dataset).await
}

/// Read every variable and attribute of an opened file into an ordered map of
/// lazy ND arrays.
///
/// The map is sorted by key, so iteration order is stable. This is the
/// lower-level building block behind [`open_dataset`].
///
/// # Errors
///
/// Returns an error when an attribute of the file cannot be read. A variable
/// this reader cannot model is skipped, as it is on the netcdf-c path.
pub fn read_arrays(
    file: &Arc<AsyncNetcdfFile>,
) -> anyhow::Result<IndexMap<String, Arc<dyn NdArrayD>>> {
    let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();

    // A file that names no dimension gets one invented name per length per
    // group. Give every length one name instead, so two groups of one file
    // broadcast against each other. A NetCDF-4 file names every dimension, so
    // this renames nothing there.
    let phony = PhonyDimensions::of_file(file);

    // ── Variables and their per-variable attributes ──────────────────────
    for info in &file.root().variables {
        let Some(variable) = file.variable(&info.path) else {
            continue;
        };

        if let Ok(array) = compat::variable_to_nd_array(file.clone(), &variable, &phony) {
            arrays.insert(info.name.clone(), array);
        }

        for attribute in variable.attributes() {
            let full_name = format!("{}.{}", info.name, attribute.name);
            if let Ok(array) = compat::attribute_to_nd_array(&full_name, &attribute.value) {
                arrays.insert(full_name, array);
            }
        }
    }

    // ── Global file attributes ──────────────────────────────────────────
    for attribute in file.attributes() {
        let key = format!(".{}", attribute.name);
        if let Ok(array) = compat::attribute_to_nd_array(&key, &attribute.value) {
            arrays.insert(key, array);
        }
    }

    // Deterministic ordering by name.
    arrays.sort_keys();

    Ok(arrays)
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use beacon_nd_array::{
        datatypes::{NdArrayDataType, TimestampNanosecond},
        NdArray,
    };
    use object_store::local::LocalFileSystem;

    const WOD_FILE: &str = "wod_ctd_1964.nc";
    const GRIDDED_FILE: &str = "gridded-example.nc";

    /// A store rooted at this crate's `test_files` directory.
    fn test_store() -> Arc<dyn ObjectStore> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_files");
        Arc::new(LocalFileSystem::new_with_prefix(root).expect("test_files store"))
    }

    async fn open(file: &str) -> AnyDataset {
        open_dataset(test_store(), Path::from(file))
            .await
            .unwrap_or_else(|e| panic!("open {file}: {e}"))
    }

    // ── Opening ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn open_dataset_reads_a_ragged_file() {
        let any = open(WOD_FILE).await;
        assert!(any.is_ragged(), "the WOD file is a ragged dataset");
        assert!(!any.dataset().arrays.is_empty());
    }

    #[tokio::test]
    async fn open_dataset_reads_a_gridded_file() {
        let any = open(GRIDDED_FILE).await;
        assert!(any.get_array("analysed_sst").is_some());
    }

    /// A store that counts the `head` requests it gets.
    #[derive(Debug)]
    struct HeadCountingStore {
        inner: Arc<dyn ObjectStore>,
        heads: std::sync::atomic::AtomicUsize,
    }

    impl std::fmt::Display for HeadCountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "HeadCountingStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for HeadCountingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if options.head {
                self.heads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A listed object carries its size, so the open sends no `head` request.
    #[tokio::test]
    async fn open_object_sends_no_head_request() {
        use object_store::ObjectStoreExt;

        let object = test_store().head(&Path::from(WOD_FILE)).await.unwrap();
        let store = Arc::new(HeadCountingStore {
            inner: test_store(),
            heads: Default::default(),
        });

        let any = open_object(store.clone(), &object).await.unwrap();

        assert!(!any.dataset().arrays.is_empty());
        assert_eq!(store.heads.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn open_dataset_returns_an_error_for_a_missing_object() {
        assert!(open_dataset(test_store(), Path::from("nope.nc"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn dataset_name_is_the_object_path() {
        let any = open(WOD_FILE).await;
        assert_eq!(any.name(), WOD_FILE);
    }

    /// The decoded `analysed_sst` values of a dataset.
    async fn sst_values(dataset: &AnyDataset) -> Vec<f64> {
        dataset
            .get_array("analysed_sst")
            .unwrap()
            .as_any()
            .downcast_ref::<NdArray<f64>>()
            .unwrap()
            .clone_into_raw_vec()
            .await
    }

    // ── Chunk shape ────────────────────────────────────────────────────

    /// A chunked variable reports the chunk shape the file stores, not the
    /// full shape. The nd engine reads one chunk at a time with it.
    #[tokio::test]
    async fn a_chunked_variable_reports_the_chunk_shape_of_the_file() {
        let any = open(GRIDDED_FILE).await;

        let sst = any.get_array("analysed_sst").unwrap();
        assert_eq!(sst.shape(), vec![1, 1208, 1920]);
        assert_eq!(sst.chunk_shape(), vec![1, 604, 960]);

        let error = any.get_array("analysis_error").unwrap();
        assert_eq!(error.chunk_shape(), vec![1, 604, 960]);

        let lon = any.get_array("lon").unwrap();
        assert_eq!(lon.chunk_shape(), vec![1920]);
    }

    /// One chunk holds the whole array. The two shapes are then equal.
    #[tokio::test]
    async fn a_single_chunk_reports_the_full_shape() {
        let any = open(GRIDDED_FILE).await;

        let ice = any.get_array("sea_ice_fraction").unwrap();
        assert_eq!(ice.chunk_shape(), vec![1, 1208, 1920]);

        let mask = any.get_array("mask").unwrap();
        assert_eq!(mask.chunk_shape(), vec![1, 1208, 1920]);
    }

    /// The WOD file stores every variable contiguously. A variable with no
    /// chunks reports its full shape. That includes the fixed-size string
    /// variables, which drop their length axis from both shapes.
    #[tokio::test]
    async fn a_variable_with_no_chunks_reports_the_full_shape() {
        let any = open(WOD_FILE).await;
        for (name, array) in &any.dataset().arrays {
            assert_eq!(
                array.chunk_shape(),
                array.shape(),
                "'{name}' has no chunks, so it must report its full shape"
            );
        }
    }

    // ── CF decoding ────────────────────────────────────────────────────

    /// `analysed_sst` is packed `int16` with `scale_factor` and `add_offset`,
    /// so it decodes to `f64`. The real (non-fill) cells sit in a physical
    /// kelvin range.
    #[tokio::test]
    async fn scale_and_offset_decode_to_f64() {
        let any = open(GRIDDED_FILE).await;

        let array = any.get_array("analysed_sst").unwrap();
        assert_eq!(array.datatype(), NdArrayDataType::F64);

        let rust_values = sst_values(&any).await;

        // The fill cells decode to the packed fill run through the same
        // arithmetic, so take the maximum to reach a real measurement.
        let warmest = rust_values.iter().copied().fold(f64::MIN, f64::max);
        assert!(
            (200.0..400.0).contains(&warmest),
            "decoded SST must be a kelvin value, got {warmest}"
        );
    }

    /// A `units` attribute in the CF time form decodes to a timestamp.
    #[tokio::test]
    async fn cf_time_decodes_to_a_timestamp() {
        let any = open(GRIDDED_FILE).await;
        let array = any.get_array("time").unwrap();
        assert_eq!(array.datatype(), NdArrayDataType::Timestamp);

        let typed = array
            .as_any()
            .downcast_ref::<NdArray<TimestampNanosecond>>()
            .unwrap();
        let raw = typed.clone_into_raw_vec().await;
        assert!(!raw.is_empty());
    }

    /// A char variable with a trailing length dimension decodes as strings,
    /// and the length axis stays out of the shape.
    #[tokio::test]
    async fn fixed_size_char_variable_decodes_as_strings() {
        let any = open(WOD_FILE).await;

        // `country` is `char country(casts, strnlen)` in the file.
        let array = any.get_array("country").unwrap();
        assert_eq!(array.datatype(), NdArrayDataType::String);
        assert_eq!(array.dimensions(), vec!["casts".to_string()]);
    }

    // ── Attributes ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn a_global_attribute_gets_a_leading_dot() {
        let any = open(WOD_FILE).await;
        assert!(any.get_array(".Conventions").is_some());
        assert!(any.get_array("Conventions").is_none());
    }

    #[tokio::test]
    async fn a_variable_attribute_gets_a_dotted_name() {
        let any = open(GRIDDED_FILE).await;
        assert!(any.get_array("analysed_sst.units").is_some());
    }

    #[tokio::test]
    async fn array_names_are_sorted() {
        let file = Arc::new(
            AsyncNetcdfFile::open_store(test_store(), Path::from(WOD_FILE))
                .await
                .unwrap(),
        );
        let keys: Vec<String> = read_arrays(&file).unwrap().keys().cloned().collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }
}
