//! Open a NetCDF object through [`object_store`].
//!
//! [`open_file`] opens the file. [`compat`](crate::oxcdf_reader::compat) turns
//! its variables and attributes into lazy ND arrays.
//!
//! # Example
//!
//! ```no_run
//! # use std::sync::Arc;
//! # async fn example(store: Arc<dyn object_store::ObjectStore>) -> anyhow::Result<()> {
//! use object_store::{path::Path, ObjectStoreExt};
//!
//! let object = store.head(&Path::from("data.nc")).await?;
//! let file = beacon_arrow_netcdf::oxcdf_reader::open_file(store, &object).await?;
//! for variable in &file.root().variables {
//!     println!("{}", variable.name);
//! }
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use object_store::{ObjectMeta, ObjectStore};
use oxcdf::{async_source::AsyncObjectStoreSource, AsyncNetcdfFile, OpenOptions};

/// Open the file that `object` describes, with the size from `object`.
///
/// The reader fetches byte ranges. It never copies the whole object, so an
/// object in S3, GCS or Azure needs no local file. The size comes from
/// `object`, so the open sends no `head` request.
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

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dimensions::PhonyDimensions;
    use crate::oxcdf_reader::compat;
    use beacon_nd_array::{
        datatypes::{NdArrayDataType, TimestampNanosecond},
        NdArray, NdArrayD,
    };
    use object_store::{local::LocalFileSystem, path::Path, ObjectStoreExt};

    const WOD_FILE: &str = "wod_ctd_1964.nc";
    const GRIDDED_FILE: &str = "gridded-example.nc";

    /// A store rooted at this crate's `test_files` directory.
    fn test_store() -> Arc<dyn ObjectStore> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_files");
        Arc::new(LocalFileSystem::new_with_prefix(root).expect("test_files store"))
    }

    async fn open(file: &str) -> Arc<AsyncNetcdfFile> {
        let store = test_store();
        let object = store.head(&Path::from(file)).await.unwrap();
        Arc::new(
            open_file(store, &object)
                .await
                .unwrap_or_else(|e| panic!("open {file}: {e}")),
        )
    }

    /// The ND array of the root variable `name`.
    fn array(file: &Arc<AsyncNetcdfFile>, name: &str) -> Arc<dyn NdArrayD> {
        let variable = file.variable(name).unwrap();
        compat::variable_to_nd_array(file.clone(), &variable, &PhonyDimensions::of_file(file))
            .unwrap()
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
    async fn open_file_sends_no_head_request() {
        let object = test_store().head(&Path::from(WOD_FILE)).await.unwrap();
        let store = Arc::new(HeadCountingStore {
            inner: test_store(),
            heads: Default::default(),
        });

        let file = open_file(store.clone(), &object).await.unwrap();

        assert!(!file.root().variables.is_empty());
        assert_eq!(store.heads.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    // ── Chunk shape ────────────────────────────────────────────────────

    /// A chunked variable reports the chunk shape the file stores, not the
    /// full shape. The nd engine reads one chunk at a time with it.
    #[tokio::test]
    async fn a_chunked_variable_reports_the_chunk_shape_of_the_file() {
        let file = open(GRIDDED_FILE).await;

        let sst = array(&file, "analysed_sst");
        assert_eq!(sst.shape(), vec![1, 1208, 1920]);
        assert_eq!(sst.chunk_shape(), vec![1, 604, 960]);

        let error = array(&file, "analysis_error");
        assert_eq!(error.chunk_shape(), vec![1, 604, 960]);

        let lon = array(&file, "lon");
        assert_eq!(lon.chunk_shape(), vec![1920]);
    }

    /// One chunk holds the whole array. The two shapes are then equal.
    #[tokio::test]
    async fn a_single_chunk_reports_the_full_shape() {
        let file = open(GRIDDED_FILE).await;

        let ice = array(&file, "sea_ice_fraction");
        assert_eq!(ice.chunk_shape(), vec![1, 1208, 1920]);

        let mask = array(&file, "mask");
        assert_eq!(mask.chunk_shape(), vec![1, 1208, 1920]);
    }

    /// The WOD file stores every variable contiguously. A variable with no
    /// chunks reports its full shape. That includes the fixed-size string
    /// variables, which drop their length axis from both shapes.
    #[tokio::test]
    async fn a_variable_with_no_chunks_reports_the_full_shape() {
        let file = open(WOD_FILE).await;
        let phony = PhonyDimensions::of_file(&file);
        let mut count = 0;
        for info in &file.root().variables {
            let variable = file.variable(&info.path).unwrap();
            let Ok(array) = compat::variable_to_nd_array(file.clone(), &variable, &phony) else {
                continue;
            };
            assert_eq!(
                array.chunk_shape(),
                array.shape(),
                "'{}' has no chunks, so it must report its full shape",
                info.name
            );
            count += 1;
        }
        assert!(count > 0);
    }

    // ── CF decoding ────────────────────────────────────────────────────

    /// `analysed_sst` is packed `int16` with `scale_factor` and `add_offset`,
    /// so it decodes to `f64`. The real (non-fill) cells sit in a physical
    /// kelvin range.
    #[tokio::test]
    async fn scale_and_offset_decode_to_f64() {
        let file = open(GRIDDED_FILE).await;

        let array = array(&file, "analysed_sst");
        assert_eq!(array.datatype(), NdArrayDataType::F64);

        let values = array
            .as_any()
            .downcast_ref::<NdArray<f64>>()
            .unwrap()
            .clone_into_raw_vec()
            .await;

        // The fill cells decode to the packed fill run through the same
        // arithmetic, so take the maximum to reach a real measurement.
        let warmest = values.iter().copied().fold(f64::MIN, f64::max);
        assert!(
            (200.0..400.0).contains(&warmest),
            "decoded SST must be a kelvin value, got {warmest}"
        );
    }

    /// A `units` attribute in the CF time form decodes to a timestamp.
    #[tokio::test]
    async fn cf_time_decodes_to_a_timestamp() {
        let file = open(GRIDDED_FILE).await;
        let array = array(&file, "time");
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
        let file = open(WOD_FILE).await;

        // `country` is `char country(casts, strnlen)` in the file.
        let array = array(&file, "country");
        assert_eq!(array.datatype(), NdArrayDataType::String);
        assert_eq!(array.dimensions(), vec!["casts".to_string()]);
    }
}
