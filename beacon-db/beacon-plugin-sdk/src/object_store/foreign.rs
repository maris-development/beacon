//! The receiver side: an [`FFI_ObjectStore`] as an `object_store::ObjectStore`.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use ::bytes::Bytes;
use ::object_store::path::Path;
use ::object_store::{
    CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result,
};
use async_trait::async_trait;
use datafusion_ffi::ffi_option::FFI_Option;
use futures::StreamExt;
use futures::stream::BoxStream;
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;

use super::OBJECT_STORE_FFI_VERSION;
use super::bytes::FFI_Bytes;
use super::export::{FFI_GetResult, FFI_ObjectStore};
use super::multipart::ForeignMultipartUpload;
use super::options::{
    FFI_CopyOptions, FFI_GetOptions, FFI_ListResult, FFI_PutMultipartOptions, FFI_PutOptions,
    FFI_Range, FFI_RenameOptions, parse_path,
};
use super::stream::{FFI_MetaStream, FFI_PathStream};

/// An `object_store::ObjectStore` over an [`FFI_ObjectStore`] from another library.
///
/// Every call crosses the table. Build it with `Arc::<dyn ObjectStore>::try_from`.
pub struct ForeignObjectStore(FFI_ObjectStore);

impl TryFrom<FFI_ObjectStore> for Arc<dyn ObjectStore> {
    type Error = Error;

    /// Returns the original store when this library made the table.
    ///
    /// Fails with `NotSupported` when the table has another [`OBJECT_STORE_FFI_VERSION`].
    fn try_from(store: FFI_ObjectStore) -> Result<Self> {
        if (store.library_marker_id)() == datafusion_ffi::get_library_marker_id() {
            return Ok(store.local_store());
        }
        // SAFETY: `version` has the same place in every table version.
        let version = unsafe { (store.version)() };
        if version != OBJECT_STORE_FFI_VERSION {
            return Err(Error::NotSupported {
                source: format!(
                    "object store FFI version {version} is not supported (want {OBJECT_STORE_FFI_VERSION})"
                )
                .into(),
            });
        }
        Ok(Arc::new(ForeignObjectStore(store)))
    }
}

impl fmt::Display for ForeignObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // SAFETY: the table is live and of this version.
        let text = unsafe { (self.0.display)(&self.0) };
        f.write_str(text.as_str())
    }
}

impl fmt::Debug for ForeignObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ForeignObjectStore({self})")
    }
}

fn path_string(path: &Path) -> SString {
    SString::from(path.as_ref())
}

fn prefix_string(prefix: Option<&Path>) -> FFI_Option<SString> {
    prefix.map(path_string).into()
}

fn receive_metas(metas: FFI_MetaStream) -> BoxStream<'static, Result<ObjectMeta>> {
    metas
        .map(|meta| meta.and_then(ObjectMeta::try_from))
        .boxed()
}

// SAFETY for each call below: the table is live and of this version, and the
// function pointer matches it.
#[async_trait]
impl ObjectStore for ForeignObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let payload: SVec<FFI_Bytes> = payload.into_iter().map(FFI_Bytes::from).collect();
        let options = FFI_PutOptions::from(&opts);
        let future = unsafe { (self.0.put_opts)(&self.0, path_string(location), payload, options) };
        future.await.into_result().map(PutResult::from)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let options = FFI_PutMultipartOptions::from(&opts);
        let future =
            unsafe { (self.0.put_multipart_opts)(&self.0, path_string(location), options) };
        let upload = future.await.into_result()?;
        Ok(Box::new(ForeignMultipartUpload::from(upload)))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let options = FFI_GetOptions::try_from(&options)?;
        let future = unsafe { (self.0.get_opts)(&self.0, path_string(location), options) };
        let FFI_GetResult { meta, payload } = future.await.into_result()?;
        let (meta, range, attributes) = meta.into_parts()?;
        let payload = payload.map(|chunk| chunk.map(Bytes::from)).boxed();
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta,
            range,
            attributes,
        })
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let ranges: SVec<FFI_Range> = ranges.iter().cloned().map(FFI_Range::from).collect();
        let future = unsafe { (self.0.get_ranges)(&self.0, path_string(location), ranges) };
        let chunks = future.await.into_result()?;
        Ok(chunks.into_iter().map(Bytes::from).collect())
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let locations = FFI_PathStream::new_unentered(locations, |path| Ok(path_string(&path)));
        let deleted = unsafe { (self.0.delete_stream)(&self.0, locations) };
        deleted
            .map(|path| path.and_then(|path| parse_path(path.as_str())))
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        receive_metas(unsafe { (self.0.list)(&self.0, prefix_string(prefix)) })
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let offset = path_string(offset);
        receive_metas(unsafe { (self.0.list_with_offset)(&self.0, prefix_string(prefix), offset) })
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let future = unsafe { (self.0.list_with_delimiter)(&self.0, prefix_string(prefix)) };
        let listed: FFI_ListResult = future.await.into_result()?;
        ListResult::try_from(listed)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let options = FFI_CopyOptions::from(&options);
        let future =
            unsafe { (self.0.copy_opts)(&self.0, path_string(from), path_string(to), options) };
        future.await.into_result()
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        let options = FFI_RenameOptions::from(&options);
        let future =
            unsafe { (self.0.rename_opts)(&self.0, path_string(from), path_string(to), options) };
        future.await.into_result()
    }
}
