//! A configured file format from another library.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion_ffi::arrow_wrappers::WrappedSchema;
use datafusion_ffi::record_batch_stream::FFI_RecordBatchStream;
use datafusion_ffi::util::FFI_Result;
use object_store::{ObjectMeta, ObjectStore};
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;
use tokio::runtime::Handle;

use super::FILE_FORMAT_FFI_VERSION;
use super::export::FFI_FileFormat;
use super::reader::FormatReader;
use super::statistics::decode_statistics;
use super::stream::import_batch_stream;
use super::types::{FFI_ScanRequest, FFI_WriteRequest, ScanRequest, SortKey, WriteRequest};
use crate::file_format::stream::export_batch_stream_entered;
use crate::object_store::{FFI_ObjectMeta, FFI_ObjectStore};

/// A [`FormatReader`] over an [`FFI_FileFormat`] from another library.
///
/// Every call crosses the table. The stores that it passes run their I/O on `handle`.
pub struct ForeignFormatReader {
    ffi: FFI_FileFormat,
    handle: Handle,
}

impl ForeignFormatReader {
    /// Returns the original reader when this library made the table.
    ///
    /// Fails when the table has another [`FILE_FORMAT_FFI_VERSION`].
    pub fn try_new(ffi: FFI_FileFormat, handle: Handle) -> Result<Arc<dyn FormatReader>> {
        if (ffi.library_marker_id)() == datafusion_ffi::get_library_marker_id() {
            return Ok(ffi.local_reader());
        }
        // SAFETY: `version` has the same place in every table version.
        let version = unsafe { (ffi.version)() };
        if version != FILE_FORMAT_FFI_VERSION {
            return Err(DataFusionError::Plan(format!(
                "file format FFI version {version} is not supported (want {FILE_FORMAT_FFI_VERSION})"
            )));
        }
        Ok(Arc::new(Self { ffi, handle }))
    }

    fn store(&self, store: Arc<dyn ObjectStore>) -> FFI_ObjectStore {
        FFI_ObjectStore::new(store, self.handle.clone())
    }
}

impl fmt::Debug for ForeignFormatReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ForeignFormatReader")
    }
}

/// Turns an error from the other library into a DataFusion error.
pub(crate) fn plugin_error(text: SString) -> DataFusionError {
    DataFusionError::Execution(text.as_str().to_string())
}

pub(crate) fn into_result<T>(result: FFI_Result<T>) -> Result<T> {
    match result {
        FFI_Result::Ok(value) => Ok(value),
        FFI_Result::Err(text) => Err(plugin_error(text)),
    }
}

fn object(object: &ObjectMeta) -> Result<FFI_ObjectMeta> {
    FFI_ObjectMeta::try_from(object).map_err(|e| DataFusionError::External(Box::new(e)))
}

// SAFETY for each call below: the live table has this version and matches its pointers.
#[async_trait]
impl FormatReader for ForeignFormatReader {
    async fn infer_schema(
        &self,
        store: Arc<dyn ObjectStore>,
        object_meta: &ObjectMeta,
    ) -> Result<SchemaRef> {
        let future =
            unsafe { (self.ffi.infer_schema)(&self.ffi, self.store(store), object(object_meta)?) };
        into_result(future.await).map(SchemaRef::from)
    }

    async fn infer_stats(
        &self,
        store: Arc<dyn ObjectStore>,
        object_meta: &ObjectMeta,
        table_schema: SchemaRef,
    ) -> Result<Option<Statistics>> {
        let future = unsafe {
            (self.ffi.infer_stats)(
                &self.ffi,
                self.store(store),
                object(object_meta)?,
                WrappedSchema::from(table_schema),
            )
        };
        let bytes: Option<SVec<u8>> = into_result(future.await)?.into();
        bytes
            .map(|bytes| decode_statistics(bytes.as_slice()))
            .transpose()
    }

    async fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        object_meta: &ObjectMeta,
        request: ScanRequest,
    ) -> Result<SendableRecordBatchStream> {
        let future = unsafe {
            (self.ffi.open)(
                &self.ffi,
                self.store(store),
                object(object_meta)?,
                FFI_ScanRequest::from(&request),
            )
        };
        into_result(future.await).map(import_batch_stream)
    }

    fn write_ordering(&self, schema: SchemaRef) -> Result<Vec<SortKey>> {
        let keys = into_result(unsafe {
            (self.ffi.write_ordering)(&self.ffi, WrappedSchema::from(schema))
        })?;
        Ok(keys.iter().map(SortKey::from).collect())
    }

    async fn write(
        &self,
        store: Arc<dyn ObjectStore>,
        request: WriteRequest,
        input: SendableRecordBatchStream,
    ) -> Result<u64> {
        let input: FFI_RecordBatchStream = export_batch_stream_entered(input, self.handle.clone());
        let future = unsafe {
            (self.ffi.write)(
                &self.ffi,
                self.store(store),
                FFI_WriteRequest::from(&request),
                input,
            )
        };
        into_result(future.await)
    }
}
