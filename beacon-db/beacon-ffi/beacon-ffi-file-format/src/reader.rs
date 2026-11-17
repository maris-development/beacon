//! The trait that a configured file format implements.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Statistics, not_impl_err};
use datafusion::error::Result;
use datafusion::execution::SendableRecordBatchStream;
use object_store::{ObjectMeta, ObjectStore};

use super::types::{ScanRequest, SortKey, WriteRequest};

/// One configured file format: the format factory makes it from the table options.
///
/// Each call is about one file. Beacon merges the schemas of many files, adapts the
/// batches to the table schema, applies the filters again, and splits the output files.
#[async_trait]
pub trait FormatReader: Send + Sync + 'static {
    /// The logical schema of one file. For an `Nd` format: the element types.
    async fn infer_schema(
        &self,
        store: Arc<dyn ObjectStore>,
        object: &ObjectMeta,
    ) -> Result<SchemaRef>;

    /// Statistics of one file against `table_schema`, or `None`.
    async fn infer_stats(
        &self,
        store: Arc<dyn ObjectStore>,
        object: &ObjectMeta,
        table_schema: SchemaRef,
    ) -> Result<Option<Statistics>> {
        let _ = (store, object, table_schema);
        Ok(None)
    }

    /// Opens one file. The batches hold some or all of the requested columns, by name.
    async fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        object: &ObjectMeta,
        request: ScanRequest,
    ) -> Result<SendableRecordBatchStream>;

    /// The input sort order that the writer needs, or an empty list.
    fn write_ordering(&self, schema: SchemaRef) -> Result<Vec<SortKey>> {
        let _ = schema;
        Ok(Vec::new())
    }

    /// Writes one output file and returns the row count.
    async fn write(
        &self,
        store: Arc<dyn ObjectStore>,
        request: WriteRequest,
        input: SendableRecordBatchStream,
    ) -> Result<u64> {
        let _ = (store, request, input);
        not_impl_err!("this format cannot write")
    }
}
