//! DataFusion [`DataSink`] that writes incoming rows into a Lance dataset.
//!
//! Lance's own `LanceTableProvider` is read-oriented, so beacon implements the
//! write path: `LanceTable::insert_into` returns a `DataSinkExec` wrapping this
//! sink, which collects the input stream and applies it via [`crate::io`].

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::sink::DataSink;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};

use crate::io::{write_stream, WriteKind};
use crate::warehouse::LanceWarehouse;

/// Sink that applies a stream of batches to a single Lance dataset (by URI).
#[derive(Debug)]
pub struct LanceDataSink {
    uri: String,
    schema: SchemaRef,
    kind: WriteKind,
    warehouse: Arc<LanceWarehouse>,
}

impl LanceDataSink {
    pub fn new(
        uri: String,
        schema: SchemaRef,
        kind: WriteKind,
        warehouse: Arc<LanceWarehouse>,
    ) -> Self {
        Self {
            uri,
            schema,
            kind,
            warehouse,
        }
    }
}

impl DisplayAs for LanceDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LanceDataSink(uri={}, kind={:?})", self.uri, self.kind)
    }
}

#[async_trait]
impl DataSink for LanceDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        // Serialize writers to this dataset across the (async) write, then stream
        // the input directly into Lance — no full-table buffering.
        let lock = self.warehouse.lock(&self.uri);
        let _guard = lock.lock().await;
        write_stream(&self.uri, self.warehouse.session(), data, self.kind)
            .await
            .map_err(|e| DataFusionError::External(e.into()))
    }
}
