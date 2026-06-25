//! DataFusion [`DataSink`] that writes incoming rows into a Lance dataset.
//!
//! Lance's own `LanceTableProvider` is read-oriented, so beacon implements the
//! write path: `LanceTable::insert_into` returns a `DataSinkExec` wrapping this
//! sink, which collects the input stream and applies it via [`crate::io`].

use std::any::Any;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::sink::DataSink;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use futures::TryStreamExt;

use crate::io::{write_batches, WriteKind};
use crate::warehouse::LanceWarehouse;

/// Sink that applies a stream of batches to a single Lance dataset.
#[derive(Debug)]
pub struct LanceDataSink {
    location: PathBuf,
    schema: SchemaRef,
    kind: WriteKind,
    warehouse: Arc<LanceWarehouse>,
}

impl LanceDataSink {
    pub fn new(
        location: PathBuf,
        schema: SchemaRef,
        kind: WriteKind,
        warehouse: Arc<LanceWarehouse>,
    ) -> Self {
        Self {
            location,
            schema,
            kind,
            warehouse,
        }
    }
}

impl DisplayAs for LanceDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LanceDataSink(location={}, kind={:?})",
            self.location.display(),
            self.kind
        )
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
        let batches = data.try_collect::<Vec<_>>().await?;
        // Serialize writers to this dataset across the (async) write.
        let lock = self.warehouse.lock(&self.location);
        let _guard = lock.lock().await;
        write_batches(&self.location, self.schema.clone(), batches, self.kind)
            .await
            .map_err(|e| DataFusionError::External(e.into()))
    }
}
