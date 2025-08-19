use std::sync::Arc;

use datafusion::{
    common::Statistics,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileSource},
    },
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use object_store::ObjectStore;

pub struct NetCDFSource {}

impl FileSource for NetCDFSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Arc<dyn FileOpener> {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        todo!()
    }

    fn with_schema(&self, schema: arrow::datatypes::SchemaRef) -> Arc<dyn FileSource> {
        todo!()
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        todo!()
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        todo!()
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        todo!()
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        todo!()
    }

    fn file_type(&self) -> &str {
        "netcdf"
    }
}

pub struct NetCDFOpener {}

impl FileOpener for NetCDFOpener {
    fn open(
        &self,
        file_meta: FileMeta,
        file: PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        todo!()
    }
}
