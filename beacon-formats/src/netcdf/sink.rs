use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_arrow_netcdf::{encoders::default::DefaultEncoder, writer::ArrowRecordBatchWriter};
use datafusion::{
    datasource::{physical_plan::FileSinkConfig, sink::DataSink},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType},
};
use futures::StreamExt;

use crate::netcdf::object_resolver::NetCDFSinkResolver;

#[derive(Debug, Clone)]
pub struct NetCDFSink {
    sink_config: FileSinkConfig,
    path_resolver: NetCDFSinkResolver,
}

impl NetCDFSink {
    pub fn new(path_resolver: Arc<NetCDFSinkResolver>, sink_config: FileSinkConfig) -> Self {
        Self {
            sink_config,
            path_resolver: (*path_resolver).clone(),
        }
    }
}

impl DisplayAs for NetCDFSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "NetCDFSink")
    }
}

#[async_trait::async_trait]
impl DataSink for NetCDFSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        self.sink_config.output_schema()
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let arrow_schema = self.sink_config.output_schema().clone();
        let output_path = self
            .path_resolver
            .resolve_output_path(&self.sink_config.table_paths[0].prefix());

        let mut rows_written: u64 = 0;
        let mut nc_writer =
            ArrowRecordBatchWriter::<DefaultEncoder>::new(output_path, arrow_schema).map_err(
                |e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to create NetCDF ArrowRecordBatchWriter: {}",
                        e
                    ))
                },
            )?;

        let mut pinned_steam = std::pin::pin!(data);

        while let Some(batch) = pinned_steam.next().await {
            let batch = batch?;
            rows_written += batch.num_rows() as u64;
            nc_writer.write_record_batch(batch).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to write record batch to NetCDF: {}",
                    e
                ))
            })?;
        }

        nc_writer.finish().map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to finish writing NetCDF: {}",
                e
            ))
        })?;

        Ok(rows_written)
    }
}
